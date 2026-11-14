// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package actors

import (
	"context"
	"errors"
	"log/slog"
	"time"

	goakt "github.com/tochemey/goakt/v4/actor"
	"github.com/tochemey/goakt/v4/breaker"

	"github.com/conveyorq/conveyor/internal/broker"
	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
)

// A gateway makes no broker round trip inside its turn: a slow or degraded
// store would otherwise hold a worker of the dispatcher pool every actor and
// grain on the node shares. Durable transitions (a result's ack, fail, or
// archive; a batch's acks; heartbeat lease extensions) run in PipeTo tasks
// over values captured in the turn, and come back as the messages below. What
// depends on the outcome and touches actor state (dependent resolution,
// completion reports and credit refills, lost-lease handling) runs in the turn
// that applies them.

// outcomeApplied reports the durable transition of one worker result.
type outcomeApplied struct {
	// taskID is the resolved task.
	taskID string
	// leaseID is the lease the result was reported under; a webhook gateway
	// keys its batch accounting by it.
	leaseID string
	// queue is the queue the task was leased from.
	queue string
	// taskType keys the circuit breaker that may withhold the completion.
	taskType string
	// success records that the task completed.
	success bool
	// terminal records that the task reached a terminal state here.
	terminal bool
}

// batchOutcomeApplied reports the durable transitions of one batch result.
type batchOutcomeApplied struct {
	// queue is the queue the batch was leased from.
	queue string
	// total is how many members this gateway still tracked.
	total int
	// succeeded is how many members completed.
	succeeded int
	// terminal are the members that reached a terminal state here.
	terminal []string
}

// leasesExtended reports a heartbeat's lease extensions: lost are the leases
// another delivery took over.
type leasesExtended struct {
	// lost are the leases found lost to another delivery.
	lost []leaseRef
}

// asyncLeaseExtended reports one asynchronous webhook delivery's lease
// extension: err is nil on success, broker.ErrLeaseLost when another delivery
// owns the task, or the failure that left the lease to the next heartbeat.
type asyncLeaseExtended struct {
	// ref is the extended lease.
	ref leaseRef
	// err is the extension's outcome.
	err error
}

// webhookWorkersListed reports the persisted webhook registrations a
// reconcile read, or the failure that read hit.
type webhookWorkersListed struct {
	// workers are the persisted registrations.
	workers []*broker.WebhookWorker
	// err is the read failure; the next tick retries.
	err error
}

// batchMember is one tracked member of a batch result, with the outcome to
// apply to it.
type batchMember struct {
	// entry is the member's delivery record, no longer tracked by the gateway.
	entry *inflightTask
	// result is the member's outcome; RELEASED when the worker omitted it.
	result *conveyorv1.Result
}

// outcomeApplied finishes a worker result once its durable transition is
// done: a terminal task resolves its dependents, and the completion report,
// which doubles as the credit refill, goes to the queue grain unless the task
// type's breaker is open.
func (g *Gateway) outcomeApplied(ctx *goakt.ReceiveContext, message outcomeApplied) {
	if message.terminal {
		g.resolveDependents(ctx, message.taskID)
	}

	if g.breakerFor(message.taskType).State() == breaker.Open {
		g.runtime.Metrics().BreakerOpen(context.Background())
		g.deferCompletion(ctx, message.queue, message.taskID, message.success)

		return
	}

	g.reportCompletion(ctx, message.queue, message.taskID, message.success)
}

// batchOutcomeApplied finishes a batch result once its durable transitions are
// done: terminal members resolve their dependents and the queue grain gets one
// completion report for the batch.
func (g *Gateway) batchOutcomeApplied(ctx *goakt.ReceiveContext, message batchOutcomeApplied) {
	for _, taskID := range message.terminal {
		g.resolveDependents(ctx, taskID)
	}

	g.reportBatchCompletion(ctx, message.queue, message.total, message.succeeded)
}

// leasesExtended applies a heartbeat's lost leases. A delivery still tracked
// under the lost lease leaves tracking, its credit returns to the grain, and
// the worker is told to cancel: another delivery owns the task now, and the
// worker's eventual result for it is dropped as unknown and would never
// refill the credit. A delivery already resolved, or re-leased since, is left
// alone.
func (g *Gateway) leasesExtended(ctx *goakt.ReceiveContext, message leasesExtended) {
	for _, ref := range message.lost {
		entry, ok := g.inflight[ref.taskID]
		if !ok || entry.leaseID != ref.leaseID {
			continue
		}

		delete(g.inflight, ref.taskID)
		g.endPayloadStreams(ref.taskID)
		g.runtime.Counters().Active.Add(-1)
		g.refundLostLease(ctx, entry)
		g.enqueueFrame(ctx, cancelFrame(ref.taskID))

		g.runtime.Logger().Debug("lease lost; worker canceled", "task_id", ref.taskID, "gateway", g.name)
	}
}

// pipeOutcome applies one result's durable transition in a PipeTo task and
// reports it back as outcomeApplied. The delivery must already be out of the
// gateway's tracking, so the task is the entry's only reader.
func pipeOutcome(ctx *goakt.ReceiveContext, runtime *Runtime, taskID string, entry *inflightTask, result *conveyorv1.Result) {
	ctx.PipeTo(ctx.Self(), func() (any, error) {
		success, terminal := applyOutcome(context.Background(), runtime, entry, result)

		return outcomeApplied{
			taskID:   taskID,
			leaseID:  entry.leaseID,
			queue:    entry.queue,
			taskType: entry.taskType,
			success:  success,
			terminal: terminal,
		}, nil
	})
}

// pipeLeaseExtensions extends each lease in a PipeTo task and reports the ones
// lost to another delivery back as leasesExtended.
func pipeLeaseExtensions(ctx *goakt.ReceiveContext, runtime *Runtime, refs []leaseRef) {
	taskLog := runtime.Broker()
	logger := runtime.Logger()
	ttl := runtime.Settings().LeaseTTL

	ctx.PipeTo(ctx.Self(), func() (any, error) {
		return leasesExtended{lost: extendLeases(context.Background(), taskLog, logger, refs, ttl)}, nil
	})
}

// applyBatchOutcomes applies every member's durable transition: successful
// members complete together in one broker round trip, every other outcome runs
// through applyOutcome one member at a time. It is a PipeTo task body.
func applyBatchOutcomes(ctx context.Context, runtime *Runtime, queue string, members []batchMember) batchOutcomeApplied {
	applied := batchOutcomeApplied{queue: queue, total: len(members)}

	var successes []broker.AckItem

	entries := make(map[string]*inflightTask, len(members))

	for _, member := range members {
		taskID := member.result.GetTaskId()
		entries[taskID] = member.entry

		if member.result.GetOutcome() == conveyorv1.TaskOutcome_TASK_OUTCOME_SUCCESS {
			successes = append(successes, broker.AckItem{TaskID: taskID, LeaseID: member.entry.leaseID, Result: member.result.GetResult()})

			continue
		}

		success, terminal := applyOutcome(ctx, runtime, member.entry, member.result)
		if success {
			applied.succeeded++
		}

		if terminal {
			applied.terminal = append(applied.terminal, taskID)
		}
	}

	acked := ackBatch(ctx, runtime, successes, entries)
	applied.succeeded += len(acked)
	applied.terminal = append(applied.terminal, acked...)

	return applied
}

// ackBatch completes a batch's successful members in one broker round trip,
// mirroring what applyOutcome does per member on SUCCESS: it records each
// member's process duration and returns the ids that completed. A member whose
// lease was lost to another delivery is missing from the broker's answer and
// earns no completion, exactly as the single-task path treats a lost lease; a
// broker error completes none and is logged the same way.
func ackBatch(ctx context.Context, runtime *Runtime, items []broker.AckItem, entries map[string]*inflightTask) []string {
	if len(items) == 0 {
		return nil
	}

	acked, err := runtime.Broker().AckBatch(ctx, items)
	if err != nil {
		runtime.Logger().Warn("durable transition failed", "members", len(items), "error", err)
	}

	completed := make(map[string]struct{}, len(acked))
	for _, taskID := range acked {
		completed[taskID] = struct{}{}
	}

	now := runtime.Clock().Now()

	for _, item := range items {
		entry := entries[item.TaskID]
		runtime.Metrics().RecordProcessDuration(context.Background(), now.Sub(entry.dispatchedAt).Seconds(), entry.queue)

		if _, ok := completed[item.TaskID]; !ok && err == nil {
			runtime.Logger().Debug("result discarded: lease lost to another delivery", "task_id", item.TaskID)
		}
	}

	return acked
}

// extendLeases extends each lease by ttl and returns the ones lost to another
// delivery; any other failure is logged and the lease left to the next
// heartbeat. It is a PipeTo task body.
func extendLeases(ctx context.Context, taskLog broker.Broker, logger *slog.Logger, refs []leaseRef, ttl time.Duration) []leaseRef {
	var lost []leaseRef

	for _, ref := range refs {
		err := taskLog.ExtendLease(ctx, ref.taskID, ref.leaseID, ttl)

		switch {
		case err == nil:

		case errors.Is(err, broker.ErrLeaseLost):
			lost = append(lost, ref)

		default:
			logger.Warn("lease extension failed", "task_id", ref.taskID, "error", err)
		}
	}

	return lost
}

// cancelFrame builds the outbox entry of a Cancel frame for one task.
func cancelFrame(taskID string) outboundFrame {
	return outboundFrame{
		frame: &conveyorv1.ServerMessage{
			Frame: &conveyorv1.ServerMessage_Cancel{Cancel: &conveyorv1.Cancel{TaskId: taskID}},
		},
	}
}
