// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package actors

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	goakt "github.com/tochemey/goakt/v4/actor"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/conveyorq/conveyor/internal/broker"
	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
)

// This file holds the group-aggregation feature across the actor tier: the
// firing sweeper, the queue grain's lease-and-dispatch, and the gateway's batch
// delivery. The message-routing cases that reach this code live in the actors'
// own Receive/OnReceive switches (gateway.go, queue_grain.go); the behavior
// they invoke is here.

// groupSweepRef is the sweeper's stable tick reference. The singleton schedules
// its own firing ticks on start; the reference lets the entry be canceled or
// replaced rather than duplicated across failover.
const groupSweepRef = "conveyor-group-sweeper-sweep"

// GroupSweeper is the aggregation firing loop: on every GroupSweepTick it reads
// the accumulating groups of every queue that holds members and fires those
// whose size, max-delay, or grace threshold has elapsed by telling the queue
// grain (FireGroup). It is a cluster singleton, like the Reaper, so exactly one
// node evaluates firing at a time; the queue grain owns the actual lease and
// dispatch.
type GroupSweeper struct {
	// runtime is the engine runtime.
	runtime *Runtime
}

// enforce interface compliance at compile time.
var _ goakt.Actor = (*GroupSweeper)(nil)

// NewGroupSweeper returns a group-sweeper actor backed by the runtime.
func NewGroupSweeper() *GroupSweeper {
	return &GroupSweeper{}
}

// PreStart implements goakt.Actor.
func (s *GroupSweeper) PreStart(ctx *goakt.Context) error {
	runtime, ok := ctx.ActorSystem().Extension(BrokerExtensionID).(*Runtime)
	if !ok {
		return fmt.Errorf("group sweeper %s: extension %q is not registered", ctx.ActorName(), BrokerExtensionID)
	}

	s.runtime = runtime
	return nil
}

// Receive handles firing ticks. Broker I/O runs synchronously, as in the
// reaper: the mailbox carries only ticks.
func (s *GroupSweeper) Receive(ctx *goakt.ReceiveContext) {
	switch ctx.Message().(type) {
	case *goakt.PostStart:
		s.scheduleTicks(ctx)

	case *conveyorv1.GroupSweepTick:
		s.sweep(ctx)

	default:
		ctx.Unhandled()
	}
}

// scheduleTicks arms the recurring firing tick on the node now hosting the
// singleton; on failover the new host re-arms here, mirroring the reaper.
func (s *GroupSweeper) scheduleTicks(ctx *goakt.ReceiveContext) {
	interval := s.runtime.Settings().GroupSweepInterval

	// A non-positive interval disables aggregation firing (the server config
	// validates a positive value; this guards embedders and tests that leave it
	// unset, so the sweeper simply never ticks rather than busy-looping).
	if interval <= 0 {
		return
	}

	if err := ctx.ActorSystem().Schedule(ctx.Context(), new(conveyorv1.GroupSweepTick), ctx.Self(), interval, goakt.WithReference(groupSweepRef)); err != nil {
		s.runtime.Logger().Error("scheduling group sweep ticks failed", "error", err)
	}
}

// PostStop implements goakt.Actor.
func (s *GroupSweeper) PostStop(_ *goakt.Context) error {
	return nil
}

// sweep fires every due aggregation group. One GroupStats scan (over the
// partial aggregating index) yields every accumulating group across all queues,
// so the pass cost tracks the number of aggregating rows, not total volume.
func (s *GroupSweeper) sweep(ctx *goakt.ReceiveContext) {
	goCtx := ctx.Context()

	groups, err := s.runtime.Broker().GroupStats(goCtx)
	if err != nil {
		// Runs under the default Stop directive like the reaper: skip this pass
		// rather than escalate; the next tick retries.
		s.runtime.Logger().Warn("group sweep: group stats failed", "error", err)

		return
	}

	now := s.runtime.Clock().Now()
	settings := s.runtime.Settings()

	for _, group := range groups {
		if groupDue(group, settings, now) {
			fireGroup(goCtx, ctx.ActorSystem(), s.runtime, group.Queue, group.Group, group.Type)
		}
	}
}

// groupDue reports whether an accumulating group has met a firing threshold:
// enough members, too long since the first member, or quiet long enough since
// the last.
func groupDue(stat broker.GroupStat, settings Settings, now time.Time) bool {
	switch {
	case int(stat.Count) >= settings.GroupMaxSize:
		return true

	case now.Sub(stat.Oldest) >= settings.GroupMaxDelay:
		return true

	case now.Sub(stat.Newest) >= settings.GroupGracePeriod:
		return true

	default:
		return false
	}
}

// fireGroup tells a queue grain to lease and batch-dispatch one due aggregation
// group. Resolving the identity activates the grain if it is not live. Firing
// is best-effort like wakeQueue: the next sweep retries a missed group.
func fireGroup(ctx context.Context, system goakt.ActorSystem, runtime *Runtime, queue, group, taskType string) {
	identity, err := system.GrainIdentity(ctx, QueueGrainName(queue), queueGrainFactory,
		goakt.WithGrainDeactivateAfter(runtime.Settings().PassivateAfter))
	if err != nil {
		runtime.Logger().Warn("resolving queue grain failed", "queue", queue, "error", err)

		return
	}

	message := &conveyorv1.FireGroup{Queue: queue, Group: group, Type: taskType}
	if err := system.TellGrain(ctx, identity, message); err != nil {
		runtime.Logger().Warn("firing group failed", "queue", queue, "group", group, "error", err)
	}
}

// ---- QueueGrain: lease and dispatch a fired group ----

// recordBatchCompletion applies one finished batch: its members leave the
// active count, the success/failure counters move, and the gateway regains the
// single credit the batch consumed (a batch is one slot regardless of size).
func (x *QueueGrain) recordBatchCompletion(message *conveyorv1.BatchCompleted) {
	counters := x.runtime.Counters()
	total := int64(message.GetTotal())
	succeeded := int64(message.GetSucceeded())

	counters.Active.Add(-total)
	counters.Completed.Add(succeeded)
	counters.Failed.Add(total - succeeded)

	for _, gateway := range x.gateways {
		if gateway.name == message.GetGatewayName() {
			gateway.credits = min(gateway.credits+1, gateway.capacity)

			break
		}
	}

	x.runtime.Logger().Debug("batch completed", "queue", x.queue, "gateway", message.GetGatewayName(), "total", total, "succeeded", succeeded)
}

// fireGroup starts an asynchronous lease of one due aggregation group. The
// capability pre-check skips the lease when no gateway can take this type's
// batch, so a group with no capable worker stays aggregating and the next sweep
// retries. The lease itself runs off the grain turn through PipeToSelf — broker
// I/O never blocks the grain — and dispatch happens in finishGroupLease.
func (x *QueueGrain) fireGroup(ctx *goakt.GrainContext, message *conveyorv1.FireGroup) {
	if x.paused {
		return
	}

	if x.pickBatchGateway(message.GetType()) == nil {
		return
	}

	settings := x.runtime.Settings()
	taskLog := x.runtime.Broker()
	queue := x.queue
	group := message.GetGroup()
	taskType := message.GetType()
	leaseID := x.runtime.NewID()
	expiresAt := timestamppb.New(x.runtime.Clock().Now().Add(settings.LeaseTTL))

	err := ctx.PipeToSelf(func() (any, error) {
		result := &conveyorv1.GroupLeaseCompleted{
			LeaseId:        leaseID,
			LeaseExpiresAt: expiresAt,
			Group:          group,
			Type:           taskType,
		}

		tasks, leaseErr := taskLog.LeaseGroup(context.Background(), queue, group, settings.GroupMaxSize, settings.LeaseTTL, leaseID)
		if leaseErr != nil {
			result.Error = leaseErr.Error()
		} else {
			result.Tasks = tasks
		}

		return result, nil
	})
	if err != nil {
		x.runtime.Logger().Warn("firing group not started", "queue", x.queue, "group", group, "error", err)
	}
}

// finishGroupLease dispatches a leased group as one batch. It re-picks a
// batch-capable gateway because credits may have moved since the lease began;
// when none is available (or the queue paused mid-lease) it releases the batch
// so it redelivers rather than idling until lease expiry.
func (x *QueueGrain) finishGroupLease(ctx *goakt.GrainContext, message *conveyorv1.GroupLeaseCompleted) {
	if message.GetError() != "" {
		x.runtime.Logger().Warn("group lease failed", "queue", x.queue, "group", message.GetGroup(), "error", message.GetError())

		return
	}

	tasks := message.GetTasks()
	if len(tasks) == 0 {
		return
	}

	gateway := x.pickBatchGateway(message.GetType())

	if x.paused || gateway == nil {
		x.releaseLeased(ctx, message.GetLeaseId(), tasks)

		return
	}

	execute := &conveyorv1.ExecuteBatch{
		Tasks:          tasks,
		LeaseId:        message.GetLeaseId(),
		LeaseExpiresAt: message.GetLeaseExpiresAt(),
		Group:          message.GetGroup(),
	}

	if err := ctx.TellActor(gateway.name, execute); err != nil {
		x.removeGateway(gateway.name)
		x.runtime.Logger().Warn("gateway unreachable; dropped from queue", "queue", x.queue, "gateway", gateway.name, "error", err)
		x.releaseLeased(ctx, message.GetLeaseId(), tasks)

		return
	}

	gateway.credits--

	counters := x.runtime.Counters()
	counters.Dispatched.Add(int64(len(tasks)))
	counters.Active.Add(int64(len(tasks)))

	x.runtime.Logger().Debug("group dispatched", "queue", x.queue, "group", message.GetGroup(), "members", len(tasks), "gateway", gateway.name)
}

// pickBatchGateway returns the next gateway, in round-robin order, that has a
// free credit and advertises taskType as batch-capable; nil when none does.
func (x *QueueGrain) pickBatchGateway(taskType string) *gatewayCredits {
	for range x.gateways {
		gateway := x.gateways[x.nextGateway%len(x.gateways)]
		x.nextGateway++

		if gateway.credits > 0 && slices.Contains(gateway.batchTypes, taskType) {
			return gateway
		}
	}

	return nil
}

// ---- Gateway: deliver a batch to the worker and apply its result ----

// dispatchBatch forwards one fired aggregation group down the worker stream as
// a single BatchDispatch and tracks its members until the batch result arrives.
// A send failure releases the whole batch, mirroring dispatch.
func (g *Gateway) dispatchBatch(message *conveyorv1.ExecuteBatch) {
	tasks := message.GetTasks()
	if len(tasks) == 0 {
		return
	}

	leaseID := message.GetLeaseId()
	now := g.runtime.Clock().Now()
	ids := make([]string, 0, len(tasks))
	deadline := message.GetLeaseExpiresAt().AsTime()

	for _, task := range tasks {
		g.inflight[task.GetId()] = &inflightTask{
			leaseID:      leaseID,
			dispatchedAt: now,
			queue:        task.GetQueue(),
			taskType:     task.GetType(),
			retried:      task.GetRetried(),
			maxRetry:     task.GetOptions().GetMaxRetry(),
		}

		ids = append(ids, task.GetId())
		deadline = tightenDeadline(deadline, task, now)
	}

	g.batches[leaseID] = ids

	frame := &conveyorv1.ServerMessage{
		Frame: &conveyorv1.ServerMessage_BatchDispatch{
			BatchDispatch: &conveyorv1.BatchDispatch{
				Tasks:    tasks,
				Deadline: timestamppb.New(deadline),
				Group:    message.GetGroup(),
			},
		},
	}

	if err := g.sender.Send(frame); err != nil {
		g.runtime.Logger().Warn("batch dispatch send failed; releasing group", "group", message.GetGroup(), "error", err)
		g.releaseBatch(leaseID, ids)
	}
}

// tightenDeadline narrows a running deadline by one member's bounds, mirroring
// executionDeadline: the batch deadline is the earliest of the lease expiry and
// every member's own deadline and now+timeout.
func tightenDeadline(deadline time.Time, task *conveyorv1.TaskEnvelope, now time.Time) time.Time {
	options := task.GetOptions()

	if options.GetDeadline().IsValid() && options.GetDeadline().AsTime().Before(deadline) {
		deadline = options.GetDeadline().AsTime()
	}

	if options.GetTimeout().IsValid() {
		if attempt := now.Add(options.GetTimeout().AsDuration()); attempt.Before(deadline) {
			deadline = attempt
		}
	}

	return deadline
}

// releaseBatch releases an undeliverable batch's members back to pending and
// drops them from tracking; redelivery follows on resume or lease expiry.
func (g *Gateway) releaseBatch(leaseID string, ids []string) {
	taskLog := g.runtime.Broker()

	for _, id := range ids {
		delete(g.inflight, id)

		if err := taskLog.Release(context.Background(), id, leaseID); err != nil && !errors.Is(err, broker.ErrLeaseLost) {
			g.runtime.Logger().Warn("releasing undeliverable batch member failed", "task_id", id, "error", err)
		}

		g.runtime.Counters().Active.Add(-1)
		g.runtime.Counters().Released.Add(1)
	}

	delete(g.batches, leaseID)
}

// batchResult applies a worker-reported batch outcome: each member's durable
// transition runs through applyOutcome, members the worker omitted are released
// (no penalty), and one BatchCompleted reports back — refilling the single
// credit the batch consumed.
func (g *Gateway) batchResult(ctx *goakt.ReceiveContext, message *conveyorv1.BatchResult) {
	results := message.GetResults()

	resultByID := make(map[string]*conveyorv1.Result, len(results))
	for _, result := range results {
		resultByID[result.GetTaskId()] = result
	}

	// Resolve the batch from any known member: all members share one lease.
	var leaseID, queue string
	for _, result := range results {
		if entry, ok := g.inflight[result.GetTaskId()]; ok {
			leaseID = entry.leaseID
			queue = entry.queue

			break
		}
	}

	if leaseID == "" {
		g.runtime.Logger().Debug("batch result for unknown batch dropped", "gateway", g.name)

		return
	}

	members := g.batches[leaseID]
	delete(g.batches, leaseID)

	goCtx := ctx.Context()
	total := 0
	succeeded := 0

	for _, taskID := range members {
		entry, ok := g.inflight[taskID]
		if !ok {
			continue
		}

		delete(g.inflight, taskID)
		total++

		result := resultByID[taskID]
		if result == nil {
			// Omitted from the batch result: released, no retry penalty.
			result = &conveyorv1.Result{TaskId: taskID, Outcome: conveyorv1.TaskOutcome_TASK_OUTCOME_RELEASED}
		}

		if g.applyOutcome(goCtx, entry, result) {
			succeeded++
		}
	}

	g.reportBatchCompletion(ctx, queue, total, succeeded)
}

// reportBatchCompletion tells the queue grain a batch finished: its members
// leave the active count and the one credit the batch held is refilled.
func (g *Gateway) reportBatchCompletion(ctx *goakt.ReceiveContext, queue string, total, succeeded int) {
	identity, ok := g.identities[queue]
	if !ok {
		g.runtime.Logger().Warn("batch completion report dropped: queue not registered", "queue", queue)

		return
	}

	completed := &conveyorv1.BatchCompleted{
		Queue:       queue,
		GatewayName: g.name,
		Total:       int32(total),
		Succeeded:   int32(succeeded),
	}

	if err := ctx.ActorSystem().TellGrain(ctx.Context(), identity, completed); err != nil {
		g.runtime.Logger().Warn("batch completion report failed", "queue", queue, "error", err)
	}
}
