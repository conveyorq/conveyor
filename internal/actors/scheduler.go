// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package actors

import (
	"context"
	"errors"
	"fmt"
	"time"

	goakt "github.com/tochemey/goakt/v4/actor"

	"github.com/conveyorq/conveyor/internal/broker"
	"github.com/conveyorq/conveyor/internal/cron"
	"github.com/conveyorq/conveyor/internal/metrics"
	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
)

// promoteScheduleRef is the scheduler's stable tick reference. The
// scheduler singleton schedules its own promotion ticks on start; the
// reference lets the entry be canceled or replaced rather than duplicated.
const promoteScheduleRef = "conveyor-scheduler-promote"

// wakeQueue tells a queue grain that due work exists. Resolving the
// identity activates the grain if it is not live anywhere in the cluster.
// Wake-ups are best-effort hints (the reaper sweep backstops lost ones),
// so failures are logged, never propagated.
func wakeQueue(ctx context.Context, system goakt.ActorSystem, runtime *Runtime, queue string, hint int64) {
	message := &conveyorv1.TasksAvailable{Queue: queue, Hint: hint}
	tellQueueGrain(ctx, system, runtime, queue, message, "waking queue grain failed", nil)
}

// Scheduler is the promotion loop: on every PromoteTick it moves due
// scheduled tasks to pending and wakes the affected queue grains. Delayed
// tasks live in the broker, never as per-task actor timers. Cron
// materialization lands here in a later phase.
type Scheduler struct {
	// runtime is the engine runtime.
	runtime *Runtime
	// passes runs one promotion pass at a time off the mailbox turn.
	passes passGate
}

// enforce interface compliance at compile time.
var _ goakt.Actor = (*Scheduler)(nil)

// NewScheduler returns a scheduler actor backed by the runtime.
func NewScheduler() *Scheduler {
	return &Scheduler{}
}

// PreStart implements goakt.Actor.
func (s *Scheduler) PreStart(ctx *goakt.Context) error {
	runtime, ok := ctx.ActorSystem().Extension(BrokerExtensionID).(*Runtime)
	if !ok {
		return fmt.Errorf("scheduler %s: extension %q is not registered", ctx.ActorName(), BrokerExtensionID)
	}

	s.runtime = runtime
	// A restart reuses this instance; a pass that ended while it was stopped
	// never reported, so the gate is cleared here.
	s.passes = passGate{}
	return nil
}

// Receive handles promotion ticks. Each pass runs off the turn through the
// pass gate: the mailbox carries only ticks, but a turn that waited on the
// broker would still hold a worker of the dispatcher pool every actor and
// grain on the node shares.
func (s *Scheduler) Receive(ctx *goakt.ReceiveContext) {
	switch ctx.Message().(type) {
	case *goakt.PostStart:
		s.scheduleTicks(ctx)

	case *conveyorv1.PromoteTick:
		system, runtime := ctx.ActorSystem(), s.runtime
		s.passes.start(ctx, func(goCtx context.Context) { promote(goCtx, system, runtime) })

	case passFinished:
		s.passes.finish()

	default:
		ctx.Unhandled()
	}
}

// scheduleTicks arms the recurring promotion tick on the node now hosting
// the singleton. On failover GoAkt relocates the singleton and the new
// host re-arms here, while the previous host's entry stops itself once
// delivery to the departed actor fails.
func (s *Scheduler) scheduleTicks(ctx *goakt.ReceiveContext) {
	interval := s.runtime.Settings().PromoteInterval

	if err := ctx.ActorSystem().Schedule(ctx.Context(), new(conveyorv1.PromoteTick), ctx.Self(), interval, goakt.WithReference(promoteScheduleRef)); err != nil {
		s.runtime.Logger().Error("scheduling promotion ticks failed", "error", err)
	}
}

// PostStop implements goakt.Actor.
func (s *Scheduler) PostStop(_ *goakt.Context) error {
	return nil
}

// promote runs one promotion pass and one cron materialization pass, waking
// the queues that gained work. It is a PipeTo task body: it uses only the
// actor system and runtime it is given.
func promote(goCtx context.Context, system goakt.ActorSystem, runtime *Runtime) {
	queues, err := runtime.Broker().PromoteScheduled(goCtx, runtime.Settings().LeaseBatchMax)
	if err != nil {
		failedPass(goCtx, runtime, metrics.PassPromoteScheduled, "promoting scheduled tasks failed", err)
	} else {
		for _, queue := range queues {
			wakeQueue(goCtx, system, runtime, queue, 0)
		}
	}

	materializeCron(goCtx, system, runtime)
}

// materializeCron fires every cron entry that is due, enqueuing a task for the
// slot and advancing the entry's cursor; a freshly upserted entry (zero next
// run) is armed from its spec without firing. Only the due entries are read.
// Duplicate fires are guarded two ways: the per-slot unique key dedups two
// schedulers racing the same slot during a relocation, and the compare-and-set
// cursor advance keeps a stale scheduler from moving the cursor backward and
// re-firing. A surviving duplicate stays within the at-least-once contract.
func materializeCron(goCtx context.Context, system goakt.ActorSystem, runtime *Runtime) {
	now := runtime.Clock().Now()

	entries, err := runtime.Broker().ListDueCronEntries(goCtx, now)
	if err != nil {
		failedPass(goCtx, runtime, metrics.PassCron, "listing due cron entries failed", err)

		return
	}

	for _, entry := range entries {
		if entry.NextRunAt.IsZero() {
			armCron(goCtx, runtime, entry, now)

			continue
		}

		fireCron(goCtx, system, runtime, entry, now)
	}
}

// armCron computes a new entry's first fire time and persists it without
// firing, so a just-created entry waits for its first real slot.
func armCron(ctx context.Context, runtime *Runtime, entry *broker.CronEntry, now time.Time) {
	next, err := cron.NextFire(entry.Spec, now)
	if err != nil {
		failedPass(ctx, runtime, metrics.PassCron, "arming cron entry failed", err, "id", entry.ID)

		return
	}

	// Expected is the zero time: arm only while the entry is still unarmed.
	if err := runtime.Broker().UpdateCronNextRun(ctx, entry.ID, time.Time{}, next); err != nil {
		failedPass(ctx, runtime, metrics.PassCron, "persisting cron next run failed", err, "id", entry.ID)
	}
}

// fireCron materializes the due slot and advances the cursor past now. A
// duplicate-task outcome is success: another scheduler already fired the slot.
// A real enqueue error leaves the cursor unchanged so the next tick retries.
func fireCron(goCtx context.Context, system goakt.ActorSystem, runtime *Runtime, entry *broker.CronEntry, now time.Time) {
	task := cron.MaterializeTask(entry, entry.NextRunAt, runtime.NewID())

	switch err := runtime.Broker().Enqueue(goCtx, task); {
	case err == nil:
		runtime.Counters().Enqueued.Add(1)
		wakeQueue(goCtx, system, runtime, entry.Queue, 0)

	case errors.Is(err, broker.ErrDuplicateTask):
		// Slot already materialized (failover/double-tick); advancing is safe.

	default:
		failedPass(goCtx, runtime, metrics.PassCron, "materializing cron task failed", err, "id", entry.ID)

		return
	}

	next, err := cron.NextFire(entry.Spec, now)
	if err != nil {
		failedPass(goCtx, runtime, metrics.PassCron, "advancing cron entry failed", err, "id", entry.ID)

		return
	}

	// Compare-and-set on the slot we just fired: if another scheduler already
	// advanced, this is a no-op and the cursor never moves backward.
	if err := runtime.Broker().UpdateCronNextRun(goCtx, entry.ID, entry.NextRunAt, next); err != nil {
		failedPass(goCtx, runtime, metrics.PassCron, "persisting cron next run failed", err, "id", entry.ID)
	}
}
