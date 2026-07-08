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
	identity, err := goakt.GrainOf[*QueueGrain](ctx, system, QueueGrainName(queue),
		goakt.WithGrainDeactivateAfter(runtime.Settings().PassivateAfter))
	if err != nil {
		runtime.Logger().Warn("resolving queue grain failed", "queue", queue, "error", err)

		return
	}

	message := &conveyorv1.TasksAvailable{Queue: queue, Hint: hint}
	if err := system.TellGrain(ctx, identity, message); err != nil {
		runtime.Logger().Warn("waking queue grain failed", "queue", queue, "error", err)
	}
}

// Scheduler is the promotion loop: on every PromoteTick it moves due
// scheduled tasks to pending and wakes the affected queue grains. Delayed
// tasks live in the broker, never as per-task actor timers. Cron
// materialization lands here in a later phase.
type Scheduler struct {
	// runtime is the engine runtime.
	runtime *Runtime
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
	return nil
}

// Receive handles promotion ticks. Broker I/O runs synchronously: the
// scheduler's mailbox carries only its own ticks, so blocking one turn
// merely delays the next tick.
func (s *Scheduler) Receive(ctx *goakt.ReceiveContext) {
	switch ctx.Message().(type) {
	case *goakt.PostStart:
		s.scheduleTicks(ctx)

	case *conveyorv1.PromoteTick:
		s.promote(ctx)

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
// the queues that gained work.
func (s *Scheduler) promote(ctx *goakt.ReceiveContext) {
	background := context.Background()

	queues, err := s.runtime.Broker().PromoteScheduled(background, s.runtime.Settings().LeaseBatchMax)
	if err != nil {
		s.runtime.Logger().Warn("promoting scheduled tasks failed", "error", err)
	} else {
		for _, queue := range queues {
			wakeQueue(background, ctx.ActorSystem(), s.runtime, queue, 0)
		}
	}

	s.materializeCron(ctx)
}

// materializeCron fires every cron entry that is due, enqueuing a task for the
// slot and advancing the entry's cursor; a freshly upserted entry (zero next
// run) is armed from its spec without firing. Only the due entries are read.
// Duplicate fires are guarded two ways: the per-slot unique key dedups two
// schedulers racing the same slot during a relocation, and the compare-and-set
// cursor advance keeps a stale scheduler from moving the cursor backward and
// re-firing. A surviving duplicate stays within the at-least-once contract.
func (s *Scheduler) materializeCron(ctx *goakt.ReceiveContext) {
	background := context.Background()
	now := s.runtime.Clock().Now()

	entries, err := s.runtime.Broker().ListDueCronEntries(background, now)
	if err != nil {
		s.runtime.Logger().Warn("listing due cron entries failed", "error", err)

		return
	}

	for _, entry := range entries {
		if entry.NextRunAt.IsZero() {
			s.armCron(background, entry, now)

			continue
		}

		s.fireCron(background, ctx, entry, now)
	}
}

// armCron computes a new entry's first fire time and persists it without
// firing, so a just-created entry waits for its first real slot.
func (s *Scheduler) armCron(ctx context.Context, entry *broker.CronEntry, now time.Time) {
	next, err := cron.NextFire(entry.Spec, now)
	if err != nil {
		s.runtime.Logger().Warn("arming cron entry failed", "id", entry.ID, "error", err)

		return
	}

	// Expected is the zero time: arm only while the entry is still unarmed.
	if err := s.runtime.Broker().UpdateCronNextRun(ctx, entry.ID, time.Time{}, next); err != nil {
		s.runtime.Logger().Warn("persisting cron next run failed", "id", entry.ID, "error", err)
	}
}

// fireCron materializes the due slot and advances the cursor past now. A
// duplicate-task outcome is success: another scheduler already fired the slot.
// A real enqueue error leaves the cursor unchanged so the next tick retries.
func (s *Scheduler) fireCron(background context.Context, ctx *goakt.ReceiveContext, entry *broker.CronEntry, now time.Time) {
	task := cron.MaterializeTask(entry, entry.NextRunAt, s.runtime.NewID())

	switch err := s.runtime.Broker().Enqueue(background, task); {
	case err == nil:
		s.runtime.Counters().Enqueued.Add(1)
		wakeQueue(background, ctx.ActorSystem(), s.runtime, entry.Queue, 0)

	case errors.Is(err, broker.ErrDuplicateTask):
		// Slot already materialized (failover/double-tick); advancing is safe.

	default:
		s.runtime.Logger().Warn("materializing cron task failed", "id", entry.ID, "error", err)

		return
	}

	next, err := cron.NextFire(entry.Spec, now)
	if err != nil {
		s.runtime.Logger().Warn("advancing cron entry failed", "id", entry.ID, "error", err)

		return
	}

	// Compare-and-set on the slot we just fired: if another scheduler already
	// advanced, this is a no-op and the cursor never moves backward.
	if err := s.runtime.Broker().UpdateCronNextRun(background, entry.ID, entry.NextRunAt, next); err != nil {
		s.runtime.Logger().Warn("persisting cron next run failed", "id", entry.ID, "error", err)
	}
}
