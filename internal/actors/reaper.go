// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package actors

import (
	"context"
	"fmt"

	goakt "github.com/tochemey/goakt/v4/actor"

	"github.com/conveyorq/conveyor/internal/metrics"
	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
)

// reapScheduleRef is the reaper's stable tick reference. The reaper
// singleton schedules its own maintenance ticks on start; the reference
// lets the entry be canceled or replaced rather than duplicated.
const reapScheduleRef = "conveyor-reaper-reap"

// Reaper is the maintenance loop: on every ReapTick it reclaims expired
// leases, purges retention-expired completed tasks, and sweeps for queues
// with due work whose wake-up hints were lost. The sweep is what makes
// enqueue-time wake-ups safe to treat as best-effort.
type Reaper struct {
	// runtime is the engine runtime.
	runtime *Runtime
	// passes runs one maintenance pass at a time off the mailbox turn.
	passes passGate
}

// enforce interface compliance at compile time.
var _ goakt.Actor = (*Reaper)(nil)

// NewReaper returns a reaper actor backed by the runtime.
func NewReaper() *Reaper {
	return &Reaper{}
}

// PreStart implements goakt.Actor.
func (r *Reaper) PreStart(ctx *goakt.Context) error {
	runtime, ok := ctx.ActorSystem().Extension(BrokerExtensionID).(*Runtime)
	if !ok {
		return fmt.Errorf("reaper %s: extension %q is not registered", ctx.ActorName(), BrokerExtensionID)
	}

	r.runtime = runtime
	// A restart reuses this instance; a pass that ended while it was stopped
	// never reported, so the gate is cleared here.
	r.passes = passGate{}
	return nil
}

// Receive handles maintenance ticks. Each pass runs off the turn through the
// pass gate, so its broker round trips never hold a dispatcher worker.
func (r *Reaper) Receive(ctx *goakt.ReceiveContext) {
	switch ctx.Message().(type) {
	case *goakt.PostStart:
		r.scheduleTicks(ctx)

	case *conveyorv1.ReapTick:
		system, runtime := ctx.ActorSystem(), r.runtime
		r.passes.start(ctx, func(goCtx context.Context) { maintain(goCtx, system, runtime) })

	case passFinished:
		r.passes.finish()

	default:
		ctx.Unhandled()
	}
}

// scheduleTicks arms the recurring maintenance tick on the node now hosting
// the singleton. On failover GoAkt relocates the singleton and the new
// host re-arms here, while the previous host's entry stops itself once
// delivery to the departed actor fails.
func (r *Reaper) scheduleTicks(ctx *goakt.ReceiveContext) {
	interval := r.runtime.Settings().ReapInterval

	if err := ctx.ActorSystem().Schedule(ctx.Context(), new(conveyorv1.ReapTick), ctx.Self(), interval, goakt.WithReference(reapScheduleRef)); err != nil {
		r.runtime.Logger().Error("scheduling maintenance ticks failed", "error", err)
	}
}

// PostStop implements goakt.Actor.
func (r *Reaper) PostStop(_ *goakt.Context) error {
	return nil
}

// maintain runs one maintenance pass: reap, purge, sweep. It is a PipeTo task
// body: it uses only the actor system and runtime it is given.
func maintain(goCtx context.Context, system goakt.ActorSystem, runtime *Runtime) {
	taskLog := runtime.Broker()
	limit := runtime.Settings().LeaseBatchMax

	reaped, err := taskLog.ReapExpiredLeases(goCtx, limit)
	if err != nil {
		failedPass(goCtx, runtime, metrics.PassReap, "reaping expired leases failed", err)
	}

	if len(reaped) > 0 {
		runtime.Metrics().LeaseExpired(goCtx, len(reaped))
	}

	for _, queue := range reaped {
		runtime.Logger().Debug("expired leases reclaimed", "queue", queue)
		wakeQueue(goCtx, system, runtime, queue, 0)
	}

	if _, err = taskLog.PurgeTerminal(goCtx, runtime.Settings().ArchiveRetention, limit); err != nil {
		failedPass(goCtx, runtime, metrics.PassPurge, "purging terminal tasks failed", err)
	}

	if _, err = taskLog.ArchiveExpired(goCtx, limit); err != nil {
		failedPass(goCtx, runtime, metrics.PassArchiveExpired, "archiving expired tasks failed", err)
	}

	// Dependency safety net: promote any task whose dependencies have since
	// reached a terminal state but that an inline resolution missed (a lost
	// wake, an admin cancel, a node with no resolver pool), and wake the queues
	// that received the freed work.
	promoted, err := taskLog.PromoteReadyDependents(goCtx, limit)
	if err != nil {
		failedPass(goCtx, runtime, metrics.PassPromoteDependents, "promoting ready dependents failed", err)
	}

	for _, queue := range promoted {
		wakeQueue(goCtx, system, runtime, queue, 0)
	}

	pending, err := taskLog.PendingCount(goCtx)
	if err != nil {
		// Do not report this error via ctx.Err: the reaper runs under the
		// default Stop directive, so escalating a transient broker failure
		// would stop the actor permanently and end all maintenance. Skipping
		// the sweep is safe because the next ReapTick retries it.
		failedPass(goCtx, runtime, metrics.PassPendingSweep, "sweeping pending counts failed", err)

		return
	}

	swept := 0

	for queue, count := range pending {
		if count > 0 {
			wakeQueue(goCtx, system, runtime, queue, count)
			swept++
		}
	}

	if swept > 0 {
		runtime.Metrics().WakeupsSwept(goCtx, swept)
	}
}

// failedPass records one failed maintenance pass: it logs the error and counts
// it under the pass, so a broker fault that keeps a pass from running is
// visible beyond the log. Extra fields, such as a cron entry id, join the log
// line.
func failedPass(ctx context.Context, runtime *Runtime, pass, message string, err error, fields ...any) {
	runtime.Logger().Warn(message, append([]any{"pass", pass, "error", err}, fields...)...)
	runtime.Metrics().MaintenanceFailure(ctx, pass)
}
