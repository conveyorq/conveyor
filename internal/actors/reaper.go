package actors

import (
	"fmt"

	goakt "github.com/tochemey/goakt/v4/actor"

	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
)

// Reaper is the maintenance loop: on every ReapTick it reclaims expired
// leases, purges retention-expired completed tasks, and sweeps for queues
// with due work whose wake-up hints were lost. The sweep is what makes
// enqueue-time wake-ups safe to treat as best-effort.
type Reaper struct {
	// runtime is the engine runtime.
	runtime *Runtime
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
	return nil
}

// Receive handles maintenance ticks. Broker I/O runs synchronously for the
// same reason as the scheduler: the mailbox carries only ticks.
func (r *Reaper) Receive(ctx *goakt.ReceiveContext) {
	switch ctx.Message().(type) {
	case *goakt.PostStart:
		// Lifecycle notification; nothing to initialize.

	case *conveyorv1.ReapTick:
		r.maintain(ctx)

	default:
		ctx.Unhandled()
	}
}

// PostStop implements goakt.Actor.
func (r *Reaper) PostStop(_ *goakt.Context) error {
	return nil
}

// maintain runs one maintenance pass: reap, purge, sweep.
func (r *Reaper) maintain(ctx *goakt.ReceiveContext) {
	goCtx := ctx.Context()
	taskLog := r.runtime.Broker()
	limit := r.runtime.Settings().LeaseBatchMax

	reaped, err := taskLog.ReapExpiredLeases(goCtx, limit)
	if err != nil {
		r.runtime.Logger().Warn("reaping expired leases failed", "error", err)
	}

	for _, queue := range reaped {
		r.runtime.Logger().Debug("expired leases reclaimed", "queue", queue)
		wakeQueue(goCtx, ctx.ActorSystem(), r.runtime, queue, 0)
	}

	if _, err = taskLog.PurgeCompleted(goCtx, limit); err != nil {
		r.runtime.Logger().Warn("purging completed tasks failed", "error", err)
	}

	pending, err := taskLog.PendingCount(goCtx)
	if err != nil {
		// Do not report this error via ctx.Err: the reaper runs under the
		// default Stop directive, so escalating a transient broker failure
		// would stop the actor permanently and end all maintenance. Skipping
		// the sweep is safe because the next ReapTick retries it.
		r.runtime.Logger().Warn("sweeping pending counts failed", "error", err)
		return
	}

	for queue, count := range pending {
		if count > 0 {
			wakeQueue(goCtx, ctx.ActorSystem(), r.runtime, queue, count)
		}
	}
}
