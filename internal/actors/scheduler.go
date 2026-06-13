package actors

import (
	"context"
	"fmt"

	goakt "github.com/tochemey/goakt/v4/actor"

	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
)

// promoteScheduleRef is the scheduler's stable tick reference. The
// scheduler singleton schedules its own promotion ticks on start; the
// reference lets the entry be canceled or replaced rather than duplicated.
const promoteScheduleRef = "conveyor-scheduler-promote"

// queueGrainFactory creates an empty QueueGrain shell; all state is
// rebuilt from the broker in OnActivate.
func queueGrainFactory(_ context.Context) (goakt.Grain, error) {
	return new(QueueGrain), nil
}

// wakeQueue tells a queue grain that due work exists. Resolving the
// identity activates the grain if it is not live anywhere in the cluster.
// Wake-ups are best-effort hints (the reaper sweep backstops lost ones),
// so failures are logged, never propagated.
func wakeQueue(ctx context.Context, system goakt.ActorSystem, runtime *Runtime, queue string, hint int64) {
	identity, err := system.GrainIdentity(ctx, QueueGrainName(queue), queueGrainFactory,
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

// promote runs one promotion pass and wakes the queues that gained work.
func (s *Scheduler) promote(ctx *goakt.ReceiveContext) {
	background := context.Background()

	queues, err := s.runtime.Broker().PromoteScheduled(background, s.runtime.Settings().LeaseBatchMax)
	if err != nil {
		s.runtime.Logger().Warn("promoting scheduled tasks failed", "error", err)

		return
	}

	for _, queue := range queues {
		wakeQueue(background, ctx.ActorSystem(), s.runtime, queue, 0)
	}
}
