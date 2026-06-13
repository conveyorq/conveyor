package actors

import (
	"context"
	"fmt"
	"strings"

	goakt "github.com/tochemey/goakt/v4/actor"
	"google.golang.org/protobuf/types/known/timestamppb"

	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
)

// queueGrainPrefix prefixes the grain identity name of every queue grain.
// Identity names must match GoAkt's [a-zA-Z0-9][a-zA-Z0-9-_.]* pattern, so
// the separator is a dot rather than the slash used in early designs.
const queueGrainPrefix = "queue."

// QueueGrainName returns the grain identity name for a queue.
func QueueGrainName(queue string) string {
	return queueGrainPrefix + queue
}

// queueFromGrainName recovers the queue name from a grain identity name.
func queueFromGrainName(name string) string {
	return strings.TrimPrefix(name, queueGrainPrefix)
}

// gatewayCredits tracks one registered gateway and its remaining dispatch
// credits.
type gatewayCredits struct {
	// name is the gateway actor name used for TellActor dispatch.
	name string
	// capacity is the gateway's declared concurrency for this queue.
	capacity int32
	// credits is the number of tasks the grain may still dispatch to it.
	credits int32
}

// QueueGrain is the per-queue dispatcher: a virtual actor with exactly one
// live activation cluster-wide. It leases due tasks from the broker when
// gateways have credits and distributes them round-robin. All of its state
// is disposable and rebuilt from the broker on activation.
type QueueGrain struct {
	// runtime is the engine runtime, resolved from the system extension on
	// activation.
	runtime *Runtime
	// queue is the queue this grain dispatches.
	queue string
	// paused mirrors the persisted queue pause flag. While paused, wake-up
	// hints are dropped: they carry no data, undispatched work stays in
	// the broker, and the reaper sweep backstops any gap after resume.
	paused bool
	// leasing guards against overlapping lease cycles.
	leasing bool
	// gateways are the registered gateways in registration order.
	gateways []*gatewayCredits
	// nextGateway is the round-robin cursor into gateways.
	nextGateway int
}

// enforce interface compliance at compile time.
var _ goakt.Grain = (*QueueGrain)(nil)

// OnActivate rebuilds the grain's disposable state from the broker.
func (x *QueueGrain) OnActivate(ctx context.Context, props *goakt.GrainProps) error {
	runtime, ok := props.ActorSystem().Extension(BrokerExtensionID).(*Runtime)
	if !ok {
		return fmt.Errorf("queue grain %s: extension %q is not registered", props.Identity().Name(), BrokerExtensionID)
	}

	paused, err := runtime.Broker().QueuePaused(ctx, queueFromGrainName(props.Identity().Name()))
	if err != nil {
		return fmt.Errorf("queue grain %s: loading pause flag: %w", props.Identity().Name(), err)
	}

	x.runtime = runtime
	x.queue = queueFromGrainName(props.Identity().Name())
	x.paused = paused
	x.leasing = false
	x.gateways = nil
	x.nextGateway = 0

	runtime.Logger().Debug("queue grain activated", "queue", x.queue, "paused", paused)

	return nil
}

// OnReceive dispatches on the wake-up hints and control messages of §8.1.
func (x *QueueGrain) OnReceive(ctx *goakt.GrainContext) {
	// Every handled branch must complete the message with NoErr or Err;
	// a turn that returns without signaling stalls the sender's tell.
	switch message := ctx.Message().(type) {
	case *conveyorv1.TaskEnqueued, *conveyorv1.TasksAvailable:
		x.maybeLease(ctx)
		ctx.NoErr()

	case *conveyorv1.TaskCompleted:
		x.recordCompletion(message)
		x.maybeLease(ctx)
		ctx.NoErr()

	case *conveyorv1.RegisterGateway:
		x.registerGateway(message)
		x.maybeLease(ctx)
		ctx.NoErr()

	case *conveyorv1.GatewayCredit:
		x.addCredits(message)
		x.maybeLease(ctx)
		ctx.NoErr()

	case *conveyorv1.LeaseCycleCompleted:
		x.finishLeaseCycle(ctx, message)
		ctx.NoErr()

	case *conveyorv1.LeasedTasksReleased:
		if message.GetFailed() > 0 {
			x.runtime.Logger().Warn("releasing leased tasks partly failed; remainder awaits lease expiry", "queue", x.queue, "released", message.GetReleased(), "failed", message.GetFailed())
		}

		ctx.NoErr()

	case *conveyorv1.CancelActive:
		x.broadcastCancel(ctx, message)
		ctx.NoErr()

	case *conveyorv1.DrainQueue:
		x.setPaused(ctx, true)

	case *conveyorv1.ResumeQueue:
		x.setPaused(ctx, false)
		x.maybeLease(ctx)

	case *goakt.StatusFailure:
		// A piped lease cycle failed outside the task function (timeout,
		// breaker). Clear the guard; the next wake-up retries.
		x.leasing = false
		x.runtime.Logger().Warn("queue grain pipe failure", "queue", x.queue, "error", message.Error())
		ctx.NoErr()

	default:
		ctx.Unhandled()
	}
}

// OnDeactivate releases nothing: the grain holds no durable state.
func (x *QueueGrain) OnDeactivate(_ context.Context, _ *goakt.GrainProps) error {
	return nil
}

// recordCompletion updates the core counters from a completion report and
// refills the reporting gateway's credit.
func (x *QueueGrain) recordCompletion(message *conveyorv1.TaskCompleted) {
	counters := x.runtime.Counters()
	counters.Active.Add(-1)

	if message.GetSuccess() {
		counters.Completed.Add(1)
	} else {
		counters.Failed.Add(1)
	}

	for _, gateway := range x.gateways {
		if gateway.name == message.GetGatewayName() {
			gateway.credits = min(gateway.credits+1, gateway.capacity)

			break
		}
	}

	x.runtime.Logger().Debug("task completed", "queue", x.queue, "task_id", message.GetTaskId(), "success", message.GetSuccess())
}

// registerGateway upserts a gateway. A new gateway is granted credits
// equal to its capacity; a re-registration (heartbeat, relocation healing)
// only refreshes the capacity so credits are never double-granted.
func (x *QueueGrain) registerGateway(message *conveyorv1.RegisterGateway) {
	for _, gateway := range x.gateways {
		if gateway.name == message.GetGatewayName() {
			gateway.capacity = message.GetCapacity()

			return
		}
	}

	x.gateways = append(x.gateways, &gatewayCredits{
		name:     message.GetGatewayName(),
		capacity: message.GetCapacity(),
		credits:  message.GetCapacity(),
	})

	x.runtime.Logger().Debug("gateway registered", "queue", x.queue, "gateway", message.GetGatewayName(), "capacity", message.GetCapacity())
}

// addCredits grants returned dispatch credits to a registered gateway,
// capped at its declared capacity so duplicate or hostile credit grants
// can never inflate dispatch beyond what the worker announced.
func (x *QueueGrain) addCredits(message *conveyorv1.GatewayCredit) {
	for _, gateway := range x.gateways {
		if gateway.name == message.GetGatewayName() {
			gateway.credits = min(gateway.credits+message.GetCredits(), gateway.capacity)

			return
		}
	}
}

// totalCredits sums the credits across registered gateways.
func (x *QueueGrain) totalCredits() int {
	total := 0
	for _, gateway := range x.gateways {
		total += int(gateway.credits)
	}

	return total
}

// maybeLease starts an asynchronous lease cycle when the grain is
// unpaused, idle, and has credits. Broker I/O never blocks the grain's
// turn: the cycle runs through PipeToSelf and reports back as a
// LeaseCycleCompleted message.
func (x *QueueGrain) maybeLease(ctx *goakt.GrainContext) {
	if x.paused || x.leasing {
		return
	}

	credits := x.totalCredits()
	if credits == 0 {
		return
	}

	settings := x.runtime.Settings()
	limit := min(credits, settings.LeaseBatchMax)
	leaseID := x.runtime.NewID()
	expiresAt := x.runtime.Clock().Now().Add(settings.LeaseTTL)
	taskLog := x.runtime.Broker()
	queue := x.queue

	x.leasing = true

	err := ctx.PipeToSelf(func() (any, error) {
		// Always return a proto result: in cluster mode every grain
		// message crosses the serialization boundary, so broker errors
		// ride inside the message instead of the pipe error path.
		result := &conveyorv1.LeaseCycleCompleted{
			LeaseId:        leaseID,
			LeaseExpiresAt: timestamppb.New(expiresAt),
		}

		tasks, err := taskLog.Lease(context.Background(), queue, limit, settings.LeaseTTL, leaseID)
		if err != nil {
			result.Error = err.Error()
		} else {
			result.Tasks = tasks
		}

		return result, nil
	})
	if err != nil {
		x.leasing = false
		x.runtime.Logger().Warn("queue grain lease cycle not started", "queue", x.queue, "error", err)
	}
}

// finishLeaseCycle distributes a completed lease cycle round-robin to
// gateways with credits, then starts another cycle while work may remain.
func (x *QueueGrain) finishLeaseCycle(ctx *goakt.GrainContext, message *conveyorv1.LeaseCycleCompleted) {
	x.leasing = false

	if message.GetError() != "" {
		x.runtime.Logger().Warn("lease cycle failed", "queue", x.queue, "error", message.GetError())

		return
	}

	tasks := message.GetTasks()
	if len(tasks) == 0 {
		return
	}

	// A pause may have landed while this cycle was in flight: the lease
	// goroutine claimed work after DrainQueue set the flag. A paused queue
	// must not dispatch, so release the whole batch back to pending; it
	// redelivers when the queue resumes.
	if x.paused {
		x.releaseLeased(ctx, message.GetLeaseId(), tasks)

		return
	}

	counters := x.runtime.Counters()

	// undeliverable collects tasks this cycle leased but could not hand to a
	// gateway: a dispatch failure removes the gateway, which is the only way
	// credits run out mid-batch, so both branches share one root cause and
	// one remedy — release them so they redeliver instead of idling until
	// the lease expires.
	var undeliverable []*conveyorv1.TaskEnvelope

	for _, task := range tasks {
		gateway := x.pickGateway()

		if gateway == nil {
			undeliverable = append(undeliverable, task)

			continue
		}

		execute := &conveyorv1.ExecuteTask{
			Task:           task,
			LeaseId:        message.GetLeaseId(),
			LeaseExpiresAt: message.GetLeaseExpiresAt(),
		}

		if err := ctx.TellActor(gateway.name, execute); err != nil {
			x.removeGateway(gateway.name)
			x.runtime.Logger().Warn("gateway unreachable; dropped from queue", "queue", x.queue, "gateway", gateway.name, "error", err)
			undeliverable = append(undeliverable, task)

			continue
		}

		gateway.credits--
		counters.Dispatched.Add(1)
		counters.Active.Add(1)

		x.runtime.Logger().Debug("task dispatched", "queue", x.queue, "task_id", task.GetId(), "gateway", gateway.name)
	}

	if len(undeliverable) > 0 {
		x.releaseLeased(ctx, message.GetLeaseId(), undeliverable)
	}

	// A non-empty batch may mean more work is due; run another cycle.
	// maybeLease itself guards pause state and remaining credits.
	x.maybeLease(ctx)
}

// releaseLeased returns an undispatched leased batch to pending off the
// grain turn, mirroring the lease cycle: the broker round trips run through
// PipeToSelf so they never block the grain's single goroutine, and the
// outcome comes back as a LeasedTasksReleased message. Broker errors ride
// inside that result rather than failing the pipe — a pipe failure routes
// to the StatusFailure handler, which clears the lease guard and would
// corrupt a later cycle. The tasks redeliver when the queue resumes or, for
// any that fail to release, on lease expiry via the reaper.
func (x *QueueGrain) releaseLeased(ctx *goakt.GrainContext, leaseID string, tasks []*conveyorv1.TaskEnvelope) {
	taskLog := x.runtime.Broker()
	queue := x.queue

	err := ctx.PipeToSelf(func() (any, error) {
		result := &conveyorv1.LeasedTasksReleased{}

		for _, task := range tasks {
			if releaseErr := taskLog.Release(context.Background(), task.GetId(), leaseID); releaseErr != nil {
				result.Failed++

				continue
			}

			result.Released++
		}

		return result, nil
	})
	if err != nil {
		x.runtime.Logger().Warn("queue grain release not started; awaiting lease expiry", "queue", queue, "count", len(tasks), "error", err)
	}
}

// pickGateway returns the next gateway with credits in round-robin order,
// or nil when none has capacity left.
func (x *QueueGrain) pickGateway() *gatewayCredits {
	for range x.gateways {
		gateway := x.gateways[x.nextGateway%len(x.gateways)]
		x.nextGateway++

		if gateway.credits > 0 {
			return gateway
		}
	}

	return nil
}

// broadcastCancel forwards an admin cancel request for an active task to
// every registered gateway. Only the session executing the task reacts
// with a worker Cancel frame; the others drop the unknown id. The
// forwarding is best-effort, matching the documented cancel contract for
// active tasks.
func (x *QueueGrain) broadcastCancel(ctx *goakt.GrainContext, message *conveyorv1.CancelActive) {
	for _, gateway := range x.gateways {
		if err := ctx.TellActor(gateway.name, message); err != nil {
			x.runtime.Logger().Warn("cancel broadcast failed", "queue", x.queue, "gateway", gateway.name, "task_id", message.GetTaskId(), "error", err)
		}
	}
}

// removeGateway forgets a gateway and its credits.
func (x *QueueGrain) removeGateway(name string) {
	for index, gateway := range x.gateways {
		if gateway.name == name {
			x.gateways = append(x.gateways[:index], x.gateways[index+1:]...)

			return
		}
	}
}

// setPaused persists and applies the queue pause flag. The persistence
// write is synchronous: pause and resume are rare admin operations and the
// flag must be durable before the grain acts on it.
func (x *QueueGrain) setPaused(ctx *goakt.GrainContext, paused bool) {
	if err := x.runtime.Broker().SetQueuePaused(ctx.Context(), x.queue, paused); err != nil {
		x.runtime.Logger().Error("persisting queue pause flag failed", "queue", x.queue, "paused", paused, "error", err)
		ctx.Err(err)

		return
	}

	x.paused = paused
	x.runtime.Logger().Info("queue pause flag changed", "queue", x.queue, "paused", paused)
	ctx.NoErr()
}
