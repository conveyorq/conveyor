package actors

import (
	"context"
	"fmt"
	"strings"

	goakt "github.com/tochemey/goakt/v4/actor"
	"google.golang.org/protobuf/types/known/timestamppb"

	conveyorv1 "github.com/tochemey/conveyor/internal/proto/conveyor/v1"
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
func (g *QueueGrain) OnActivate(ctx context.Context, props *goakt.GrainProps) error {
	runtime, ok := props.ActorSystem().Extension(BrokerExtensionID).(*Runtime)
	if !ok {
		return fmt.Errorf("queue grain %s: extension %q is not registered", props.Identity().Name(), BrokerExtensionID)
	}

	paused, err := runtime.Broker().QueuePaused(ctx, queueFromGrainName(props.Identity().Name()))
	if err != nil {
		return fmt.Errorf("queue grain %s: loading pause flag: %w", props.Identity().Name(), err)
	}

	g.runtime = runtime
	g.queue = queueFromGrainName(props.Identity().Name())
	g.paused = paused
	g.leasing = false
	g.gateways = nil
	g.nextGateway = 0

	runtime.Logger().Debug("queue grain activated", "queue", g.queue, "paused", paused)

	return nil
}

// OnReceive dispatches on the wake-up hints and control messages of §8.1.
func (g *QueueGrain) OnReceive(ctx *goakt.GrainContext) {
	// Every handled branch must complete the message with NoErr or Err;
	// a turn that returns without signaling stalls the sender's tell.
	switch message := ctx.Message().(type) {
	case *conveyorv1.TaskEnqueued, *conveyorv1.TasksAvailable:
		g.maybeLease(ctx)
		ctx.NoErr()

	case *conveyorv1.TaskCompleted:
		g.recordCompletion(message)
		g.maybeLease(ctx)
		ctx.NoErr()

	case *conveyorv1.RegisterGateway:
		g.registerGateway(message)
		g.maybeLease(ctx)
		ctx.NoErr()

	case *conveyorv1.GatewayCredit:
		g.addCredits(message)
		g.maybeLease(ctx)
		ctx.NoErr()

	case *conveyorv1.LeaseCycleCompleted:
		g.finishLeaseCycle(ctx, message)
		ctx.NoErr()

	case *conveyorv1.CancelActive:
		g.broadcastCancel(ctx, message)
		ctx.NoErr()

	case *conveyorv1.DrainQueue:
		g.setPaused(ctx, true)

	case *conveyorv1.ResumeQueue:
		g.setPaused(ctx, false)
		g.maybeLease(ctx)

	case *goakt.StatusFailure:
		// A piped lease cycle failed outside the task function (timeout,
		// breaker). Clear the guard; the next wake-up retries.
		g.leasing = false
		g.runtime.Logger().Warn("queue grain pipe failure", "queue", g.queue, "error", message.Error())
		ctx.NoErr()

	default:
		ctx.Unhandled()
	}
}

// OnDeactivate releases nothing: the grain holds no durable state.
func (g *QueueGrain) OnDeactivate(_ context.Context, _ *goakt.GrainProps) error {
	return nil
}

// recordCompletion updates the core counters from a completion report and
// refills the reporting gateway's credit.
func (g *QueueGrain) recordCompletion(message *conveyorv1.TaskCompleted) {
	counters := g.runtime.Counters()
	counters.Active.Add(-1)

	if message.GetSuccess() {
		counters.Completed.Add(1)
	} else {
		counters.Failed.Add(1)
	}

	for _, gateway := range g.gateways {
		if gateway.name == message.GetGatewayName() {
			gateway.credits = min(gateway.credits+1, gateway.capacity)

			break
		}
	}

	g.runtime.Logger().Debug("task completed", "queue", g.queue, "task_id", message.GetTaskId(), "success", message.GetSuccess())
}

// registerGateway upserts a gateway. A new gateway is granted credits
// equal to its capacity; a re-registration (heartbeat, relocation healing)
// only refreshes the capacity so credits are never double-granted.
func (g *QueueGrain) registerGateway(message *conveyorv1.RegisterGateway) {
	for _, gateway := range g.gateways {
		if gateway.name == message.GetGatewayName() {
			gateway.capacity = message.GetCapacity()

			return
		}
	}

	g.gateways = append(g.gateways, &gatewayCredits{
		name:     message.GetGatewayName(),
		capacity: message.GetCapacity(),
		credits:  message.GetCapacity(),
	})

	g.runtime.Logger().Debug("gateway registered", "queue", g.queue, "gateway", message.GetGatewayName(), "capacity", message.GetCapacity())
}

// addCredits grants returned dispatch credits to a registered gateway,
// capped at its declared capacity so duplicate or hostile credit grants
// can never inflate dispatch beyond what the worker announced.
func (g *QueueGrain) addCredits(message *conveyorv1.GatewayCredit) {
	for _, gateway := range g.gateways {
		if gateway.name == message.GetGatewayName() {
			gateway.credits = min(gateway.credits+message.GetCredits(), gateway.capacity)

			return
		}
	}
}

// totalCredits sums the credits across registered gateways.
func (g *QueueGrain) totalCredits() int {
	total := 0
	for _, gateway := range g.gateways {
		total += int(gateway.credits)
	}

	return total
}

// maybeLease starts an asynchronous lease cycle when the grain is
// unpaused, idle, and has credits. Broker I/O never blocks the grain's
// turn: the cycle runs through PipeToSelf and reports back as a
// LeaseCycleCompleted message.
func (g *QueueGrain) maybeLease(ctx *goakt.GrainContext) {
	if g.paused || g.leasing {
		return
	}

	credits := g.totalCredits()
	if credits == 0 {
		return
	}

	settings := g.runtime.Settings()
	limit := min(credits, settings.LeaseBatchMax)
	leaseID := g.runtime.NewID()
	expiresAt := g.runtime.Clock().Now().Add(settings.LeaseTTL)
	taskLog := g.runtime.Broker()
	queue := g.queue

	g.leasing = true

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
		g.leasing = false
		g.runtime.Logger().Warn("queue grain lease cycle not started", "queue", g.queue, "error", err)
	}
}

// finishLeaseCycle distributes a completed lease cycle round-robin to
// gateways with credits, then starts another cycle while work may remain.
func (g *QueueGrain) finishLeaseCycle(ctx *goakt.GrainContext, message *conveyorv1.LeaseCycleCompleted) {
	g.leasing = false

	if message.GetError() != "" {
		g.runtime.Logger().Warn("lease cycle failed", "queue", g.queue, "error", message.GetError())

		return
	}

	tasks := message.GetTasks()
	if len(tasks) == 0 {
		return
	}

	counters := g.runtime.Counters()

	for _, task := range tasks {
		gateway := g.pickGateway()

		if gateway == nil {
			// Credits vanished mid-cycle (gateway removal). The leased
			// tasks stay in the broker and redeliver on lease expiry,
			// exactly like a gateway crash.
			g.runtime.Logger().Warn("leased tasks without credits; awaiting lease expiry", "queue", g.queue, "task_id", task.GetId())

			continue
		}

		execute := &conveyorv1.ExecuteTask{
			Task:           task,
			LeaseId:        message.GetLeaseId(),
			LeaseExpiresAt: message.GetLeaseExpiresAt(),
		}

		if err := ctx.TellActor(gateway.name, execute); err != nil {
			g.removeGateway(gateway.name)
			g.runtime.Logger().Warn("gateway unreachable; dropped from queue", "queue", g.queue, "gateway", gateway.name, "error", err)

			continue
		}

		gateway.credits--
		counters.Dispatched.Add(1)
		counters.Active.Add(1)

		g.runtime.Logger().Debug("task dispatched", "queue", g.queue, "task_id", task.GetId(), "gateway", gateway.name)
	}

	// A non-empty batch may mean more work is due; run another cycle.
	// maybeLease itself guards pause state and remaining credits.
	g.maybeLease(ctx)
}

// pickGateway returns the next gateway with credits in round-robin order,
// or nil when none has capacity left.
func (g *QueueGrain) pickGateway() *gatewayCredits {
	for range g.gateways {
		gateway := g.gateways[g.nextGateway%len(g.gateways)]
		g.nextGateway++

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
func (g *QueueGrain) broadcastCancel(ctx *goakt.GrainContext, message *conveyorv1.CancelActive) {
	for _, gateway := range g.gateways {
		if err := ctx.TellActor(gateway.name, message); err != nil {
			g.runtime.Logger().Warn("cancel broadcast failed", "queue", g.queue, "gateway", gateway.name, "task_id", message.GetTaskId(), "error", err)
		}
	}
}

// removeGateway forgets a gateway and its credits.
func (g *QueueGrain) removeGateway(name string) {
	for index, gateway := range g.gateways {
		if gateway.name == name {
			g.gateways = append(g.gateways[:index], g.gateways[index+1:]...)

			return
		}
	}
}

// setPaused persists and applies the queue pause flag. The persistence
// write is synchronous: pause and resume are rare admin operations and the
// flag must be durable before the grain acts on it.
func (g *QueueGrain) setPaused(ctx *goakt.GrainContext, paused bool) {
	if err := g.runtime.Broker().SetQueuePaused(ctx.Context(), g.queue, paused); err != nil {
		g.runtime.Logger().Error("persisting queue pause flag failed", "queue", g.queue, "paused", paused, "error", err)
		ctx.Err(err)

		return
	}

	g.paused = paused
	g.runtime.Logger().Info("queue pause flag changed", "queue", g.queue, "paused", paused)
	ctx.NoErr()
}
