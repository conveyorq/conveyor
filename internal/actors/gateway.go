package actors

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	goakt "github.com/tochemey/goakt/v4/actor"
	"github.com/tochemey/goakt/v4/breaker"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/conveyorq/conveyor/internal/backoff"
	"github.com/conveyorq/conveyor/internal/broker"
	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
)

// gatewayNamePrefix prefixes the actor name of every worker gateway.
const gatewayNamePrefix = "gateway-"

// registerInterval is the cadence of gateway re-registration heartbeats to
// queue grains; re-registration heals grain relocation.
const registerInterval = 30 * time.Second

// retriesExhaustedMessage prefixes the archive reason when a retryable
// failure lands on a task that has no retries left.
const retriesExhaustedMessage = "retries exhausted: "

// canceledByAdminMessage prefixes the archive reason when an admin
// canceled a task mid-execution and its aborted attempt would otherwise
// have been retried.
const canceledByAdminMessage = "canceled by admin: "

// Per-task-type circuit breaker parameters. A type whose recent outcomes
// are mostly retryable failures stops earning immediate credit refills,
// so a dead downstream cannot burn through retries at full speed.
const (
	// breakerFailureRate is the sliding-window failure rate that opens
	// the breaker.
	breakerFailureRate = 0.5
	// breakerMinRequests is the minimum number of recorded outcomes
	// before the failure rate is evaluated.
	breakerMinRequests = 10
	// breakerOpenTimeout is how long an open breaker withholds credit
	// refills before probing again.
	breakerOpenTimeout = 30 * time.Second
)

// errRetryableFailure feeds RETRY outcomes into the circuit breaker's
// failure stats.
var errRetryableFailure = errors.New("retryable task failure")

// deferredCompletion re-enters a completion report that was withheld
// while the task type's circuit breaker was open. It is a plain local
// message: the gateway never relocates.
type deferredCompletion struct {
	// queue is the queue grain owed the completion report.
	queue string
	// taskID identifies the resolved task.
	taskID string
	// success is the recorded outcome of the execution.
	success bool
}

// registerTick asks the gateway to re-announce itself to its queue grains.
// It is a plain local message: the gateway never relocates, so it never
// crosses the serialization boundary.
type registerTick struct{}

// drainSession asks the gateway to release every in-flight task. It runs
// as a normal mailbox turn, serialized with dispatches and results, which
// is what makes the release race-free; the gateway answers sessionDrained.
type drainSession struct{}

// sessionDrained acknowledges a drainSession request.
type sessionDrained struct{}

// drainTimeout bounds how long a session close waits for the drain turn.
const drainTimeout = 10 * time.Second

// FrameSender pushes server frames down one worker session stream. The
// session handler implements it over the ConnectRPC stream; sends must be
// safe for concurrent use.
type FrameSender interface {
	// Send writes one frame to the worker. An error means the stream is
	// broken and the session is ending.
	Send(message *conveyorv1.ServerMessage) error
}

// GatewaySession describes one accepted worker session.
type GatewaySession struct {
	// SessionID is the server-assigned session ULID.
	SessionID string
	// Queues are the queue names the worker serves.
	Queues []string
	// Concurrency is the worker's declared total execution slots; it is
	// the dispatch capacity granted to each declared queue.
	Concurrency int32
}

// inflightTask is the gateway's record of one dispatched, unresolved task.
type inflightTask struct {
	// leaseID scopes every durable transition for this delivery.
	leaseID string
	// queue is the queue the task was leased from.
	queue string
	// taskType is the handler routing key, keying the circuit breaker.
	taskType string
	// retried is the task's retry counter at dispatch time.
	retried int32
	// maxRetry is the task's retry budget.
	maxRetry int32
	// cancelRequested records an admin cancel for this delivery: an
	// aborted attempt archives instead of retrying.
	cancelRequested bool
}

// Gateway is the per-session bridge between the actor world and one worker
// stream. It is the only component that executes durable transitions
// for its worker's tasks: queue grains dispatch ExecuteTask to it, worker
// frames are forwarded to it by the session handler, and on stream close it
// releases everything still in flight.
//
// Construction carries only the stream binding (session and sender); every
// rebuildable field is set in PreStart, so a supervision restart always
// starts from clean state. The stream binding itself cannot be rebuilt on
// another node, which is why the gateway is spawned relocation-disabled and
// dies with its stream.
type Gateway struct {
	// session identifies the worker stream this gateway serves.
	session GatewaySession
	// sender pushes frames down the worker stream.
	sender FrameSender

	// runtime is the engine runtime, resolved from the system extension in
	// PreStart.
	runtime *Runtime
	// strategy computes retry backoff delays.
	strategy backoff.Strategy
	// name is this gateway's actor name, resolved at start.
	name string
	// identities caches the queue grain identity per declared queue.
	identities map[string]*goakt.GrainIdentity
	// inflight tracks dispatched tasks by id until their result arrives.
	inflight map[string]*inflightTask
	// breakers holds the per-task-type circuit breakers.
	breakers map[string]*breaker.CircuitBreaker
}

// enforce interface compliance at compile time.
var _ goakt.Actor = (*Gateway)(nil)

// newGateway binds a gateway to one accepted worker session stream.
func newGateway(session GatewaySession, sender FrameSender) *Gateway {
	return &Gateway{session: session, sender: sender}
}

// PreStart resolves the engine runtime from the system extension and
// initializes the gateway's working state. It runs on first start and again
// on every supervision restart, so all rebuildable state is reset here.
func (g *Gateway) PreStart(ctx *goakt.Context) error {
	runtime, ok := ctx.Extension(BrokerExtensionID).(*Runtime)
	if !ok {
		return fmt.Errorf("gateway %s: extension %q is not registered", g.session.SessionID, BrokerExtensionID)
	}

	g.runtime = runtime
	g.strategy = backoff.New(backoff.DefaultBase, backoff.DefaultCap)
	g.identities = make(map[string]*goakt.GrainIdentity, len(g.session.Queues))
	g.inflight = make(map[string]*inflightTask)
	g.breakers = make(map[string]*breaker.CircuitBreaker)

	return nil
}

// Receive bridges queue grain dispatches and worker frames.
func (g *Gateway) Receive(ctx *goakt.ReceiveContext) {
	switch message := ctx.Message().(type) {
	case *goakt.PostStart:
		g.name = ctx.Self().Name()
		g.register(ctx)

	case registerTick:
		g.register(ctx)

	case drainSession:
		g.drain(ctx)
		ctx.Response(sessionDrained{})

	case *conveyorv1.ExecuteTask:
		g.dispatch(message)

	case *conveyorv1.Result:
		g.result(ctx, message)

	case *conveyorv1.Heartbeat:
		g.heartbeat(message)

	case *conveyorv1.Credit:
		g.credit(ctx, message)

	case *conveyorv1.CancelActive:
		g.cancelActive(message)

	case deferredCompletion:
		g.reportCompletion(ctx, message.queue, message.taskID, message.success)

	default:
		ctx.Unhandled()
	}
}

// PostStop only logs: it runs outside the mailbox and must not touch actor
// state. In-flight releases happen in the drain turn that every session
// close requests before stopping; anything that slips past it (an actor
// panic, a hard node death) is recovered by lease expiry.
func (g *Gateway) PostStop(_ *goakt.Context) error {
	g.runtime.Logger().Debug("gateway stopped", "session_id", g.session.SessionID)

	return nil
}

// drain releases every in-flight task so the broker redelivers it
// immediately with no retry penalty. This single path covers graceful
// worker shutdown and worker crashes alike: the session handler requests
// it whenever the stream ends, for any reason.
func (g *Gateway) drain(ctx *goakt.ReceiveContext) {
	goCtx := ctx.Context()
	taskLog := g.runtime.Broker()

	for taskID, entry := range g.inflight {
		err := taskLog.Release(goCtx, taskID, entry.leaseID)
		if err != nil && !errors.Is(err, broker.ErrLeaseLost) {
			g.runtime.Logger().Warn("releasing in-flight task on session close failed", "task_id", taskID, "error", err)
		}

		g.runtime.Counters().Active.Add(-1)
	}

	g.inflight = make(map[string]*inflightTask)
	g.runtime.Logger().Debug("gateway drained", "gateway", g.name, "session_id", g.session.SessionID)
}

// register announces this gateway and its capacity to every declared queue
// grain. It runs at start and on every registerTick; re-registration only
// refreshes capacity on the grain side, so credits are never double-granted.
func (g *Gateway) register(ctx *goakt.ReceiveContext) {
	goCtx := ctx.Context()
	system := ctx.ActorSystem()

	for _, queue := range g.session.Queues {
		identity, err := system.GrainIdentity(goCtx, QueueGrainName(queue), queueGrainFactory,
			goakt.WithGrainDeactivateAfter(g.runtime.Settings().PassivateAfter))
		if err != nil {
			g.runtime.Logger().Warn("resolving queue grain failed; next tick retries", "queue", queue, "error", err)

			continue
		}

		g.identities[queue] = identity

		err = system.TellGrain(goCtx, identity, &conveyorv1.RegisterGateway{
			Queue:       queue,
			GatewayName: g.name,
			Capacity:    g.session.Concurrency,
		})
		if err != nil {
			g.runtime.Logger().Warn("gateway registration failed; next tick retries", "queue", queue, "error", err)
		}
	}
}

// dispatch forwards one leased task down the worker stream and tracks it
// until its result arrives. A send failure means the stream is broken: the
// task is released immediately instead of waiting for lease expiry; the
// session handler is about to stop this gateway.
func (g *Gateway) dispatch(message *conveyorv1.ExecuteTask) {
	task := message.GetTask()

	g.inflight[task.GetId()] = &inflightTask{
		leaseID:  message.GetLeaseId(),
		queue:    task.GetQueue(),
		taskType: task.GetType(),
		retried:  task.GetRetried(),
		maxRetry: task.GetOptions().GetMaxRetry(),
	}

	frame := &conveyorv1.ServerMessage{
		Frame: &conveyorv1.ServerMessage_Dispatch{
			Dispatch: &conveyorv1.Dispatch{
				Task:     task,
				Deadline: g.executionDeadline(message),
			},
		},
	}

	if err := g.sender.Send(frame); err != nil {
		delete(g.inflight, task.GetId())

		releaseErr := g.runtime.Broker().Release(context.Background(), task.GetId(), message.GetLeaseId())
		if releaseErr != nil && !errors.Is(releaseErr, broker.ErrLeaseLost) {
			g.runtime.Logger().Warn("releasing undeliverable task failed", "task_id", task.GetId(), "error", releaseErr)
		}

		g.runtime.Counters().Active.Add(-1)
		g.runtime.Logger().Warn("dispatch send failed; task released", "task_id", task.GetId(), "error", err)
	}
}

// executionDeadline computes the effective deadline of one delivery:
// min(lease expiry, task deadline, now + task timeout).
func (g *Gateway) executionDeadline(message *conveyorv1.ExecuteTask) *timestamppb.Timestamp {
	deadline := message.GetLeaseExpiresAt().AsTime()
	options := message.GetTask().GetOptions()

	if options.GetDeadline().IsValid() && options.GetDeadline().AsTime().Before(deadline) {
		deadline = options.GetDeadline().AsTime()
	}

	if options.GetTimeout().IsValid() {
		attemptDeadline := g.runtime.Clock().Now().Add(options.GetTimeout().AsDuration())

		if attemptDeadline.Before(deadline) {
			deadline = attemptDeadline
		}
	}

	return timestamppb.New(deadline)
}

// result maps a worker-reported outcome to its durable transition and
// reports completion to the queue grain, which doubles as the credit
// refill. A result for an unknown task id is dropped: it is either a
// duplicate or a leftover from a delivery whose lease was already lost.
func (g *Gateway) result(ctx *goakt.ReceiveContext, message *conveyorv1.Result) {
	entry, ok := g.inflight[message.GetTaskId()]
	if !ok {
		g.runtime.Logger().Debug("result for unknown task dropped", "task_id", message.GetTaskId(), "gateway", g.name)

		return
	}

	delete(g.inflight, message.GetTaskId())

	goCtx := ctx.Context()
	taskLog := g.runtime.Broker()
	taskID := message.GetTaskId()

	var err error

	switch message.GetOutcome() {
	case conveyorv1.TaskOutcome_TASK_OUTCOME_SUCCESS:
		err = taskLog.Ack(goCtx, taskID, entry.leaseID, message.GetResult())

	case conveyorv1.TaskOutcome_TASK_OUTCOME_RETRY:
		switch {
		case entry.cancelRequested:
			// The admin canceled this delivery; the handler aborted and
			// must not earn a retry.
			err = taskLog.Archive(goCtx, taskID, entry.leaseID, canceledByAdminMessage+message.GetErrorMsg())

		case entry.retried >= entry.maxRetry:
			err = taskLog.Archive(goCtx, taskID, entry.leaseID, retriesExhaustedMessage+message.GetErrorMsg())

		default:
			processAt := g.runtime.Clock().Now().Add(g.strategy.Delay(entry.retried))
			err = taskLog.Fail(goCtx, taskID, entry.leaseID, message.GetErrorMsg(), processAt)
		}

	case conveyorv1.TaskOutcome_TASK_OUTCOME_SKIP_RETRY:
		err = taskLog.Archive(goCtx, taskID, entry.leaseID, message.GetErrorMsg())

	case conveyorv1.TaskOutcome_TASK_OUTCOME_RELEASED:
		err = taskLog.Release(goCtx, taskID, entry.leaseID)

	default:
		// The session handler validates outcomes before forwarding; an
		// unknown value here means a version skew. Release: redelivery is
		// always safe under the at-least-once contract.
		err = taskLog.Release(goCtx, taskID, entry.leaseID)
	}

	if err != nil {
		if errors.Is(err, broker.ErrLeaseLost) {
			g.runtime.Logger().Debug("result discarded: lease lost to another delivery", "task_id", taskID)
		} else {
			g.runtime.Logger().Warn("durable transition failed", "task_id", taskID, "error", err)
		}
	}

	success := message.GetOutcome() == conveyorv1.TaskOutcome_TASK_OUTCOME_SUCCESS && err == nil

	g.recordOutcome(entry.taskType, message.GetOutcome())

	if g.breakerFor(entry.taskType).State() == breaker.Open {
		g.deferCompletion(ctx, entry.queue, taskID, success)

		return
	}

	g.reportCompletion(ctx, entry.queue, taskID, success)
}

// breakerFor returns the task type's circuit breaker, creating it on
// first use.
func (g *Gateway) breakerFor(taskType string) *breaker.CircuitBreaker {
	circuitBreaker, exists := g.breakers[taskType]

	if !exists {
		circuitBreaker = breaker.NewCircuitBreaker(
			breaker.WithFailureRate(breakerFailureRate),
			breaker.WithMinRequests(breakerMinRequests),
			breaker.WithOpenTimeout(breakerOpenTimeout),
		)
		g.breakers[taskType] = circuitBreaker
	}

	return circuitBreaker
}

// recordOutcome feeds one execution outcome into the task type's circuit
// breaker: RETRY counts as a failure, SUCCESS as a success, and all other
// outcomes (release, skip-retry) carry no health signal.
func (g *Gateway) recordOutcome(taskType string, outcome conveyorv1.TaskOutcome) {
	var failed bool

	switch outcome {
	case conveyorv1.TaskOutcome_TASK_OUTCOME_SUCCESS:
		failed = false

	case conveyorv1.TaskOutcome_TASK_OUTCOME_RETRY:
		failed = true

	default:
		return
	}

	// Execute with an instant probe records the outcome in the breaker's
	// sliding window; while open it rejects, which simply drops the
	// sample.
	_, _ = g.breakerFor(taskType).Execute(context.Background(), func(context.Context) (any, error) {
		if failed {
			return nil, errRetryableFailure
		}

		return nil, nil
	})
}

// deferCompletion withholds a completion report — and with it the queue
// grain's credit refill — until the open breaker's timeout lapses, so a
// failing task type drains at probe speed instead of full speed. If the
// deferral cannot be scheduled the report goes out immediately: losing
// throughput capping is safer than losing a credit.
func (g *Gateway) deferCompletion(ctx *goakt.ReceiveContext, queue, taskID string, success bool) {
	deferred := deferredCompletion{queue: queue, taskID: taskID, success: success}

	if err := ctx.ActorSystem().ScheduleOnce(ctx.Context(), deferred, ctx.Self(), breakerOpenTimeout); err != nil {
		g.runtime.Logger().Warn("deferring completion failed; reporting immediately", "task_id", taskID, "error", err)
		g.reportCompletion(ctx, queue, taskID, success)

		return
	}

	g.runtime.Logger().Debug("completion withheld: circuit open for task type", "task_id", taskID, "queue", queue)
}

// heartbeat extends the lease of every task the worker reports as still
// executing. A lost lease means another delivery owns the task now: the
// worker is told to cancel and the slot is reported back to the grain.
func (g *Gateway) heartbeat(message *conveyorv1.Heartbeat) {
	goCtx := context.Background()
	taskLog := g.runtime.Broker()
	ttl := g.runtime.Settings().LeaseTTL

	for _, taskID := range message.GetActiveTaskIds() {
		entry, ok := g.inflight[taskID]
		if !ok {
			continue
		}

		err := taskLog.ExtendLease(goCtx, taskID, entry.leaseID, ttl)
		if err == nil {
			continue
		}

		if !errors.Is(err, broker.ErrLeaseLost) {
			g.runtime.Logger().Warn("lease extension failed", "task_id", taskID, "error", err)

			continue
		}

		delete(g.inflight, taskID)
		g.runtime.Counters().Active.Add(-1)

		cancel := &conveyorv1.ServerMessage{
			Frame: &conveyorv1.ServerMessage_Cancel{Cancel: &conveyorv1.Cancel{TaskId: taskID}},
		}

		if sendErr := g.sender.Send(cancel); sendErr != nil {
			g.runtime.Logger().Warn("cancel frame send failed", "task_id", taskID, "error", sendErr)
		}

		g.runtime.Logger().Debug("lease lost; worker canceled", "task_id", taskID, "gateway", g.name)
	}
}

// cancelActive forwards a best-effort Cancel frame for an admin-canceled
// task this session is executing; an unknown id belongs to another session
// and is dropped. The delivery is marked so the handler's aborted attempt
// archives instead of earning a retry; only a genuine success outcome
// still completes the task.
func (g *Gateway) cancelActive(message *conveyorv1.CancelActive) {
	entry, ok := g.inflight[message.GetTaskId()]
	if !ok {
		return
	}

	entry.cancelRequested = true

	cancel := &conveyorv1.ServerMessage{
		Frame: &conveyorv1.ServerMessage_Cancel{Cancel: &conveyorv1.Cancel{TaskId: message.GetTaskId()}},
	}

	if err := g.sender.Send(cancel); err != nil {
		g.runtime.Logger().Warn("admin cancel frame send failed", "task_id", message.GetTaskId(), "error", err)

		return
	}

	g.runtime.Logger().Debug("admin cancel forwarded to worker", "task_id", message.GetTaskId(), "gateway", g.name)
}

// credit forwards worker-opened slots to every declared queue grain. The
// grain caps credits at the declared capacity, so this can never inflate
// dispatch beyond what registration granted.
func (g *Gateway) credit(ctx *goakt.ReceiveContext, message *conveyorv1.Credit) {
	goCtx := ctx.Context()
	system := ctx.ActorSystem()

	for queue, identity := range g.identities {
		grant := &conveyorv1.GatewayCredit{
			Queue:       queue,
			GatewayName: g.name,
			Credits:     message.GetN(),
		}

		if err := system.TellGrain(goCtx, identity, grant); err != nil {
			g.runtime.Logger().Warn("credit grant failed", "queue", queue, "error", err)
		}
	}
}

// reportCompletion tells the task's queue grain that one execution slot is
// free again. The grain decrements its active count and refills one credit.
func (g *Gateway) reportCompletion(ctx *goakt.ReceiveContext, queue, taskID string, success bool) {
	identity, ok := g.identities[queue]
	if !ok {
		g.runtime.Logger().Warn("completion report dropped: queue not registered", "queue", queue, "task_id", taskID)

		return
	}

	completed := &conveyorv1.TaskCompleted{
		TaskId:      taskID,
		Queue:       queue,
		Success:     success,
		GatewayName: g.name,
	}

	if err := ctx.ActorSystem().TellGrain(ctx.Context(), identity, completed); err != nil {
		g.runtime.Logger().Warn("completion report failed", "task_id", taskID, "error", err)
	}
}

// GatewayHandle lets the session handler drive its gateway actor: worker
// frames are forwarded with Tell, and Stop releases everything in flight
// when the stream ends.
type GatewayHandle struct {
	// pid is the gateway actor.
	pid *goakt.PID
	// logger reports drain failures on session close.
	logger *slog.Logger
}

// Tell forwards one worker frame to the gateway actor.
func (h *GatewayHandle) Tell(ctx context.Context, message any) error {
	return goakt.Tell(ctx, h.pid, message)
}

// Stop drains the gateway — releasing every in-flight task for immediate
// redelivery — and shuts it down. The drain runs as a mailbox turn so it
// serializes with dispatches and results; a drain failure is logged and
// the shutdown proceeds, with lease expiry as the recovery backstop.
func (h *GatewayHandle) Stop(ctx context.Context) error {
	if _, err := goakt.Ask(ctx, h.pid, drainSession{}, drainTimeout); err != nil {
		h.logger.Warn("gateway drain failed; lease expiry will recover", "gateway", h.pid.Name(), "error", err)
	}

	return h.pid.Shutdown(ctx)
}

// SpawnGateway starts the gateway actor for one accepted worker session and
// schedules its re-registration heartbeat. The actor is long-lived (it must
// not passivate while the stream lives) and relocation-disabled (it is
// bound to its node-local stream and must die with its node).
func (e *Engine) SpawnGateway(ctx context.Context, session GatewaySession, sender FrameSender) (*GatewayHandle, error) {
	gateway := newGateway(session, sender)

	pid, err := e.system.Spawn(ctx, gatewayNamePrefix+session.SessionID, gateway,
		goakt.WithLongLived(), goakt.WithRelocationDisabled())
	if err != nil {
		return nil, fmt.Errorf("spawning gateway: %w", err)
	}

	if err := e.system.Schedule(ctx, registerTick{}, pid, registerInterval); err != nil {
		_ = pid.Shutdown(ctx)

		return nil, fmt.Errorf("scheduling gateway registration heartbeat: %w", err)
	}

	return &GatewayHandle{pid: pid, logger: e.runtime.Logger()}, nil
}
