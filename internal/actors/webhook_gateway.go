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
	"github.com/tochemey/goakt/v4/breaker"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/conveyorq/conveyor/internal/backoff"
	"github.com/conveyorq/conveyor/internal/broker"
	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
	"github.com/conveyorq/conveyor/internal/webhook"
)

// webhookGatewayPrefix prefixes the actor name of every webhook gateway; the
// suffix is the registration name, which shares the actor-name grammar.
const webhookGatewayPrefix = "webhook-"

// webhookReconcileRef is the manager's stable tick reference, so a relocated
// manager replaces its reconcile schedule instead of duplicating it.
const webhookReconcileRef = "conveyor-webhook-reconcile"

// webhookRegisterRefPrefix prefixes each webhook gateway's tick reference.
const webhookRegisterRefPrefix = "conveyor-webhook-register-"

// defaultRequestTimeout bounds a synchronous delivery when the registration
// does not set its own request timeout.
const defaultRequestTimeout = 30 * time.Second

// Endpoint circuit breaker parameters. The breaker watches transport
// failures only (connection errors, timeouts, non-200 answers, malformed
// envelopes): a task-level failure is a task problem, not an endpoint
// problem. While open, the gateway withholds capacity so queue grains stop
// leasing to it and tasks stay pending instead of churning through retries.
const (
	// endpointBreakerFailureRate is the sliding-window transport-failure
	// rate that opens the breaker.
	endpointBreakerFailureRate = 0.5
	// endpointBreakerMinRequests is the minimum number of recorded
	// deliveries before the failure rate is evaluated.
	endpointBreakerMinRequests = 3
	// defaultEndpointBreakerOpenTimeout is how long an open breaker
	// withholds capacity before probing again, when the settings carry no
	// override.
	defaultEndpointBreakerOpenTimeout = 30 * time.Second
)

// errTransportFailure feeds transport failures into the endpoint breaker's
// failure stats.
var errTransportFailure = errors.New("webhook transport failure")

// updateRegistration hands a webhook gateway its latest persisted
// registration.
type updateRegistration struct {
	// worker is the registration snapshot.
	worker *broker.WebhookWorker
}

// gatewayStopped reports that a webhook gateway finished draining and shut
// down, so the manager may reuse its name.
type gatewayStopped struct {
	// name is the registration name.
	name string
}

// notificationSent reports a fire-and-forget endpoint notification finished;
// the gateway has nothing to do with it.
type notificationSent struct{}

// webhookDelivery reports one finished delivery attempt back to the webhook
// gateway's mailbox.
type webhookDelivery struct {
	// taskID identifies the delivered task.
	taskID string
	// outcome classifies the endpoint's answer.
	outcome webhook.Outcome
	// message is the failure detail, empty on success.
	message string
}

// webhookBatchDelivery reports one finished batch delivery attempt.
type webhookBatchDelivery struct {
	// leaseID identifies the batch.
	leaseID string
	// outcomes maps each answered member task id to its classification.
	outcomes map[string]webhookDelivery
	// transportError, when non-empty, failed the whole POST: every member
	// retries.
	transportError string
}

// webhookBatch tracks one fired group until every member resolves; only
// then does the batch completion report go out and refill the single credit
// the batch consumed. Members completing synchronously resolve immediately;
// accepted members resolve when their callback (or staleness) does.
type webhookBatch struct {
	// queue is the queue the batch was leased from.
	queue string
	// members are the batch's task ids.
	members []string
	// remaining counts the unresolved members.
	remaining int
	// succeeded counts the members that completed successfully.
	succeeded int
}

// WebhookManager is the cluster singleton that keeps one webhook gateway
// child running per active registration. It reconciles children against the
// broker on a tick, so registrations created, changed, paused, or deleted
// anywhere in the cluster converge here, and a relocated manager (node loss)
// rebuilds every gateway on its new host.
type WebhookManager struct {
	// runtime is the engine runtime.
	runtime *Runtime
	// children maps registration name to the running gateway child.
	children map[string]*goakt.PID
	// stopping holds names whose child is draining; the name cannot be
	// respawned until the drain confirms.
	stopping map[string]bool
}

// enforce interface compliance at compile time.
var _ goakt.Actor = (*WebhookManager)(nil)

// NewWebhookManager returns the webhook manager actor.
func NewWebhookManager() *WebhookManager {
	return &WebhookManager{}
}

// PreStart resolves the runtime and resets the child bookkeeping.
func (m *WebhookManager) PreStart(ctx *goakt.Context) error {
	runtime, ok := ctx.ActorSystem().Extension(BrokerExtensionID).(*Runtime)
	if !ok {
		return fmt.Errorf("webhook manager %s: extension %q is not registered", ctx.ActorName(), BrokerExtensionID)
	}

	m.runtime = runtime
	m.children = make(map[string]*goakt.PID)
	m.stopping = make(map[string]bool)

	return nil
}

// Receive drives reconciliation.
func (m *WebhookManager) Receive(ctx *goakt.ReceiveContext) {
	switch message := ctx.Message().(type) {
	case *goakt.PostStart:
		m.scheduleTicks(ctx)
		m.reconcile(ctx)

	case *conveyorv1.WebhookReconcile:
		m.reconcile(ctx)

	case gatewayStopped:
		delete(m.stopping, message.name)

	default:
		ctx.Unhandled()
	}
}

// PostStop implements goakt.Actor. Children are shut down with their parent
// by the actor system; the next manager host respawns them from the broker.
func (m *WebhookManager) PostStop(_ *goakt.Context) error {
	return nil
}

// scheduleTicks arms the recurring reconcile tick on the node now hosting
// the singleton, exactly like the maintenance singletons arm theirs. A
// manager without its tick is silently dead and nothing else would retry
// the scheduling, so the failure escalates to supervision: the restart
// re-runs PostStart and re-arms.
func (m *WebhookManager) scheduleTicks(ctx *goakt.ReceiveContext) {
	if err := armTick(ctx, new(conveyorv1.WebhookReconcile), webhookReconcileRef); err != nil {
		ctx.Err(fmt.Errorf("scheduling webhook reconcile ticks: %w", err))
	}
}

// reconcile converges the running gateway children onto the persisted
// registrations: active registrations get a child (spawned or refreshed),
// paused and deleted ones lose theirs.
func (m *WebhookManager) reconcile(ctx *goakt.ReceiveContext) {
	workers, err := m.runtime.Broker().ListWebhookWorkers(ctx.Context())
	if err != nil {
		m.runtime.Logger().Warn("listing webhook workers failed; next tick retries", "error", err)

		return
	}

	desired := make(map[string]*broker.WebhookWorker, len(workers))

	for _, worker := range workers {
		if worker.Paused {
			continue
		}

		// A gateway signs every delivery and mints every lease token with the
		// registration's newest secret, so one without a secret cannot start.
		// Skip it instead of spawning a gateway that would panic resolving
		// Secrets[0]; the upsert paths validate this, so a secret-less row is
		// corrupt persisted data, not an expected input.
		if len(worker.Secrets) == 0 {
			m.runtime.Logger().Warn("webhook registration has no secrets; skipping", "registration", worker.Name)

			continue
		}

		desired[worker.Name] = worker
	}

	for name, child := range m.children {
		if _, keep := desired[name]; !keep || !child.IsRunning() {
			m.stopChild(ctx, name, child)
		}
	}

	for name, worker := range desired {
		if m.stopping[name] {
			continue
		}

		child, running := m.children[name]
		if running {
			ctx.Tell(child, updateRegistration{worker: worker})

			continue
		}

		spawned := ctx.Spawn(webhookGatewayPrefix+name, newWebhookGateway(worker),
			goakt.WithLongLived(), goakt.WithRelocationDisabled())
		if spawned == nil {
			m.runtime.Logger().Warn("spawning webhook gateway failed; next tick retries", "registration", name)

			continue
		}

		m.children[name] = spawned
		m.runtime.Logger().Info("webhook gateway started", "registration", name, "url", worker.URL)
	}
}

// stopChild drains a gateway and shuts it down off the mailbox turn. The
// name is reserved until the shutdown confirms, so a re-created registration
// cannot collide with its own draining predecessor.
func (m *WebhookManager) stopChild(ctx *goakt.ReceiveContext, name string, child *goakt.PID) {
	delete(m.children, name)
	m.stopping[name] = true

	logger := m.runtime.Logger()

	ctx.PipeTo(ctx.Self(), func() (any, error) {
		background := context.Background()

		if child.IsRunning() {
			if _, err := goakt.Ask(background, child, drainSession{}, drainTimeout); err != nil {
				logger.Warn("webhook gateway drain failed; lease expiry will recover", "registration", name, "error", err)
			}

			if err := child.Shutdown(background); err != nil {
				logger.Warn("webhook gateway shutdown failed", "registration", name, "error", err)
			}
		}

		return gatewayStopped{name: name}, nil
	})
}

// WebhookGateway is the delivery bridge for one webhook worker registration:
// the webhook analog of the per-session Gateway. Queue grains dispatch to it
// like to any gateway; it delivers each task as a JSON-RPC call to the
// registered endpoint off its mailbox turn, maps the response to the durable
// transition, and reports completion back for the credit refill. A delivery
// the endpoint accepted for asynchronous completion stays in flight, its
// lease driven by the endpoint's Heartbeat callbacks, until its ReportResult
// callback resolves it.
type WebhookGateway struct {
	// registration is the latest registration snapshot.
	registration *broker.WebhookWorker

	// runtime is the engine runtime, resolved in PreStart.
	runtime *Runtime
	// strategy computes default retry backoff delays.
	strategy backoff.Strategy
	// name is this gateway's actor name, resolved at start.
	name string
	// client posts the JSON-RPC calls.
	client *webhook.Client
	// signer stamps the delivery signature headers, keyed by the
	// registration's newest secret.
	signer *webhook.HMACSigner
	// identities caches the queue grain identity per served queue.
	identities map[string]*goakt.GrainIdentity
	// inflight tracks dispatched tasks by id until their delivery resolves.
	inflight map[string]*inflightTask
	// batchStates tracks each fired group by lease id until every member
	// resolves.
	batchStates map[string]*webhookBatch
	// aborts cancels the open HTTP request of each in-flight task, for admin
	// cancel and lost leases.
	aborts map[string]context.CancelFunc
	// async holds each asynchronously completing task's last heartbeat time;
	// membership marks the delivery as endpoint-driven.
	async map[string]time.Time
	// endpointBreaker opens on transport failures; while open the gateway
	// withholds capacity so grains stop leasing to a dead endpoint.
	endpointBreaker *breaker.CircuitBreaker
	// breakerOpenTimeout is how long withheld capacity stays withheld
	// before a probe delivery is allowed to test the endpoint again.
	breakerOpenTimeout time.Duration
	// withholding records that the last registration announced zero
	// capacity, so a recovered breaker knows to re-announce the real one.
	withholding bool
	// withheldAt is when capacity was withheld, driving the probe timer:
	// the breaker transitions out of open only when a call flows, and no
	// call flows at zero capacity, so the gateway owns the probe schedule.
	withheldAt time.Time
	// probing records that the endpoint's open timeout has lapsed and the
	// gateway announced a single slot so exactly one delivery at a time tests
	// the endpoint, instead of restoring full capacity onto a still-dead one.
	probing bool
	// typeBreakers holds the per-task-type circuit breakers, mirroring the
	// stream gateway's completion throttling for failing task types.
	typeBreakers map[string]*breaker.CircuitBreaker
}

// enforce interface compliance at compile time.
var _ goakt.Actor = (*WebhookGateway)(nil)

// newWebhookGateway binds a gateway to one registration snapshot.
func newWebhookGateway(worker *broker.WebhookWorker) *WebhookGateway {
	return &WebhookGateway{registration: worker}
}

// PreStart resolves the engine runtime and initializes the working state; it
// runs again on every supervision restart, so all rebuildable state is reset
// here.
func (w *WebhookGateway) PreStart(ctx *goakt.Context) error {
	runtime, ok := ctx.Extension(BrokerExtensionID).(*Runtime)
	if !ok {
		return fmt.Errorf("webhook gateway %s: extension %q is not registered", ctx.ActorName(), BrokerExtensionID)
	}

	w.runtime = runtime

	w.strategy = runtime.Settings().RetryBackoff
	if w.strategy.Base() <= 0 {
		w.strategy = backoff.New(backoff.DefaultBase, backoff.DefaultCap)
	}

	w.client = webhook.NewClient()
	w.signer = webhook.NewHMACSigner(w.registration.Secrets[0], runtime.Clock())
	w.identities = make(map[string]*goakt.GrainIdentity, len(w.registration.Queues))
	w.inflight = make(map[string]*inflightTask)
	w.batchStates = make(map[string]*webhookBatch)
	w.aborts = make(map[string]context.CancelFunc)
	w.async = make(map[string]time.Time)
	w.withholding = false
	w.probing = false
	w.typeBreakers = make(map[string]*breaker.CircuitBreaker)

	w.breakerOpenTimeout = runtime.Settings().WebhookBreakerOpenTimeout
	if w.breakerOpenTimeout <= 0 {
		w.breakerOpenTimeout = defaultEndpointBreakerOpenTimeout
	}

	w.endpointBreaker = breaker.NewCircuitBreaker(
		breaker.WithFailureRate(endpointBreakerFailureRate),
		breaker.WithMinRequests(endpointBreakerMinRequests),
		breaker.WithOpenTimeout(w.breakerOpenTimeout),
	)

	return nil
}

// Receive bridges queue grain dispatches, endpoint responses, and
// asynchronous-completion callbacks.
func (w *WebhookGateway) Receive(ctx *goakt.ReceiveContext) {
	switch message := ctx.Message().(type) {
	case *goakt.PostStart:
		w.name = ctx.Self().Name()
		w.register(ctx)
		w.scheduleTicks(ctx)

	case registerTick:
		w.syncBreakerCapacity(ctx)
		w.register(ctx)
		w.extendLeases(ctx)
		w.reapStaleAsync(ctx)

	case updateRegistration:
		w.apply(ctx, message.worker)

	case drainSession:
		w.drain(ctx)
		ctx.Response(sessionDrained{})

	case *conveyorv1.ExecuteTask:
		w.deliver(ctx, message)

	case *conveyorv1.ExecuteBatch:
		w.deliverBatch(ctx, message)

	case webhookDelivery:
		w.complete(ctx, message)

	case webhookBatchDelivery:
		w.completeBatch(ctx, message)

	case *conveyorv1.WebhookLeaseHeartbeat:
		w.heartbeatAsync(ctx, message)

	case *conveyorv1.WebhookLeaseResult:
		w.resultAsync(ctx, message)

	case *conveyorv1.CancelActive:
		w.cancelActive(ctx, message)

	case deferredCompletion:
		w.reportCompletion(ctx, message.queue, message.taskID, message.success)

	case notificationSent:

	default:
		ctx.Unhandled()
	}
}

// PostStop only logs: it runs outside the mailbox and must not touch actor
// state. The manager drains this gateway before stopping it; anything that
// slips past (a panic, a node death) is recovered by lease expiry.
func (w *WebhookGateway) PostStop(_ *goakt.Context) error {
	w.runtime.Logger().Debug("webhook gateway stopped", "registration", w.registration.Name)

	return nil
}

// scheduleTicks arms this gateway's re-registration and lease-extension
// tick; the reference is name-scoped so gateways never collide. A gateway
// without its tick is silently dead (no registration heal, no lease
// extension, no stale-async reaping) and nothing else would retry the
// scheduling, so the failure escalates to supervision: the restart re-runs
// PostStart and re-arms.
func (w *WebhookGateway) scheduleTicks(ctx *goakt.ReceiveContext) {
	if err := armTick(ctx, registerTick{}, webhookRegisterRefPrefix+w.registration.Name); err != nil {
		ctx.Err(fmt.Errorf("scheduling webhook gateway ticks for %s: %w", w.registration.Name, err))
	}
}

// armTick replaces the schedule registered under reference with a fresh
// recurring tick for the current actor. The predecessor's entry may still
// exist (a paused registration resuming before the old entry noticed its
// actor died), so a stale entry is canceled first: the reference identifies
// the role, not the actor incarnation.
func armTick(ctx *goakt.ReceiveContext, message any, reference string) error {
	system := ctx.ActorSystem()

	// A cancel failure only matters if the subsequent Schedule also fails;
	// canceling an absent reference is the common case.
	_ = system.CancelSchedule(reference)

	return system.Schedule(ctx.Context(), message, ctx.Self(), registerInterval, goakt.WithReference(reference))
}

// register announces this gateway and its capacity to every served queue
// grain, exactly like a stream gateway announces its session.
func (w *WebhookGateway) register(ctx *goakt.ReceiveContext) {
	goCtx := ctx.Context()
	system := ctx.ActorSystem()

	for queue, weight := range w.registration.Queues {
		identity, err := goakt.GrainOf[*QueueGrain](goCtx, system, QueueGrainName(queue),
			goakt.WithGrainDeactivateAfter(w.runtime.Settings().PassivateAfter))
		if err != nil {
			w.runtime.Logger().Warn("resolving queue grain failed; next tick retries", "queue", queue, "error", err)

			continue
		}

		w.identities[queue] = identity

		err = system.TellGrain(goCtx, identity, &conveyorv1.RegisterGateway{
			Queue:       queue,
			GatewayName: w.name,
			Capacity:    w.capacity(),
			BatchTypes:  w.registration.BatchTypes,
			Weight:      weight,
		})
		if err != nil {
			w.runtime.Logger().Warn("webhook gateway registration failed; next tick retries", "queue", queue, "error", err)
		}
	}
}

// capacity is the concurrency this gateway announces: the registration's
// while the endpoint is healthy, zero while the breaker withholds, and one
// while probing a recovering endpoint. Don't lease what you can't deliver,
// and probe a recovering endpoint with a single delivery, not a flood.
func (w *WebhookGateway) capacity() int32 {
	switch {
	case w.withholding:
		return 0

	case w.probing:
		return 1

	default:
		return w.registration.Concurrency
	}
}

// recordTransport feeds one delivery's transport health into the endpoint
// breaker and re-announces capacity when the breaker's state demands it. A
// sample recorded during probing decides the probe (the breaker closed or
// reopened on this outcome); every other sample runs the ordinary
// open-detection and probe-timer sync.
func (w *WebhookGateway) recordTransport(ctx *goakt.ReceiveContext, failed bool) {
	wasProbing := w.probing

	// Execute with an instant probe records the sample in the breaker's
	// sliding window; while open it rejects, which simply drops the sample.
	_, _ = w.endpointBreaker.Execute(context.Background(), func(context.Context) (any, error) {
		if failed {
			return nil, errTransportFailure
		}

		return nil, nil
	})

	if wasProbing {
		w.resolveProbe(ctx)

		return
	}

	w.syncBreakerCapacity(ctx)
}

// syncBreakerCapacity converges the announced capacity onto the endpoint's
// health for the tick and for ordinary (non-probing) samples: an opening
// breaker withholds capacity immediately, and once the open timeout lapses a
// single probe slot opens so one delivery at a time tests the endpoint. The
// probe's own outcome, handled in resolveProbe, ends the probing window, so
// this never leaves it. The gateway owns the probe timer because the breaker
// leaves the open state only when a call flows, and no call flows at zero
// capacity.
func (w *WebhookGateway) syncBreakerCapacity(ctx *goakt.ReceiveContext) {
	now := w.runtime.Clock().Now()

	switch {
	case !w.withholding && !w.probing && w.endpointBreaker.State() == breaker.Open:
		w.withholding = true
		w.withheldAt = now
		w.register(ctx)
		w.runtime.Metrics().BreakerOpen(context.Background())
		w.runtime.Logger().Warn("webhook endpoint unhealthy; capacity withheld", "gateway", w.name, "url", w.registration.URL)

	case w.withholding && now.Sub(w.withheldAt) >= w.breakerOpenTimeout:
		w.withholding = false
		w.probing = true
		w.register(ctx)
		w.runtime.Logger().Info("webhook endpoint probing; single delivery slot restored", "gateway", w.name, "url", w.registration.URL)
	}
}

// resolveProbe ends or extends the probing window from a probe delivery's
// recorded outcome: a closed breaker restores full capacity, a reopened one
// withholds again, and an undecided one (not yet enough half-open samples)
// keeps the single probe slot open for the next delivery.
func (w *WebhookGateway) resolveProbe(ctx *goakt.ReceiveContext) {
	switch w.endpointBreaker.State() {
	case breaker.Closed:
		w.probing = false
		w.register(ctx)
		w.runtime.Logger().Info("webhook endpoint recovered; capacity restored", "gateway", w.name, "url", w.registration.URL)

	case breaker.Open:
		w.probing = false
		w.withholding = true
		w.withheldAt = w.runtime.Clock().Now()
		w.register(ctx)
		w.runtime.Metrics().BreakerOpen(context.Background())
		w.runtime.Logger().Warn("webhook endpoint still unhealthy; capacity withheld", "gateway", w.name, "url", w.registration.URL)
	}
}

// typeBreakerFor returns the task type's circuit breaker, creating it on
// first use.
func (w *WebhookGateway) typeBreakerFor(taskType string) *breaker.CircuitBreaker {
	circuitBreaker, exists := w.typeBreakers[taskType]

	if !exists {
		circuitBreaker = breaker.NewCircuitBreaker(
			breaker.WithFailureRate(breakerFailureRate),
			breaker.WithMinRequests(breakerMinRequests),
			breaker.WithOpenTimeout(breakerOpenTimeout),
		)
		w.typeBreakers[taskType] = circuitBreaker
	}

	return circuitBreaker
}

// recordTypeOutcome feeds one execution outcome into the task type's
// circuit breaker, mirroring the stream gateway: RETRY counts as a failure,
// SUCCESS as a success, and other outcomes carry no health signal.
func (w *WebhookGateway) recordTypeOutcome(taskType string, outcome conveyorv1.TaskOutcome) {
	var failed bool

	switch outcome {
	case conveyorv1.TaskOutcome_TASK_OUTCOME_SUCCESS:
		failed = false

	case conveyorv1.TaskOutcome_TASK_OUTCOME_RETRY:
		failed = true

	default:
		return
	}

	_, _ = w.typeBreakerFor(taskType).Execute(context.Background(), func(context.Context) (any, error) {
		if failed {
			return nil, errRetryableFailure
		}

		return nil, nil
	})
}

// deferCompletion withholds a completion report — and with it the queue
// grain's credit refill — until the open type breaker's timeout lapses, so
// a failing task type drains at probe speed instead of full speed.
func (w *WebhookGateway) deferCompletion(ctx *goakt.ReceiveContext, queue, taskID string, success bool) {
	deferred := deferredCompletion{queue: queue, taskID: taskID, success: success}

	if err := ctx.ActorSystem().ScheduleOnce(ctx.Context(), deferred, ctx.Self(), breakerOpenTimeout); err != nil {
		w.runtime.Logger().Warn("deferring webhook completion failed; reporting immediately", "task_id", taskID, "error", err)
		w.reportCompletion(ctx, queue, taskID, success)

		return
	}

	w.runtime.Logger().Debug("webhook completion withheld: circuit open for task type", "task_id", taskID, "queue", queue)
}

// apply swaps in a fresh registration snapshot: queues no longer served
// announce zero capacity so their grains stop leasing here, then the new
// snapshot registers.
func (w *WebhookGateway) apply(ctx *goakt.ReceiveContext, worker *broker.WebhookWorker) {
	goCtx := ctx.Context()
	system := ctx.ActorSystem()

	for queue, identity := range w.identities {
		if _, kept := worker.Queues[queue]; kept {
			continue
		}

		withdraw := &conveyorv1.RegisterGateway{Queue: queue, GatewayName: w.name}
		if err := system.TellGrain(goCtx, identity, withdraw); err != nil {
			w.runtime.Logger().Warn("withdrawing webhook gateway failed", "queue", queue, "error", err)
		}

		delete(w.identities, queue)
	}

	w.registration = worker
	w.signer = webhook.NewHMACSigner(worker.Secrets[0], w.runtime.Clock())
	w.register(ctx)
}

// deliver posts one leased task to the endpoint as a JSON-RPC call, off the
// mailbox turn, and tracks it until the response message arrives.
func (w *WebhookGateway) deliver(ctx *goakt.ReceiveContext, message *conveyorv1.ExecuteTask) {
	task := message.GetTask()
	now := w.runtime.Clock().Now()

	w.inflight[task.GetId()] = &inflightTask{
		leaseID:      message.GetLeaseId(),
		dispatchedAt: now,
		queue:        task.GetQueue(),
		taskType:     task.GetType(),
		retried:      task.GetRetried(),
		maxRetry:     task.GetOptions().GetMaxRetry(),
		strategy:     retryStrategyFor(w.strategy, task.GetOptions().GetRetryPolicy()),
	}

	// Queue latency: how long the task waited from enqueue to dispatch.
	if enqueuedAt := task.GetEnqueuedAt(); enqueuedAt.IsValid() {
		w.runtime.Metrics().RecordQueueLatency(context.Background(), now.Sub(enqueuedAt.AsTime()).Seconds(), task.GetQueue())
	}

	deadline := w.deliveryDeadline(now, executionDeadlineAt(now, message.GetLeaseExpiresAt(), task.GetOptions()))
	request := webhook.NewExecuteRequest(message.GetLeaseId(), w.taskParams(task, message.GetLeaseId(), deadline))

	requestCtx, abort := context.WithDeadline(context.Background(), deadline)
	w.aborts[task.GetId()] = abort

	taskID := task.GetId()
	url := w.registration.URL
	client := w.client
	signer := w.signer

	ctx.PipeTo(ctx.Self(), func() (any, error) {
		defer abort()

		response, err := client.Call(requestCtx, url, signer, request)
		if err != nil {
			return webhookDelivery{taskID: taskID, outcome: webhook.OutcomeTransportFailure, message: err.Error()}, nil
		}

		outcome, failure := response.Classify()

		return webhookDelivery{taskID: taskID, outcome: outcome, message: failure}, nil
	})
}

// deliverBatch posts one fired aggregation group to the endpoint as a
// JSON-RPC batch: one call per member, one POST, members answered
// individually.
func (w *WebhookGateway) deliverBatch(ctx *goakt.ReceiveContext, message *conveyorv1.ExecuteBatch) {
	tasks := message.GetTasks()
	if len(tasks) == 0 {
		return
	}

	leaseID := message.GetLeaseId()
	now := w.runtime.Clock().Now()
	ids := make([]string, 0, len(tasks))
	requests := make([]*webhook.Request, 0, len(tasks))
	deadline := w.deliveryDeadline(now, message.GetLeaseExpiresAt().AsTime())

	for _, task := range tasks {
		deadline = tightenDeadline(deadline, task, now)
	}

	for _, task := range tasks {
		w.inflight[task.GetId()] = &inflightTask{
			leaseID:      leaseID,
			dispatchedAt: now,
			queue:        task.GetQueue(),
			taskType:     task.GetType(),
			retried:      task.GetRetried(),
			maxRetry:     task.GetOptions().GetMaxRetry(),
			strategy:     retryStrategyFor(w.strategy, task.GetOptions().GetRetryPolicy()),
		}

		ids = append(ids, task.GetId())
		// Batch member calls are keyed by task id: every member shares the
		// batch's lease, so the lease id cannot correlate the responses.
		requests = append(requests, webhook.NewExecuteRequest(task.GetId(), w.taskParams(task, leaseID, deadline)))
	}

	w.batchStates[leaseID] = &webhookBatch{
		queue:     tasks[0].GetQueue(),
		members:   ids,
		remaining: len(ids),
	}

	requestCtx, abort := context.WithDeadline(context.Background(), deadline)

	for _, id := range ids {
		w.aborts[id] = abort
	}

	url := w.registration.URL
	client := w.client
	signer := w.signer

	ctx.PipeTo(ctx.Self(), func() (any, error) {
		defer abort()

		responses, err := client.CallBatch(requestCtx, url, signer, requests)
		if err != nil {
			return webhookBatchDelivery{leaseID: leaseID, transportError: err.Error()}, nil
		}

		outcomes := make(map[string]webhookDelivery, len(responses))

		for id, response := range responses {
			outcome, failure := response.Classify()
			outcomes[id] = webhookDelivery{taskID: id, outcome: outcome, message: failure}
		}

		return webhookBatchDelivery{leaseID: leaseID, outcomes: outcomes}, nil
	})
}

// deliveryDeadline bounds one POST: the endpoint must answer within the
// registration's request timeout, and never past the task's own execution
// deadline. An accepted (asynchronous) delivery escapes this bound: its
// lease is then driven by heartbeats, not by the open request.
func (w *WebhookGateway) deliveryDeadline(now time.Time, executionDeadline time.Time) time.Time {
	requestTimeout := w.registration.RequestTimeout
	if requestTimeout <= 0 {
		requestTimeout = defaultRequestTimeout
	}

	deadline := now.Add(requestTimeout)
	if executionDeadline.Before(deadline) {
		deadline = executionDeadline
	}

	return deadline
}

// taskParams builds the JSON-RPC arguments of one delivered task, including
// the lease an asynchronous completion needs.
func (w *WebhookGateway) taskParams(task *conveyorv1.TaskEnvelope, leaseID string, deadline time.Time) *webhook.TaskParams {
	token := webhook.MintLeaseToken(w.registration.Secrets[0], w.registration.Name, task.GetId(), leaseID)

	return &webhook.TaskParams{
		TaskID:      task.GetId(),
		Queue:       task.GetQueue(),
		Type:        task.GetType(),
		Attempt:     task.GetRetried() + 1,
		MaxRetry:    task.GetOptions().GetMaxRetry(),
		Deadline:    deadline.UTC().Format(time.RFC3339Nano),
		ContentType: task.GetContentType(),
		Payload:     task.GetPayload(),
		Metadata:    task.GetMetadata(),
		Lease: &webhook.LeaseParams{
			Token:             token,
			HeartbeatInterval: w.heartbeatInterval().String(),
		},
	}
}

// heartbeatInterval is the cadence an asynchronously completing endpoint is
// told to heartbeat at: a third of the lease TTL, so the lease survives a
// missed beat. Deriving it from the configured TTL keeps the advertised
// cadence within the lease even when an operator shortens the TTL, which a
// fixed interval could silently outlive.
func (w *WebhookGateway) heartbeatInterval() time.Duration {
	return w.runtime.Settings().LeaseTTL / 3
}

// complete routes one delivery response: an accepted answer parks the task
// in asynchronous mode, everything else resolves it. A response for an
// unknown task id is dropped: a duplicate, or a delivery whose lease was
// already lost.
func (w *WebhookGateway) complete(ctx *goakt.ReceiveContext, message webhookDelivery) {
	if _, ok := w.inflight[message.taskID]; !ok {
		w.runtime.Logger().Debug("delivery result for unknown task dropped", "task_id", message.taskID, "gateway", w.name)

		return
	}

	w.recordTransport(ctx, message.outcome == webhook.OutcomeTransportFailure)

	if message.outcome == webhook.OutcomeAccepted {
		w.acceptAsync(message.taskID)

		return
	}

	w.resolveDelivery(ctx, message.taskID, deliveryResult(message))
}

// completeBatch routes one batch delivery response: accepted members park
// in asynchronous mode, answered members resolve, omitted members release
// without penalty, and a transport failure retries every member.
func (w *WebhookGateway) completeBatch(ctx *goakt.ReceiveContext, message webhookBatchDelivery) {
	state, ok := w.batchStates[message.leaseID]
	if !ok {
		w.runtime.Logger().Debug("batch delivery result for unknown batch dropped", "gateway", w.name)

		return
	}

	// One POST carried the whole batch, so it is one transport sample.
	w.recordTransport(ctx, message.transportError != "")

	for _, taskID := range state.members {
		if _, tracked := w.inflight[taskID]; !tracked {
			continue
		}

		if answer, answered := message.outcomes[taskID]; answered && message.transportError == "" && answer.outcome == webhook.OutcomeAccepted {
			w.acceptAsync(taskID)

			continue
		}

		w.resolveDelivery(ctx, taskID, batchMemberResult(message, taskID))
	}
}

// acceptAsync parks one delivery in asynchronous mode: the slot stays held,
// the endpoint's heartbeats keep the lease, and its ReportResult callback
// resolves it.
func (w *WebhookGateway) acceptAsync(taskID string) {
	delete(w.aborts, taskID)
	w.async[taskID] = w.runtime.Clock().Now()
	w.runtime.Logger().Debug("delivery accepted for asynchronous completion", "task_id", taskID, "gateway", w.name)
}

// resolveDelivery applies one resolved delivery's durable transition and
// reports its freed slot: directly for a single dispatch, through the batch
// bookkeeping for a group member.
func (w *WebhookGateway) resolveDelivery(ctx *goakt.ReceiveContext, taskID string, result *conveyorv1.Result) {
	entry, ok := w.inflight[taskID]
	if !ok {
		return
	}

	delete(w.inflight, taskID)
	delete(w.aborts, taskID)
	delete(w.async, taskID)

	success, terminal := applyOutcome(ctx.Context(), w.runtime, entry, result)
	w.recordTypeOutcome(entry.taskType, result.GetOutcome())

	if terminal {
		w.resolveDependents(ctx, taskID)
	}

	if state, batched := w.batchStates[entry.leaseID]; batched {
		w.resolveBatchMember(ctx, entry.leaseID, state, success)

		return
	}

	if w.typeBreakerFor(entry.taskType).State() == breaker.Open {
		w.runtime.Metrics().BreakerOpen(context.Background())
		w.deferCompletion(ctx, entry.queue, taskID, success)

		return
	}

	w.reportCompletion(ctx, entry.queue, taskID, success)
}

// resolveBatchMember records one member's resolution and, once the last
// member resolves, reports the whole batch, refilling its single credit.
func (w *WebhookGateway) resolveBatchMember(ctx *goakt.ReceiveContext, leaseID string, state *webhookBatch, success bool) {
	if success {
		state.succeeded++
	}

	if state.remaining--; state.remaining > 0 {
		return
	}

	delete(w.batchStates, leaseID)
	w.reportBatchCompletion(ctx, state.queue, len(state.members), state.succeeded)
}

// heartbeatAsync extends one asynchronously completing delivery's lease. A
// stale lease id (a superseded delivery), an unknown task, or a synchronous
// delivery is dropped; a lost lease drops the delivery, returns its slot, and
// tells the still-live endpoint to stop, since the lease it just refreshed
// belongs to another delivery now.
func (w *WebhookGateway) heartbeatAsync(ctx *goakt.ReceiveContext, message *conveyorv1.WebhookLeaseHeartbeat) {
	entry, ok := w.inflight[message.GetTaskId()]
	if !ok || entry.leaseID != message.GetLeaseId() {
		return
	}

	if _, isAsync := w.async[message.GetTaskId()]; !isAsync {
		return
	}

	err := w.runtime.Broker().ExtendLease(ctx.Context(), message.GetTaskId(), entry.leaseID, w.runtime.Settings().LeaseTTL)
	if err == nil {
		w.async[message.GetTaskId()] = w.runtime.Clock().Now()

		return
	}

	if errors.Is(err, broker.ErrLeaseLost) {
		w.dropAsync(ctx, message.GetTaskId(), entry, true)

		return
	}

	w.runtime.Logger().Warn("webhook async lease extension failed", "task_id", message.GetTaskId(), "error", err)
}

// resultAsync resolves one asynchronously completing delivery from its
// ReportResult callback. Stale lease ids and unknown tasks are dropped;
// only the execution outcomes are accepted.
func (w *WebhookGateway) resultAsync(ctx *goakt.ReceiveContext, message *conveyorv1.WebhookLeaseResult) {
	entry, ok := w.inflight[message.GetTaskId()]
	if !ok || entry.leaseID != message.GetLeaseId() {
		w.runtime.Logger().Debug("async result for unknown delivery dropped", "task_id", message.GetTaskId(), "gateway", w.name)

		return
	}

	switch message.GetOutcome() {
	case conveyorv1.TaskOutcome_TASK_OUTCOME_SUCCESS,
		conveyorv1.TaskOutcome_TASK_OUTCOME_RETRY,
		conveyorv1.TaskOutcome_TASK_OUTCOME_SKIP_RETRY:

	default:
		w.runtime.Logger().Debug("async result with invalid outcome dropped", "task_id", message.GetTaskId(), "outcome", message.GetOutcome())

		return
	}

	result := &conveyorv1.Result{
		TaskId:   message.GetTaskId(),
		Outcome:  message.GetOutcome(),
		ErrorMsg: message.GetErrorMsg(),
		Result:   message.GetResult(),
	}

	w.resolveDelivery(ctx, message.GetTaskId(), result)
}

// reapStaleAsync forgets asynchronous deliveries whose endpoint stopped
// heartbeating: their leases have expired and the reaper redelivers the
// tasks, so holding their slots any longer only starves the queue.
func (w *WebhookGateway) reapStaleAsync(ctx *goakt.ReceiveContext) {
	now := w.runtime.Clock().Now()
	ttl := w.runtime.Settings().LeaseTTL

	for taskID, lastBeat := range w.async {
		if now.Sub(lastBeat) <= ttl {
			continue
		}

		entry, ok := w.inflight[taskID]
		if !ok {
			delete(w.async, taskID)

			continue
		}

		w.runtime.Logger().Debug("async delivery stopped heartbeating; slot reclaimed", "task_id", taskID, "gateway", w.name)
		w.dropAsync(ctx, taskID, entry, false)
	}
}

// dropAsync forgets one asynchronous delivery whose lease is gone. The
// durable side needs nothing: the reaper already owns recovery. A batched
// member defers its active-count and credit accounting to the batch's own
// completion, recording itself as resolved-without-success; a single delivery
// frees its active slot and returns the one credit it held, so a stale
// delivery does not permanently shrink this gateway's capacity. When
// notifyCancel is set the endpoint is still live (it just heartbeated), so it
// is told to stop the work its lost lease no longer authorizes.
func (w *WebhookGateway) dropAsync(ctx *goakt.ReceiveContext, taskID string, entry *inflightTask, notifyCancel bool) {
	delete(w.inflight, taskID)
	delete(w.async, taskID)
	delete(w.aborts, taskID)

	if notifyCancel {
		w.pushCancelNotification(ctx, taskID)
	}

	if state, batched := w.batchStates[entry.leaseID]; batched {
		w.resolveBatchMember(ctx, entry.leaseID, state, false)

		return
	}

	w.runtime.Counters().Active.Add(-1)
	w.refillCredit(ctx, entry.queue)
}

// refillCredit returns to its queue grain the single dispatch credit a
// dropped asynchronous delivery still held. The delivery did not complete —
// the reaper redelivers it — so no completion is reported and no outcome
// counted; only the credit returns, capped at capacity by the grain.
func (w *WebhookGateway) refillCredit(ctx *goakt.ReceiveContext, queue string) {
	identity, ok := w.identities[queue]
	if !ok {
		w.runtime.Logger().Warn("webhook credit refill dropped: queue not registered", "queue", queue)

		return
	}

	credit := &conveyorv1.GatewayCredit{Queue: queue, GatewayName: w.name, Credits: 1}
	if err := ctx.ActorSystem().TellGrain(ctx.Context(), identity, credit); err != nil {
		w.runtime.Logger().Warn("webhook credit refill failed", "queue", queue, "error", err)
	}
}

// extendLeases keeps every open synchronous delivery's lease alive; the
// open request is the lease, so no endpoint heartbeat exists to do it.
// Asynchronous deliveries are skipped: their endpoint drives the lease, and
// a silent endpoint must lose it. A lease lost to another delivery aborts
// the open request: this gateway no longer owns the task.
func (w *WebhookGateway) extendLeases(ctx *goakt.ReceiveContext) {
	goCtx := ctx.Context()
	taskLog := w.runtime.Broker()
	ttl := w.runtime.Settings().LeaseTTL

	for taskID, entry := range w.inflight {
		if _, isAsync := w.async[taskID]; isAsync {
			continue
		}

		err := taskLog.ExtendLease(goCtx, taskID, entry.leaseID, ttl)
		if err == nil {
			continue
		}

		if !errors.Is(err, broker.ErrLeaseLost) {
			w.runtime.Logger().Warn("webhook lease extension failed", "task_id", taskID, "error", err)

			continue
		}

		delete(w.inflight, taskID)
		w.abortRequest(taskID)
		w.runtime.Counters().Active.Add(-1)
		w.runtime.Logger().Debug("lease lost; webhook delivery aborted", "task_id", taskID, "gateway", w.name)
	}
}

// cancelActive marks an admin-canceled delivery so its aborted attempt
// archives instead of retrying. A synchronous delivery's open request is
// aborted; an asynchronous one gets a best-effort cancel notification
// pushed to the endpoint. An unknown id belongs to another gateway.
func (w *WebhookGateway) cancelActive(ctx *goakt.ReceiveContext, message *conveyorv1.CancelActive) {
	taskID := message.GetTaskId()

	entry, ok := w.inflight[taskID]
	if !ok {
		return
	}

	entry.cancelRequested = true

	if _, isAsync := w.async[taskID]; !isAsync {
		w.abortRequest(taskID)
		w.runtime.Logger().Debug("admin cancel aborted webhook delivery", "task_id", taskID, "gateway", w.name)

		return
	}

	w.pushCancelNotification(ctx, taskID)
	w.runtime.Logger().Debug("admin cancel pushed to webhook endpoint", "task_id", taskID, "gateway", w.name)
}

// pushCancelNotification tells the endpoint, best-effort and off the mailbox
// turn, to stop working one task: an admin cancel of an accepted delivery, or
// a lease this gateway lost to another delivery. The notification carries no
// id and expects no response, and the endpoint may already have finished.
func (w *WebhookGateway) pushCancelNotification(ctx *goakt.ReceiveContext, taskID string) {
	notification := webhook.NewCancelNotification(taskID)
	url := w.registration.URL
	client := w.client
	signer := w.signer
	logger := w.runtime.Logger()

	ctx.PipeTo(ctx.Self(), func() (any, error) {
		if err := client.Notify(context.Background(), url, signer, notification); err != nil {
			logger.Debug("cancel notification failed", "task_id", taskID, "error", err)
		}

		return notificationSent{}, nil
	})
}

// abortRequest cancels the open HTTP request of one delivery, if any.
func (w *WebhookGateway) abortRequest(taskID string) {
	abort, ok := w.aborts[taskID]
	if !ok {
		return
	}

	abort()
	delete(w.aborts, taskID)
}

// drain releases every in-flight task for immediate redelivery and aborts
// every open request; late responses and callbacks arrive for unknown ids
// and are dropped.
func (w *WebhookGateway) drain(ctx *goakt.ReceiveContext) {
	goCtx := ctx.Context()
	taskLog := w.runtime.Broker()

	for taskID, entry := range w.inflight {
		err := taskLog.Release(goCtx, taskID, entry.leaseID)
		if err != nil && !errors.Is(err, broker.ErrLeaseLost) {
			w.runtime.Logger().Warn("releasing in-flight webhook task failed", "task_id", taskID, "error", err)
		}

		w.abortRequest(taskID)
		w.runtime.Counters().Active.Add(-1)
	}

	w.inflight = make(map[string]*inflightTask)
	w.batchStates = make(map[string]*webhookBatch)
	w.aborts = make(map[string]context.CancelFunc)
	w.async = make(map[string]time.Time)
	w.runtime.Logger().Debug("webhook gateway drained", "gateway", w.name)
}

// resolveDependents hands a terminally resolved task to the node's resolver
// pool, mirroring the stream gateway.
func (w *WebhookGateway) resolveDependents(ctx *goakt.ReceiveContext, taskID string) {
	resolver := w.runtime.Resolver()
	if resolver == nil {
		return
	}

	ctx.Tell(resolver, goakt.NewBroadcast(&conveyorv1.ResolveDependents{TaskId: taskID}))
}

// reportCompletion tells the task's queue grain that one execution slot is
// free again.
func (w *WebhookGateway) reportCompletion(ctx *goakt.ReceiveContext, queue, taskID string, success bool) {
	identity, ok := w.identities[queue]
	if !ok {
		w.runtime.Logger().Warn("webhook completion report dropped: queue not registered", "queue", queue, "task_id", taskID)

		return
	}

	completed := &conveyorv1.TaskCompleted{
		TaskId:      taskID,
		Queue:       queue,
		Success:     success,
		GatewayName: w.name,
	}

	if err := ctx.ActorSystem().TellGrain(ctx.Context(), identity, completed); err != nil {
		w.runtime.Logger().Warn("webhook completion report failed", "task_id", taskID, "error", err)
	}
}

// reportBatchCompletion tells the queue grain a batch finished, refilling
// the one credit the batch held.
func (w *WebhookGateway) reportBatchCompletion(ctx *goakt.ReceiveContext, queue string, total, succeeded int) {
	identity, ok := w.identities[queue]
	if !ok {
		w.runtime.Logger().Warn("webhook batch completion report dropped: queue not registered", "queue", queue)

		return
	}

	completed := &conveyorv1.BatchCompleted{
		Queue:       queue,
		GatewayName: w.name,
		Total:       int32(total),
		Succeeded:   int32(succeeded),
	}

	if err := ctx.ActorSystem().TellGrain(ctx.Context(), identity, completed); err != nil {
		w.runtime.Logger().Warn("webhook batch completion report failed", "queue", queue, "error", err)
	}
}

// deliveryResult translates one classified endpoint answer into the wire
// result shape applyOutcome consumes. Accepted answers never reach it: the
// caller parks those in asynchronous mode instead.
func deliveryResult(message webhookDelivery) *conveyorv1.Result {
	result := &conveyorv1.Result{TaskId: message.taskID, ErrorMsg: message.message}

	switch message.outcome {
	case webhook.OutcomeCompleted:
		result.Outcome = conveyorv1.TaskOutcome_TASK_OUTCOME_SUCCESS

	case webhook.OutcomeSkipRetry:
		result.Outcome = conveyorv1.TaskOutcome_TASK_OUTCOME_SKIP_RETRY

	default:
		// OutcomeRetry and OutcomeTransportFailure.
		result.Outcome = conveyorv1.TaskOutcome_TASK_OUTCOME_RETRY
	}

	return result
}

// batchMemberResult resolves one batch member's result: its own answer when
// the endpoint gave one, a retry when the whole POST failed, and a
// penalty-free release when the endpoint omitted it.
func batchMemberResult(message webhookBatchDelivery, taskID string) *conveyorv1.Result {
	if message.transportError != "" {
		return &conveyorv1.Result{
			TaskId:   taskID,
			Outcome:  conveyorv1.TaskOutcome_TASK_OUTCOME_RETRY,
			ErrorMsg: message.transportError,
		}
	}

	answer, answered := message.outcomes[taskID]
	if !answered {
		return &conveyorv1.Result{TaskId: taskID, Outcome: conveyorv1.TaskOutcome_TASK_OUTCOME_RELEASED}
	}

	return deliveryResult(answer)
}

// executionDeadlineAt computes the effective deadline of one delivery:
// min(lease expiry, task deadline, now + task timeout). It is the dispatch
// deadline both gateway kinds show the worker.
func executionDeadlineAt(now time.Time, leaseExpiresAt *timestamppb.Timestamp, options *conveyorv1.TaskOptions) time.Time {
	deadline := leaseExpiresAt.AsTime()

	if options.GetDeadline().IsValid() && options.GetDeadline().AsTime().Before(deadline) {
		deadline = options.GetDeadline().AsTime()
	}

	if options.GetTimeout().IsValid() {
		attemptDeadline := now.Add(options.GetTimeout().AsDuration())

		if attemptDeadline.Before(deadline) {
			deadline = attemptDeadline
		}
	}

	return deadline
}
