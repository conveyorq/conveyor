// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package actors

import (
	"context"
	"fmt"
	"math"
	"strings"

	goakt "github.com/tochemey/goakt/v4/actor"
	"golang.org/x/time/rate"
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
	// weight is the gateway's declared dispatch weight for this queue. Dispatch
	// is proportional to weight: a gateway with twice the weight of a peer draws
	// twice the share of leased tasks, credits permitting. Always at least one.
	weight int32
	// currentWeight is the smooth weighted round-robin accumulator. Each pick
	// adds weight to every eligible gateway, selects the highest, then subtracts
	// the eligible total from the winner. This spreads each gateway's turns
	// evenly across the cycle rather than clustering them in bursts.
	currentWeight int32
	// batchTypes are the task types this gateway's worker handles as batches;
	// a fired group dispatches only to a gateway advertising the group's type.
	batchTypes []string
}

// QueueGrain is the per-queue dispatcher: a virtual actor with exactly one
// live activation cluster-wide. It leases due tasks from the broker when
// gateways have credits and distributes them across gateways in proportion to
// their declared weights. All of its state is disposable and rebuilt from the
// broker on activation.
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
	// limiter caps how fast this queue dispatches, or nil when the queue is
	// unlimited. It is the queue's effective rate limit — its own override or
	// the server's global default — rebuilt on activation and on change. The
	// bucket is driven by the injected clock, so it is the only live token
	// state and never touches the broker on the dispatch path.
	limiter *rate.Limiter
	// throttled records whether the limiter last deferred a lease cycle, so the
	// throttled metric counts episodes rather than every wake-up that finds the
	// bucket empty.
	throttled bool
	// concurrencyLimit is the most tasks sharing a concurrency key this queue
	// dispatches at once, or zero when the queue's keys are unbounded. Like the
	// rate limit it is the queue's persisted override, loaded at activation and
	// updated on change; it never touches the broker on the dispatch path.
	concurrencyLimit int
	// activeByKey counts the tasks currently in flight per concurrency key. It is
	// the live keyed semaphore: a key at concurrencyLimit holds its remaining
	// tasks back at dispatch until an active one completes. A slot is freed on
	// completion or, for a task re-leased after its lease was lost, kept rather
	// than double-counted (see inFlightKey). It is not freed when the reaper
	// dead-letters a keyed task whose retries are exhausted — that terminal
	// transition sends no completion — so a task crash-looped to its retry limit
	// leaks its slot until the grain passivates and rebuilds this from scratch.
	// The leak is conservative (it over-restricts a key, never over-admits).
	activeByKey map[string]int
	// inFlightKey maps a dispatched task id to its concurrency key, so a
	// completion decrements the right key and a re-lease of the same task (its
	// prior lease lost to a crash the reaper reclaimed) reuses its slot instead
	// of being blocked by it. Only keyed tasks are recorded.
	inFlightKey map[string]string
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
	x.throttled = false
	x.activeByKey = make(map[string]int)
	x.inFlightKey = make(map[string]string)

	if err := x.loadRateLimit(ctx); err != nil {
		return err
	}

	if err := x.loadConcurrencyLimit(ctx); err != nil {
		return err
	}

	runtime.Logger().Debug("queue grain activated", "queue", x.queue, "paused", paused,
		"rate_limited", x.limiter != nil, "concurrency_limit", x.concurrencyLimit)

	return nil
}

// loadRateLimit rebuilds the queue's effective token bucket: its persisted
// override if one exists, otherwise the server's global default. It runs only
// at activation, so the one broker read it makes is never on the dispatch path.
// A disabled kill-switch skips the read and leaves the queue unlimited. The new
// bucket starts full, so a queue that passivates and reactivates may burst once
// more — an accepted consequence of keeping live token state only in the grain.
func (x *QueueGrain) loadRateLimit(ctx context.Context) error {
	x.limiter = nil

	settings := x.runtime.Settings()
	if !settings.RateLimitEnabled {
		return nil
	}

	override, ok, err := x.runtime.Broker().QueueRateLimit(ctx, x.queue)
	if err != nil {
		return fmt.Errorf("queue grain %s: loading rate limit: %w", x.queue, err)
	}

	if ok {
		x.limiter = buildLimiter(override.RatePerSec, override.Burst)

		return nil
	}

	x.limiter = buildLimiter(settings.RateLimitRatePerSec, settings.RateLimitBurst)

	return nil
}

// buildLimiter returns a full token bucket for the given rate and burst, or nil
// when the limit is unset or invalid (a non-positive or non-finite rate, or a
// burst below one, means the queue is unlimited). The finite guard defends the
// dispatch path against a NaN/Inf value reaching rate.NewLimiter, whatever its
// source.
func buildLimiter(ratePerSec float64, burst int) *rate.Limiter {
	if ratePerSec <= 0 || math.IsNaN(ratePerSec) || math.IsInf(ratePerSec, 0) || burst < 1 {
		return nil
	}

	return rate.NewLimiter(rate.Limit(ratePerSec), burst)
}

// loadConcurrencyLimit reads the queue's persisted per-key concurrency limit. It
// runs only at activation, so the one broker read it makes is never on the
// dispatch path. No override leaves the queue's keys unbounded (limit zero).
func (x *QueueGrain) loadConcurrencyLimit(ctx context.Context) error {
	x.concurrencyLimit = 0

	override, ok, err := x.runtime.Broker().QueueConcurrencyLimit(ctx, x.queue)
	if err != nil {
		return fmt.Errorf("queue grain %s: loading concurrency limit: %w", x.queue, err)
	}

	if ok && override.MaxActive >= 1 {
		x.concurrencyLimit = override.MaxActive
	}

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

	case *conveyorv1.BatchCompleted:
		x.recordBatchCompletion(message)
		x.maybeLease(ctx)
		ctx.NoErr()

	case *conveyorv1.FireGroup:
		x.fireGroup(ctx, message)
		ctx.NoErr()

	case *conveyorv1.GroupLeaseCompleted:
		x.finishGroupLease(ctx, message)
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
		x.runtime.Counters().Released.Add(int64(message.GetReleased()))

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

	case *conveyorv1.RateLimitChanged:
		x.applyRateLimitChange(message)
		x.maybeLease(ctx)
		ctx.NoErr()

	case *conveyorv1.ConcurrencyLimitChanged:
		x.applyConcurrencyLimitChange(message)
		x.maybeLease(ctx)
		ctx.NoErr()

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

	x.releaseConcurrencyKey(message.GetTaskId())

	x.runtime.Logger().Debug("task completed", "queue", x.queue, "task_id", message.GetTaskId(), "success", message.GetSuccess())
}

// releaseConcurrencyKey frees the concurrency slot a finished task held, so the
// next task sharing its key may dispatch. A task with no key, or one whose slot
// was lost to a relocation, is a no-op.
func (x *QueueGrain) releaseConcurrencyKey(taskID string) {
	key, ok := x.inFlightKey[taskID]
	if !ok {
		return
	}

	delete(x.inFlightKey, taskID)

	if x.activeByKey[key]--; x.activeByKey[key] <= 0 {
		delete(x.activeByKey, key)
	}
}

// registerGateway upserts a gateway. A new gateway is granted credits
// equal to its capacity; a re-registration (heartbeat, relocation healing)
// only refreshes the capacity so credits are never double-granted.
func (x *QueueGrain) registerGateway(message *conveyorv1.RegisterGateway) {
	// A non-positive weight (an older worker that predates weighted dispatch, or
	// the proto zero value) is treated as the neutral weight one, so an
	// unweighted fleet falls back to plain round-robin.
	weight := max(message.GetWeight(), 1)

	for _, gateway := range x.gateways {
		if gateway.name == message.GetGatewayName() {
			previous := gateway.capacity
			gateway.capacity = message.GetCapacity()
			gateway.weight = weight
			gateway.batchTypes = message.GetBatchTypes()

			// A capacity change takes effect now, not after old credits are
			// spent. A shrink (a webhook gateway withholding, or dropping
			// this queue) clamps; a growth grants the delta, because refunds
			// that landed while the cap was lower were lost to the clamp and
			// nothing else would restore them. A steady re-registration
			// heartbeat changes nothing, so credits are never double-granted.
			switch {
			case gateway.capacity < previous:
				gateway.credits = min(gateway.credits, gateway.capacity)

			case gateway.capacity > previous:
				gateway.credits = min(gateway.credits+gateway.capacity-previous, gateway.capacity)
			}

			return
		}
	}

	x.gateways = append(x.gateways, &gatewayCredits{
		name:       message.GetGatewayName(),
		capacity:   message.GetCapacity(),
		credits:    message.GetCapacity(),
		weight:     weight,
		batchTypes: message.GetBatchTypes(),
	})

	x.runtime.Logger().Debug("gateway registered", "queue", x.queue, "gateway", message.GetGatewayName(), "capacity", message.GetCapacity(), "weight", weight)
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

	// Rate limit: lease only as many tasks as the bucket can spend now, so the
	// excess stays pending in the broker rather than idling under a lease. A
	// fully drained bucket defers the whole cycle; the next wake-up — a
	// completion, a returned credit, an enqueue, or the reaper's pending sweep —
	// retries once tokens have refilled. The bucket is consumed in
	// finishLeaseCycle for the tasks actually dispatched.
	if x.limiter != nil {
		available := int(x.limiter.TokensAt(x.runtime.Clock().Now()))

		if available <= 0 {
			if !x.throttled {
				x.throttled = true
				x.runtime.Metrics().RateLimited(ctx.Context(), x.queue)
			}

			return
		}

		x.throttled = false
		limit = min(limit, available)
	}

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

	// undeliverable collects tasks this cycle leased but did not dispatch: a
	// gateway became unreachable (credits run out only that way), or the task's
	// concurrency key is saturated. Both share one remedy — release them so they
	// redeliver instead of idling until the lease expires.
	var undeliverable []*conveyorv1.TaskEnvelope

	saturated := false

	for _, task := range tasks {
		// Concurrency key: hold the task back when its key already has the most
		// tasks the queue runs at once in flight. It redelivers when an active
		// task with the same key completes and reopens the gate (that completion
		// re-runs maybeLease). Checked before pickGateway so a held-back task
		// neither consumes a gateway credit nor a rate-limit token.
		key := task.GetOptions().GetConcurrencyKey()

		// A task already counted against its key is one this grain dispatched
		// before and is re-leasing now because its prior lease was lost (a worker
		// crash the reaper reclaimed to retry). It keeps its slot: the gate must
		// not block it behind its own count, and the dispatch must not count it
		// twice. Without this a limit-1 (mutex) key deadlocks on a single crash.
		holdsSlot := key != "" && x.inFlightKey[task.GetId()] == key

		if key != "" && !holdsSlot && x.concurrencyLimit > 0 && x.activeByKey[key] >= x.concurrencyLimit {
			undeliverable = append(undeliverable, task)
			saturated = true

			continue
		}

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

		// Reserve a slot for the key so the rest of this batch — and later
		// cycles — see it occupied until the task completes. A re-dispatch of a
		// task that already holds its slot only refreshes the id mapping.
		if key != "" {
			if !holdsSlot {
				x.activeByKey[key]++
			}

			x.inFlightKey[task.GetId()] = key
		}

		x.runtime.Logger().Debug("task dispatched", "queue", x.queue, "task_id", task.GetId(), "gateway", gateway.name)
	}

	if saturated {
		x.runtime.Metrics().ConcurrencyLimited(ctx.Context(), x.queue)
	}

	dispatched := len(tasks) - len(undeliverable)

	// Spend a token per task actually dispatched. maybeLease capped this cycle
	// to the bucket's available tokens, so the reservation never waits. Clamp to
	// the burst: a rate-limit change between this cycle's lease and its
	// completion can shrink the bucket below the in-flight count, and ReserveN
	// with n above the burst would consume nothing at all.
	if x.limiter != nil && dispatched > 0 {
		x.limiter.ReserveN(x.runtime.Clock().Now(), min(dispatched, x.limiter.Burst()))
	}

	if len(undeliverable) > 0 {
		x.releaseLeased(ctx, message.GetLeaseId(), undeliverable)
	}

	// Run another cycle only when this one dispatched something: more work may
	// be due. If nothing dispatched — every leased task held back by a saturated
	// concurrency key — re-leasing would just churn the same tasks. A completion
	// (which frees a key slot), a credit, an enqueue, or the reaper's pending
	// sweep re-triggers the cycle instead.
	if dispatched > 0 {
		x.maybeLease(ctx)
	}
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

// pickGateway returns a gateway with credits chosen by weighted round-robin,
// or nil when none has capacity left.
func (x *QueueGrain) pickGateway() *gatewayCredits {
	return x.selectWeightedGateway(func(gateway *gatewayCredits) bool {
		return gateway.credits > 0
	})
}

// selectWeightedGateway runs one step of smooth weighted round-robin over the
// gateways the eligible predicate admits, returning the chosen gateway or nil
// when none qualifies. Each step adds every eligible gateway's weight to its
// running accumulator, selects the gateway with the highest accumulator, then
// debits the eligible weight total from the winner. Across many steps each
// gateway is chosen in proportion to its weight, with its turns spread evenly
// rather than clustered. The accumulator sum is conserved at zero each step, so
// restricting eligibility per call (credits, batch capability) keeps the scheme
// stable as the eligible set shifts.
func (x *QueueGrain) selectWeightedGateway(eligible func(*gatewayCredits) bool) *gatewayCredits {
	var selected *gatewayCredits
	var total int32

	for _, gateway := range x.gateways {
		if !eligible(gateway) {
			continue
		}

		gateway.currentWeight += gateway.weight
		total += gateway.weight

		if selected == nil || gateway.currentWeight > selected.currentWeight {
			selected = gateway
		}
	}

	if selected == nil {
		return nil
	}

	selected.currentWeight -= total

	return selected
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

// applyRateLimitChange rebuilds the queue's token bucket from an Admin API
// change. The new values ride in the message, so the grain never reads the
// broker on the turn: a positive rate sets the override, while a non-positive
// rate clears it and reverts the queue to the server's global default. The
// kill-switch wins — a disabled limiter stays nil.
func (x *QueueGrain) applyRateLimitChange(message *conveyorv1.RateLimitChanged) {
	settings := x.runtime.Settings()

	if !settings.RateLimitEnabled {
		x.limiter = nil
		x.runtime.Logger().Info("queue rate limit change ignored; rate limiting disabled", "queue", x.queue)

		return
	}

	ratePerSec, burst := message.GetRatePerSec(), int(message.GetBurst())

	if ratePerSec <= 0 {
		ratePerSec, burst = settings.RateLimitRatePerSec, settings.RateLimitBurst
	}

	x.limiter = buildLimiter(ratePerSec, burst)
	x.throttled = false
	x.runtime.Logger().Info("queue rate limit changed", "queue", x.queue, "rate_per_sec", ratePerSec, "burst", burst)
}

// applyConcurrencyLimitChange updates the queue's per-key concurrency limit from
// an Admin API change. The new value rides in the message, so the grain never
// reads the broker on the turn: a positive max-active sets the limit, while zero
// clears it and leaves the queue's keys unbounded. The live per-key counts are
// untouched — they track in-flight work, not the limit, so lowering the limit
// simply holds new dispatches until a key drains below it.
func (x *QueueGrain) applyConcurrencyLimitChange(message *conveyorv1.ConcurrencyLimitChanged) {
	x.concurrencyLimit = 0

	if maxActive := int(message.GetMaxActive()); maxActive >= 1 {
		x.concurrencyLimit = maxActive
	}

	x.runtime.Logger().Info("queue concurrency limit changed", "queue", x.queue, "max_active", x.concurrencyLimit)
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
