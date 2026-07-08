// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package actors

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	goakt "github.com/tochemey/goakt/v4/actor"
	goaktlog "github.com/tochemey/goakt/v4/log"

	"github.com/conveyorq/conveyor/internal/broker/memory"
	"github.com/conveyorq/conveyor/internal/clock"
	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
)

func TestQueueGrainActivationRequiresRuntimeExtension(t *testing.T) {
	ctx := context.Background()

	system, err := goakt.NewActorSystem("bare-grain-system", goakt.WithLogger(goaktlog.DiscardLogger))
	require.NoError(t, err)
	require.NoError(t, system.Start(ctx))

	t.Cleanup(func() { _ = system.Stop(ctx) })

	// Resolving the grain identity activates it, running OnActivate against a
	// system with no engine runtime registered.
	_, err = goakt.GrainOf[*QueueGrain](ctx, system, QueueGrainName("q"))
	require.ErrorContains(t, err, "is not registered")
}

func TestQueueGrainActivationPausedReadError(t *testing.T) {
	ctx := context.Background()
	taskLog := newFaultBroker(memory.New(clock.System()))
	engine := startEngine(t, taskLog)

	taskLog.fault(methodQueuePaused, errors.New("pause read down"))

	err := engine.TellQueue(ctx, "activate-fault", &conveyorv1.TasksAvailable{Queue: "activate-fault"})
	require.Error(t, err, "activation must fail when the pause flag cannot be read")
}

func TestQueueGrainAppliesRateLimitChange(t *testing.T) {
	ctx := context.Background()
	engine := startEngine(t, memory.New(clock.System()))

	require.NoError(t, engine.TellQueue(ctx, "rl-change",
		&conveyorv1.RateLimitChanged{Queue: "rl-change", RatePerSec: 10, Burst: 5}))
}

func TestQueueGrainAppliesConcurrencyLimitChange(t *testing.T) {
	ctx := context.Background()
	engine := startEngine(t, memory.New(clock.System()))

	require.NoError(t, engine.TellQueue(ctx, "cc-change",
		&conveyorv1.ConcurrencyLimitChanged{Queue: "cc-change", MaxActive: 4}))
}

func TestQueueGrainNameRoundTrip(t *testing.T) {
	require.Equal(t, "queue.critical", QueueGrainName("critical"))
	require.Equal(t, "critical", queueFromGrainName(QueueGrainName("critical")))
}

func TestBuildLimiter(t *testing.T) {
	require.Nil(t, buildLimiter(0, 5), "a non-positive rate means unlimited")
	require.Nil(t, buildLimiter(-1, 5), "a negative rate means unlimited")
	require.Nil(t, buildLimiter(10, 0), "a burst below one means unlimited")

	limiter := buildLimiter(10, 5)
	require.NotNil(t, limiter)
	require.EqualValues(t, 10, limiter.Limit())
	require.Equal(t, 5, limiter.Burst())
}

// rateLimitGrain builds a bare grain whose runtime carries the given global
// default and kill-switch, backed by a fake clock and an empty memory broker.
func rateLimitGrain(t *testing.T, enabled bool, defaultRate float64, defaultBurst int) (*QueueGrain, *memory.Broker) {
	t.Helper()

	fake := clock.NewFake(time.Date(2026, 6, 16, 12, 0, 0, 0, time.UTC))
	taskLog := memory.New(fake)
	t.Cleanup(func() { _ = taskLog.Close() })

	settings := testSettings
	settings.RateLimitEnabled = enabled
	settings.RateLimitRatePerSec = defaultRate
	settings.RateLimitBurst = defaultBurst

	runtime := NewRuntime(taskLog, fake, settings, quietLogger())

	return &QueueGrain{runtime: runtime, queue: "default"}, taskLog
}

// faultGrain builds a bare grain whose runtime is backed by a fault broker, so
// the activation-time config reads can be driven to fail.
func faultGrain(t *testing.T) (*QueueGrain, *faultBroker) {
	t.Helper()

	fake := clock.NewFake(time.Date(2026, 6, 16, 12, 0, 0, 0, time.UTC))
	inner := memory.New(fake)
	t.Cleanup(func() { _ = inner.Close() })

	faultLog := newFaultBroker(inner)

	settings := testSettings
	settings.RateLimitEnabled = true

	runtime := NewRuntime(faultLog, fake, settings, quietLogger())

	return &QueueGrain{runtime: runtime, queue: "default"}, faultLog
}

func TestLoadRateLimitBrokerError(t *testing.T) {
	grain, faultLog := faultGrain(t)
	faultLog.fault(methodQueueRateLimit, errors.New("rate down"))

	require.Error(t, grain.loadRateLimit(context.Background()))
}

func TestLoadConcurrencyLimitBrokerError(t *testing.T) {
	grain, faultLog := faultGrain(t)
	faultLog.fault(methodQueueConcurrency, errors.New("concurrency down"))

	require.Error(t, grain.loadConcurrencyLimit(context.Background()))
}

func TestLoadRateLimitUsesGlobalDefault(t *testing.T) {
	grain, _ := rateLimitGrain(t, true, 10, 5)

	require.NoError(t, grain.loadRateLimit(context.Background()))
	require.NotNil(t, grain.limiter)
	require.EqualValues(t, 10, grain.limiter.Limit())
	require.Equal(t, 5, grain.limiter.Burst())
}

func TestLoadRateLimitOverrideReplacesDefault(t *testing.T) {
	grain, taskLog := rateLimitGrain(t, true, 100, 50)
	require.NoError(t, taskLog.SetQueueRateLimit(context.Background(), "default", 3, 2))

	require.NoError(t, grain.loadRateLimit(context.Background()))
	require.NotNil(t, grain.limiter)
	require.EqualValues(t, 3, grain.limiter.Limit(), "the override rate replaces the default")
	require.Equal(t, 2, grain.limiter.Burst(), "the override burst replaces the default")
}

func TestLoadRateLimitDisabledLeavesUnlimited(t *testing.T) {
	grain, taskLog := rateLimitGrain(t, false, 10, 5)
	require.NoError(t, taskLog.SetQueueRateLimit(context.Background(), "default", 3, 2))

	require.NoError(t, grain.loadRateLimit(context.Background()))
	require.Nil(t, grain.limiter, "the kill-switch overrides both default and override")
}

func TestApplyRateLimitChange(t *testing.T) {
	grain, _ := rateLimitGrain(t, true, 10, 5)

	// A positive rate sets the override.
	grain.applyRateLimitChange(&conveyorv1.RateLimitChanged{RatePerSec: 7, Burst: 3})
	require.NotNil(t, grain.limiter)
	require.EqualValues(t, 7, grain.limiter.Limit())
	require.Equal(t, 3, grain.limiter.Burst())

	// A non-positive rate clears the override, reverting to the global default.
	grain.applyRateLimitChange(&conveyorv1.RateLimitChanged{RatePerSec: 0})
	require.NotNil(t, grain.limiter)
	require.EqualValues(t, 10, grain.limiter.Limit())
	require.Equal(t, 5, grain.limiter.Burst())
}

func TestApplyRateLimitChangeIgnoredWhenDisabled(t *testing.T) {
	grain, _ := rateLimitGrain(t, false, 0, 0)

	grain.applyRateLimitChange(&conveyorv1.RateLimitChanged{RatePerSec: 7, Burst: 3})
	require.Nil(t, grain.limiter, "the kill-switch ignores runtime changes")
}

// TestQueueGrainRateLimitThrottlesDispatch drives the enforcement end-to-end on
// a frozen clock: the burst dispatches, the rest stay pending, and advancing the
// clock by one refill window releases exactly another burst.
func TestQueueGrainRateLimitThrottlesDispatch(t *testing.T) {
	const queue = "default"

	ctx := context.Background()
	fake := clock.NewFake(time.Date(2026, 6, 16, 12, 0, 0, 0, time.UTC))
	taskLog := memory.New(fake)
	t.Cleanup(func() { _ = taskLog.Close() })

	settings := testSettings
	settings.RateLimitEnabled = true
	settings.RateLimitRatePerSec = 5
	settings.RateLimitBurst = 3

	engine := newNodeWithClock(taskLog, fake, settings, freePorts(t, 3), nil)
	require.NoError(t, engine.Start(ctx))

	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = engine.Stop(stopCtx)
	})

	// Ample credits so the rate limit binds, not gateway capacity.
	log := &dispatchLog{}
	spawnGateway(t, engine, &mockGateway{queue: queue, capacity: 100, log: log})

	enqueueTasks(t, engine, queue, 10)

	// The frozen bucket holds exactly burst tokens: 3 dispatch, 7 stay pending.
	require.Eventually(t, func() bool { return len(log.snapshot()) >= 3 }, 3*time.Second, 10*time.Millisecond)
	time.Sleep(300 * time.Millisecond)
	require.Len(t, log.snapshot(), 3, "the rate limit caps dispatch at the burst while the clock is frozen")

	// One second refills 5 tokens, capped at the burst of 3.
	fake.Advance(time.Second)
	require.NoError(t, engine.TellQueue(ctx, queue, &conveyorv1.TasksAvailable{Queue: queue}))

	require.Eventually(t, func() bool { return len(log.snapshot()) >= 6 }, 3*time.Second, 10*time.Millisecond)
	time.Sleep(300 * time.Millisecond)
	require.Len(t, log.snapshot(), 6, "a refill releases exactly another burst")
}

func TestRegisterGatewayGrantsInitialCredits(t *testing.T) {
	grain := &QueueGrain{runtime: newTestRuntime(t)}

	grain.registerGateway(&conveyorv1.RegisterGateway{GatewayName: "gateway-1", Capacity: 4})

	require.Len(t, grain.gateways, 1)
	require.EqualValues(t, 4, grain.gateways[0].credits)
	require.Equal(t, 4, grain.totalCredits())
	require.EqualValues(t, 1, grain.gateways[0].weight, "an omitted weight registers as the neutral weight one")
}

func TestRegisterGatewayStoresAndRefreshesWeight(t *testing.T) {
	grain := &QueueGrain{runtime: newTestRuntime(t)}

	grain.registerGateway(&conveyorv1.RegisterGateway{GatewayName: "gateway-1", Capacity: 4, Weight: 3})
	require.EqualValues(t, 3, grain.gateways[0].weight, "a declared weight is stored")

	grain.registerGateway(&conveyorv1.RegisterGateway{GatewayName: "gateway-1", Capacity: 4, Weight: 7})
	require.EqualValues(t, 7, grain.gateways[0].weight, "re-registration refreshes the weight")

	grain.registerGateway(&conveyorv1.RegisterGateway{GatewayName: "gateway-1", Capacity: 4, Weight: -2})
	require.EqualValues(t, 1, grain.gateways[0].weight, "a non-positive weight clamps to one")
}

func TestRegisterGatewayReRegistrationDoesNotDoubleGrant(t *testing.T) {
	grain := &QueueGrain{runtime: newTestRuntime(t)}

	grain.registerGateway(&conveyorv1.RegisterGateway{GatewayName: "gateway-1", Capacity: 4})
	grain.gateways[0].credits = 1 // three dispatches in flight

	grain.registerGateway(&conveyorv1.RegisterGateway{GatewayName: "gateway-1", Capacity: 6})

	require.Len(t, grain.gateways, 1)
	require.EqualValues(t, 6, grain.gateways[0].capacity, "re-registration refreshes capacity")
	require.EqualValues(t, 1, grain.gateways[0].credits, "re-registration must not re-grant credits")
}

func TestAddCreditsIsCappedAtCapacity(t *testing.T) {
	grain := &QueueGrain{runtime: newTestRuntime(t)}

	grain.registerGateway(&conveyorv1.RegisterGateway{GatewayName: "gateway-1", Capacity: 4})
	grain.gateways[0].credits = 3

	grain.addCredits(&conveyorv1.GatewayCredit{GatewayName: "gateway-1", Credits: 100})
	require.EqualValues(t, 4, grain.gateways[0].credits, "credits never exceed capacity")

	grain.addCredits(&conveyorv1.GatewayCredit{GatewayName: "unknown", Credits: 5})
	require.EqualValues(t, 4, grain.gateways[0].credits, "unknown gateways are ignored")
}

func TestRecordCompletionRefillIsCapped(t *testing.T) {
	grain := &QueueGrain{runtime: newTestRuntime(t)}

	grain.registerGateway(&conveyorv1.RegisterGateway{GatewayName: "gateway-1", Capacity: 2})

	// A duplicate completion report must not push credits past capacity.
	grain.recordCompletion(&conveyorv1.TaskCompleted{GatewayName: "gateway-1", Success: true})
	require.EqualValues(t, 2, grain.gateways[0].credits)

	require.EqualValues(t, 1, grain.runtime.Counters().Completed.Load())
	require.EqualValues(t, -1, grain.runtime.Counters().Active.Load())
}

func TestRecordCompletionCountsFailures(t *testing.T) {
	grain := &QueueGrain{runtime: newTestRuntime(t)}

	grain.registerGateway(&conveyorv1.RegisterGateway{GatewayName: "gateway-1", Capacity: 2})
	grain.recordCompletion(&conveyorv1.TaskCompleted{GatewayName: "gateway-1", Success: false})

	require.EqualValues(t, 1, grain.runtime.Counters().Failed.Load())
	require.Zero(t, grain.runtime.Counters().Completed.Load())
}

func TestPickGatewayRoundRobinSkipsExhausted(t *testing.T) {
	grain := &QueueGrain{runtime: newTestRuntime(t)}

	grain.registerGateway(&conveyorv1.RegisterGateway{GatewayName: "gateway-1", Capacity: 1})
	grain.registerGateway(&conveyorv1.RegisterGateway{GatewayName: "gateway-2", Capacity: 1})
	grain.registerGateway(&conveyorv1.RegisterGateway{GatewayName: "gateway-3", Capacity: 1})
	grain.gateways[1].credits = 0

	first := grain.pickGateway()
	second := grain.pickGateway()

	require.Equal(t, "gateway-1", first.name)
	require.Equal(t, "gateway-3", second.name, "exhausted gateways are skipped")

	grain.gateways[0].credits = 0
	grain.gateways[2].credits = 0
	require.Nil(t, grain.pickGateway(), "no gateway with credits left")
}

// TestPickGatewayWeightedProportions proves the selection step alone hands out
// turns in proportion to the declared weights: with weights 3 and 1 and credits
// that never run out, a long run of picks splits exactly three to one and never
// clusters a gateway's turns (smooth weighted round-robin).
func TestPickGatewayWeightedProportions(t *testing.T) {
	grain := &QueueGrain{runtime: newTestRuntime(t)}

	grain.registerGateway(&conveyorv1.RegisterGateway{GatewayName: "heavy", Capacity: 1000, Weight: 3})
	grain.registerGateway(&conveyorv1.RegisterGateway{GatewayName: "light", Capacity: 1000, Weight: 1})

	counts := map[string]int{}

	const picks = 400

	maxRun := map[string]int{}
	runOf := ""
	runLen := 0

	for range picks {
		gateway := grain.pickGateway()
		require.NotNil(t, gateway)
		counts[gateway.name]++

		if gateway.name == runOf {
			runLen++
		} else {
			runOf, runLen = gateway.name, 1
		}

		maxRun[gateway.name] = max(maxRun[gateway.name], runLen)
	}

	require.Equal(t, 300, counts["heavy"], "weight 3 of 4 draws three quarters of the picks")
	require.Equal(t, 100, counts["light"], "weight 1 of 4 draws one quarter of the picks")
	require.LessOrEqual(t, maxRun["heavy"], 3, "smooth weighting must not cluster the heavy gateway's turns")
}

// TestPickGatewayWeightingExcludesExhausted confirms weighting only ranks
// gateways that still have credits: a heavy gateway out of credits is skipped
// entirely, so a lighter peer takes every pick rather than being starved.
func TestPickGatewayWeightingExcludesExhausted(t *testing.T) {
	grain := &QueueGrain{runtime: newTestRuntime(t)}

	grain.registerGateway(&conveyorv1.RegisterGateway{GatewayName: "heavy", Capacity: 10, Weight: 5})
	grain.registerGateway(&conveyorv1.RegisterGateway{GatewayName: "light", Capacity: 10, Weight: 1})
	grain.gateways[0].credits = 0

	for range 5 {
		gateway := grain.pickGateway()
		require.NotNil(t, gateway)
		require.Equal(t, "light", gateway.name, "an exhausted heavy gateway is excluded from weighting")
	}
}

func TestRemoveGatewayForgetsCredits(t *testing.T) {
	grain := &QueueGrain{runtime: newTestRuntime(t)}

	grain.registerGateway(&conveyorv1.RegisterGateway{GatewayName: "gateway-1", Capacity: 2})
	grain.registerGateway(&conveyorv1.RegisterGateway{GatewayName: "gateway-2", Capacity: 3})

	grain.removeGateway("gateway-1")

	require.Len(t, grain.gateways, 1)
	require.Equal(t, 3, grain.totalCredits())

	grain.removeGateway("gateway-unknown")
	require.Len(t, grain.gateways, 1)
}

// TestQueueGrainDrainAndResume drives the pause flow end to end: DrainQueue
// persists the flag and stops dispatch, ResumeQueue restores it.
func TestQueueGrainDrainAndResume(t *testing.T) {
	const queue = "drainable"

	ctx := context.Background()
	taskLog := memory.New(clock.System())
	engine := startEngine(t, taskLog)
	recorder := newFrameRecorder()

	handle, err := engine.SpawnGateway(ctx, GatewaySession{
		SessionID:   "session-drainable",
		Queues:      []string{queue},
		Concurrency: 4,
	}, recorder)
	require.NoError(t, err)

	t.Cleanup(func() { _ = handle.Stop(ctx) })

	go func() {
		for dispatch := range recorder.dispatched {
			result := &conveyorv1.Result{
				TaskId:  dispatch.GetTask().GetId(),
				Outcome: conveyorv1.TaskOutcome_TASK_OUTCOME_SUCCESS,
			}
			_ = handle.Tell(context.Background(), result)
		}
	}()

	require.NoError(t, engine.TellQueue(ctx, queue, &conveyorv1.DrainQueue{Queue: queue}))

	require.Eventually(t, func() bool {
		paused, err := taskLog.QueuePaused(ctx, queue)

		return err == nil && paused
	}, 10*time.Second, 10*time.Millisecond, "drain must persist the pause flag")

	// Work enqueued while paused stays in the broker.
	require.NoError(t, engine.Enqueue(ctx, newTask("task-while-paused", queue, "test:paused", 4)))

	time.Sleep(500 * time.Millisecond)

	_, state, err := taskLog.GetTask(ctx, "task-while-paused")
	require.NoError(t, err)
	require.Equal(t, conveyorv1.TaskState_TASK_STATE_PENDING, state, "paused queues must not dispatch")

	require.NoError(t, engine.TellQueue(ctx, queue, &conveyorv1.ResumeQueue{Queue: queue}))

	requireTaskState(t, engine, "task-while-paused", conveyorv1.TaskState_TASK_STATE_COMPLETED)

	paused, err := taskLog.QueuePaused(ctx, queue)
	require.NoError(t, err)
	require.False(t, paused)
}

// TestQueueGrainBroadcastsCancelToGateways drives the admin-cancel fan-out:
// a CancelActive delivered to the queue grain is forwarded to every registered
// gateway, and the session holding the task emits a worker Cancel frame.
func TestQueueGrainBroadcastsCancelToGateways(t *testing.T) {
	const queue = "grain-cancel"

	ctx := context.Background()
	taskLog := memory.New(clock.System())
	pauseQueue(t, taskLog, queue)
	engine := startEngine(t, taskLog)
	recorder := newFrameRecorder()

	handle, err := engine.SpawnGateway(ctx, GatewaySession{
		SessionID:   "session-grain-cancel",
		Queues:      []string{queue},
		Concurrency: 4,
	}, recorder)
	require.NoError(t, err)

	t.Cleanup(func() { _ = handle.Stop(ctx) })

	// Put a task in flight on the gateway directly so it has something to cancel.
	execute := leaseOne(t, engine, queue, "task-grain-cancel", "lease-gc")
	require.NoError(t, handle.Tell(ctx, execute))

	// Route the cancel through the queue grain. Once the gateway's registration
	// has reached the grain, the broadcast lands on the owning session as a
	// Cancel frame; retry the tell until that registration window closes.
	require.Eventually(t, func() bool {
		require.NoError(t, engine.TellQueue(ctx, queue, &conveyorv1.CancelActive{TaskId: "task-grain-cancel"}))

		ids := recorder.cancels()

		return len(ids) >= 1 && ids[0] == "task-grain-cancel"
	}, 10*time.Second, 50*time.Millisecond, "the grain must broadcast the admin cancel to the owning gateway")
}

// TestQueueGrainBroadcastCancelToDeadGatewayIsBestEffort covers the failure
// branch of broadcastCancel: a registered gateway with no live actor makes the
// forwarding TellActor fail. The cancel is best-effort, so the error is logged
// and the grain turn still completes cleanly rather than escalating.
func TestQueueGrainBroadcastCancelToDeadGatewayIsBestEffort(t *testing.T) {
	const queue = "cancel-dead-gateway"

	ctx := context.Background()
	taskLog := memory.New(clock.System())
	engine := startEngine(t, taskLog)

	// A phantom gateway: registered for credits but with no actor behind it,
	// so the cancel broadcast's TellActor fails.
	require.NoError(t, engine.TellQueue(ctx, queue, &conveyorv1.RegisterGateway{
		Queue:       queue,
		GatewayName: "ghost-gateway",
		Capacity:    1,
	}))

	require.NoError(t, engine.TellQueue(ctx, queue, &conveyorv1.CancelActive{TaskId: "task-anything"}),
		"a cancel that cannot reach its gateway is logged, not failed")
}

// TestQueueGrainReleasesUndispatchableTasks drives the dispatch-failure
// path: a gateway registered with credits but no live actor makes the
// grain's TellActor dispatch fail. The grain must drop the gateway and
// release the leased task back to pending so it redelivers, rather than
// stranding it as leased until the lease expires.
func TestQueueGrainReleasesUndispatchableTasks(t *testing.T) {
	const queue = "default"

	ctx := context.Background()
	taskLog := memory.New(clock.System())
	engine := startEngine(t, taskLog)

	// A phantom gateway: credits without an actor. Registering it directly
	// grants the grain dispatch credits, but no actor answers TellActor.
	require.NoError(t, engine.TellQueue(ctx, queue, &conveyorv1.RegisterGateway{
		Queue:       queue,
		GatewayName: "ghost-gateway",
		Capacity:    1,
	}))

	require.NoError(t, engine.Enqueue(ctx, newTask("task-ghost", queue, "test:ok", 4)))

	// Only the phantom gateway exists, so the grain leases the task, fails to
	// dispatch it, drops the gateway, and releases the task. Settle, then
	// assert it landed back in pending rather than stranded as leased — which
	// would block redelivery for a full lease TTL.
	time.Sleep(500 * time.Millisecond)

	_, state, err := taskLog.GetTask(ctx, "task-ghost")
	require.NoError(t, err)
	require.Equal(t, conveyorv1.TaskState_TASK_STATE_PENDING, state, "an undispatchable task must be released back to pending")

	// A real gateway now drains it promptly, proving the task was available
	// in the broker rather than held under a dead lease.
	spawnGateway(t, engine, &mockGateway{queue: queue, capacity: 1})

	requireTaskState(t, engine, "task-ghost", conveyorv1.TaskState_TASK_STATE_COMPLETED)
}

// TestQueueGrainSetPausedBrokerError covers the persistence failure inside
// setPaused: a DrainQueue whose pause write fails surfaces the broker error
// back through the grain turn rather than flipping the flag, so the caller
// learns the queue was not actually paused.
func TestQueueGrainSetPausedBrokerError(t *testing.T) {
	const queue = "pause-fault"

	ctx := context.Background()
	taskLog := newFaultBroker(memory.New(clock.System()))
	engine := startEngine(t, taskLog)

	taskLog.fault(methodSetQueuePaused, errors.New("pause write down"))

	err := engine.TellQueue(ctx, queue, &conveyorv1.DrainQueue{Queue: queue})
	require.Error(t, err, "a failed pause write must surface through the grain turn")
}

// TestQueueGrainReleaseLeasedBrokerError covers the failure branch of
// releaseLeased: when an undispatchable batch cannot be released back to
// pending, the members stay leased until the reaper reclaims them on lease
// expiry rather than redelivering immediately.
func TestQueueGrainReleaseLeasedBrokerError(t *testing.T) {
	const queue = "release-fault"

	ctx := context.Background()
	taskLog := newFaultBroker(memory.New(clock.System()))
	engine := startEngine(t, taskLog)

	// A phantom gateway grants credits with no actor behind it, so the grain
	// leases the task, fails to dispatch it, and tries to release it.
	require.NoError(t, engine.TellQueue(ctx, queue, &conveyorv1.RegisterGateway{
		Queue:       queue,
		GatewayName: "ghost-gateway",
		Capacity:    1,
	}))

	// The release itself fails, so the task cannot return to pending.
	taskLog.fault(methodRelease, errors.New("release down"))

	require.NoError(t, engine.Enqueue(ctx, newTask("task-stuck", queue, "test:ok", 4)))

	// Settle the lease-and-failed-release cycle, then assert the task is held
	// active under its lease rather than back in pending.
	time.Sleep(500 * time.Millisecond)

	_, state, err := taskLog.GetTask(ctx, "task-stuck")
	require.NoError(t, err)
	require.Equal(t, conveyorv1.TaskState_TASK_STATE_ACTIVE, state,
		"a task whose release failed stays leased until the reaper reclaims it")
}

// TestQueueGrainWeightedDispatchTracksWeights drives the full dispatch path with
// two gateways of different declared weights on one queue and asserts the
// delivered task counts track the 3:1 weight ratio within tolerance. Both
// gateways carry capacity well above their share of the stream, so the weighting
// — not credit exhaustion — sets the proportions: credit flow control caps a
// saturated gateway and spills its overflow to peers, which would equalize the
// split and mask the weights. The guarantee under test is that while credits are
// available, selection follows the declared weights.
func TestQueueGrainWeightedDispatchTracksWeights(t *testing.T) {
	const (
		queue = "weighted"
		total = 400
	)

	taskLog := memory.New(clock.System())
	engine := startEngine(t, taskLog)

	heavyLog := &dispatchLog{}
	lightLog := &dispatchLog{}

	spawnGateway(t, engine, &mockGateway{queue: queue, name: "heavy", capacity: 1000, weight: 3, log: heavyLog})
	spawnGateway(t, engine, &mockGateway{queue: queue, name: "light", capacity: 1000, weight: 1, log: lightLog})

	enqueueTasks(t, engine, queue, total)

	require.Eventually(t, func() bool {
		return len(heavyLog.snapshot())+len(lightLog.snapshot()) >= total
	}, 30*time.Second, 20*time.Millisecond, "every enqueued task must be dispatched")

	heavy, light := len(heavyLog.snapshot()), len(lightLog.snapshot())
	require.Equal(t, total, heavy+light, "no task is dispatched twice")

	// Expected split is 300:100. Allow a generous tolerance: a lease batch can
	// straddle a credit boundary and shift a handful of tasks between gateways.
	require.InDelta(t, 300, heavy, 40, "the weight-3 gateway draws ~three quarters of the stream")
	require.InDelta(t, 100, light, 40, "the weight-1 gateway draws ~one quarter of the stream")
}

// concurrencyGrain builds a bare grain backed by a fake clock and empty memory
// broker, with the per-key maps initialized as OnActivate would.
func concurrencyGrain(t *testing.T) (*QueueGrain, *memory.Broker) {
	t.Helper()

	fake := clock.NewFake(time.Date(2026, 6, 16, 12, 0, 0, 0, time.UTC))
	taskLog := memory.New(fake)
	t.Cleanup(func() { _ = taskLog.Close() })

	runtime := NewRuntime(taskLog, fake, testSettings, quietLogger())

	return &QueueGrain{
		runtime:     runtime,
		queue:       "default",
		activeByKey: map[string]int{},
		inFlightKey: map[string]string{},
	}, taskLog
}

func TestLoadConcurrencyLimit(t *testing.T) {
	grain, taskLog := concurrencyGrain(t)
	require.NoError(t, taskLog.SetQueueConcurrencyLimit(context.Background(), "default", 5))

	require.NoError(t, grain.loadConcurrencyLimit(context.Background()))
	require.Equal(t, 5, grain.concurrencyLimit)
}

func TestLoadConcurrencyLimitUnsetIsUnbounded(t *testing.T) {
	grain, _ := concurrencyGrain(t)

	require.NoError(t, grain.loadConcurrencyLimit(context.Background()))
	require.Equal(t, 0, grain.concurrencyLimit, "no override leaves keys unbounded")
}

func TestApplyConcurrencyLimitChange(t *testing.T) {
	grain, _ := concurrencyGrain(t)

	grain.applyConcurrencyLimitChange(&conveyorv1.ConcurrencyLimitChanged{Queue: "default", MaxActive: 7})
	require.Equal(t, 7, grain.concurrencyLimit)

	grain.applyConcurrencyLimitChange(&conveyorv1.ConcurrencyLimitChanged{Queue: "default"})
	require.Equal(t, 0, grain.concurrencyLimit, "a zero max-active clears the limit")
}

func TestReleaseConcurrencyKey(t *testing.T) {
	grain, _ := concurrencyGrain(t)
	grain.activeByKey = map[string]int{"k": 2}
	grain.inFlightKey = map[string]string{"t1": "k", "t2": "k"}

	grain.releaseConcurrencyKey("t1")
	require.Equal(t, 1, grain.activeByKey["k"])

	grain.releaseConcurrencyKey("t2")
	_, ok := grain.activeByKey["k"]
	require.False(t, ok, "the key is dropped once it reaches zero")

	// Unknown task ids and keyless tasks are no-ops.
	grain.releaseConcurrencyKey("unknown")
	require.Empty(t, grain.inFlightKey)
}

// receiveDispatch waits up to within for the next dispatch frame.
func receiveDispatch(recorder *frameRecorder, within time.Duration) (*conveyorv1.Dispatch, bool) {
	select {
	case dispatch := <-recorder.dispatched:
		return dispatch, true
	case <-time.After(within):
		return nil, false
	}
}

// TestConcurrencyLimitCapsActivePerKey drives the full enforcement path: with a
// limit of 2, only two tasks sharing a key dispatch at once, the rest are held
// back, and completing one frees a slot for the next.
func TestConcurrencyLimitCapsActivePerKey(t *testing.T) {
	ctx := context.Background()
	taskLog := memory.New(clock.System())
	require.NoError(t, taskLog.SetQueueConcurrencyLimit(ctx, "default", 2))

	engine := startEngine(t, taskLog)
	recorder := newFrameRecorder()

	handle, err := engine.SpawnGateway(ctx, GatewaySession{
		SessionID:   "session-concurrency",
		Queues:      []string{"default"},
		Concurrency: 10,
	}, recorder)
	require.NoError(t, err)

	t.Cleanup(func() { _ = handle.Stop(ctx) })

	// Five tasks share one concurrency key; the gateway has ample credits, so
	// only the key limit can hold them back.
	for i := range 5 {
		task := newTask(fmt.Sprintf("task-%02d", i), "default", "test:ok", 5)
		task.Options.ConcurrencyKey = "tenant-1"
		require.NoError(t, engine.Enqueue(ctx, task))
	}

	// Exactly two dispatch.
	first, ok := receiveDispatch(recorder, 2*time.Second)
	require.True(t, ok, "the first task should dispatch")
	_, ok = receiveDispatch(recorder, 2*time.Second)
	require.True(t, ok, "the second task should dispatch")

	// No third while the key is at its limit, even across a reaper sweep.
	_, ok = receiveDispatch(recorder, 400*time.Millisecond)
	require.False(t, ok, "a third task must not dispatch while the key holds its 2 active slots")

	// Completing one frees a slot; a third dispatches.
	require.NoError(t, handle.Tell(ctx, &conveyorv1.Result{
		TaskId:  first.GetTask().GetId(),
		Outcome: conveyorv1.TaskOutcome_TASK_OUTCOME_SUCCESS,
	}))

	_, ok = receiveDispatch(recorder, 2*time.Second)
	require.True(t, ok, "a held-back task should dispatch once a slot frees")
}

// TestConcurrencyKeyRedispatchAfterLeaseExpiry guards the slot-reconciliation
// fix: a limit-1 ("mutex") key whose worker crashes must re-dispatch its task
// after the reaper reclaims the expired lease, not deadlock behind the crashed
// task's own stale slot. Uses short-lease recovery settings so the lease expires
// within test time.
func TestConcurrencyKeyRedispatchAfterLeaseExpiry(t *testing.T) {
	ctx := context.Background()
	taskLog := memory.New(clock.System())
	require.NoError(t, taskLog.SetQueueConcurrencyLimit(ctx, "default", 1))

	engine := newNode(taskLog, recoverySettings, freePorts(t, 3), nil)
	require.NoError(t, engine.Start(ctx))

	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = engine.Stop(stopCtx)
	})

	recorder := newFrameRecorder()

	handle, err := engine.SpawnGateway(ctx, GatewaySession{
		SessionID:   "session-mutex",
		Queues:      []string{"default"},
		Concurrency: 10,
	}, recorder)
	require.NoError(t, err)

	t.Cleanup(func() { _ = handle.Stop(ctx) })

	task := newTask("task-mutex", "default", "test:ok", 5)
	task.Options.ConcurrencyKey = "resource-1"
	require.NoError(t, engine.Enqueue(ctx, task))

	// First dispatch; the worker never reports completion, simulating a crash.
	first, ok := receiveDispatch(recorder, 3*time.Second)
	require.True(t, ok, "the task should dispatch")
	require.Equal(t, "task-mutex", first.GetTask().GetId())

	// The lease expires and the reaper reclaims the task to retry. The key must
	// re-dispatch it rather than deadlock on its own stale slot.
	second, ok := receiveDispatch(recorder, 8*time.Second)
	require.True(t, ok, "the limit-1 key must re-dispatch its task after a crash, not deadlock on its own slot")
	require.Equal(t, "task-mutex", second.GetTask().GetId())
}
