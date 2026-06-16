// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package actors

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/conveyorq/conveyor/internal/broker/memory"
	"github.com/conveyorq/conveyor/internal/clock"
	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
)

func TestQueueGrainNameRoundTrip(t *testing.T) {
	require.Equal(t, "queue.critical", QueueGrainName("critical"))
	require.Equal(t, "critical", queueFromGrainName(QueueGrainName("critical")))
}

func TestRegisterGatewayGrantsInitialCredits(t *testing.T) {
	grain := &QueueGrain{runtime: newTestRuntime(t)}

	grain.registerGateway(&conveyorv1.RegisterGateway{GatewayName: "gateway-1", Capacity: 4})

	require.Len(t, grain.gateways, 1)
	require.EqualValues(t, 4, grain.gateways[0].credits)
	require.Equal(t, 4, grain.totalCredits())
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
