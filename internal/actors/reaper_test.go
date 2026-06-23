// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package actors

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	goakt "github.com/tochemey/goakt/v4/actor"
	goaktlog "github.com/tochemey/goakt/v4/log"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/conveyorq/conveyor/internal/broker"
	"github.com/conveyorq/conveyor/internal/broker/memory"
	"github.com/conveyorq/conveyor/internal/clock"
	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
)

func TestReaperPreStartRequiresRuntimeExtension(t *testing.T) {
	ctx := context.Background()

	system, err := goakt.NewActorSystem("bare-reaper-system", goakt.WithLogger(goaktlog.DiscardLogger))
	require.NoError(t, err)
	require.NoError(t, system.Start(ctx))

	t.Cleanup(func() { _ = system.Stop(ctx) })

	_, err = system.Spawn(ctx, "reaper-no-runtime", NewReaper())
	require.ErrorContains(t, err, "is not registered")
}

func TestReaperIgnoresUnknownMessage(t *testing.T) {
	ctx := context.Background()
	engine := startEngine(t, memory.New(clock.System()))

	pid, err := engine.System().Spawn(ctx, "extra-reaper", NewReaper())
	require.NoError(t, err)
	require.NoError(t, goakt.Tell(ctx, pid, new(conveyorv1.PromoteTick)))
	require.True(t, pid.IsRunning())
}

// TestReaperReclaimsExpiredLeases verifies lease reaping: an active task
// whose lease lapsed returns to retry with an incremented counter. The
// queue is paused so no grain re-leases it away from the assertion.
func TestReaperReclaimsExpiredLeases(t *testing.T) {
	const queue = "reaping"

	ctx := context.Background()
	taskLog := memory.New(clock.System())
	pauseQueue(t, taskLog, queue)
	engine := startEngine(t, taskLog)

	require.NoError(t, taskLog.Enqueue(ctx, newTask("task-stalled", queue, "test:stall", 4)))

	leased, err := taskLog.Lease(ctx, queue, 1, 50*time.Millisecond, "lease-stall")
	require.NoError(t, err)
	require.Len(t, leased, 1)

	requireTaskState(t, engine, "task-stalled", conveyorv1.TaskState_TASK_STATE_RETRY)

	envelope, _, err := taskLog.GetTask(ctx, "task-stalled")
	require.NoError(t, err)
	require.EqualValues(t, 1, envelope.GetRetried(), "a reclaimed lease counts as one retry")
}

// TestReaperArchivesExpiredTasks verifies the pre-dispatch TTL end to end: a
// task whose expiry lapses while it waits is moved to archived by the reaper's
// sweep, not run. The queue is paused so the only path to a terminal state is
// the reaper, proving the actor drives it.
func TestReaperArchivesExpiredTasks(t *testing.T) {
	const queue = "expiring"

	ctx := context.Background()
	taskLog := memory.New(clock.System())
	pauseQueue(t, taskLog, queue)
	engine := startEngine(t, taskLog)

	task := newTask("task-expiring", queue, "test:expire", 4)
	task.Options.ExpiresAt = timestamppb.New(clock.System().Now().Add(100 * time.Millisecond))
	require.NoError(t, taskLog.Enqueue(ctx, task))

	requireTaskState(t, engine, "task-expiring", conveyorv1.TaskState_TASK_STATE_ARCHIVED)

	envelope, _, err := taskLog.GetTask(ctx, "task-expiring")
	require.NoError(t, err)
	require.Equal(t, broker.TaskExpiredMessage, envelope.GetLastError())
}

// TestReaperSweepRecoversLostWakeups verifies the sweep backstop: work
// committed to the broker without any wake-up hint still gets dispatched.
func TestReaperSweepRecoversLostWakeups(t *testing.T) {
	ctx := context.Background()
	taskLog := memory.New(clock.System())
	engine := startEngine(t, taskLog)
	recorder := newFrameRecorder()

	handle, err := engine.SpawnGateway(ctx, GatewaySession{
		SessionID:   "session-sweep",
		Queues:      []string{"swept"},
		Concurrency: 2,
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

	// Straight to the broker: no TaskEnqueued hint ever fires.
	require.NoError(t, taskLog.Enqueue(ctx, newTask("task-swept", "swept", "test:sweep", 4)))

	requireTaskState(t, engine, "task-swept", conveyorv1.TaskState_TASK_STATE_COMPLETED)
}
