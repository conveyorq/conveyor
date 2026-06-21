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

// TestDependencyChainDispatchesAfterDependency drives the full Phase 3 inline
// path: a dependent starts blocked, the dependency completes through the
// gateway, the gateway hands the finished task to the resolver pool, the
// resolver promotes the dependent and wakes its queue, and the dependent is
// then dispatched and completed.
func TestDependencyChainDispatchesAfterDependency(t *testing.T) {
	ctx := context.Background()
	taskLog := memory.New(clock.System())
	engine := startEngine(t, taskLog)
	recorder := newFrameRecorder()

	handle, err := engine.SpawnGateway(ctx, GatewaySession{
		SessionID:   "session-deps",
		Queues:      []string{"default"},
		Concurrency: 4,
	}, recorder)
	require.NoError(t, err)

	t.Cleanup(func() { _ = handle.Stop(ctx) })

	// Act as the worker: every dispatched task succeeds immediately.
	go func() {
		for dispatch := range recorder.dispatched {
			_ = handle.Tell(context.Background(), &conveyorv1.Result{
				TaskId:  dispatch.GetTask().GetId(),
				Outcome: conveyorv1.TaskOutcome_TASK_OUTCOME_SUCCESS,
			})
		}
	}()

	// task-b waits on task-a, so it cannot run until task-a completes.
	taskA := newTask("task-a", "default", "test:ok", 5)
	taskB := newTask("task-b", "default", "test:ok", 5)
	taskB.Options.DependsOn = []*conveyorv1.TaskDependency{{TaskId: "task-a"}}

	require.NoError(t, engine.Enqueue(ctx, taskA))
	require.NoError(t, engine.Enqueue(ctx, taskB))

	// The dependent can only reach completed if the dependency's completion
	// resolved it and woke its queue; a broken chain leaves it blocked forever.
	require.Eventually(t, completedReaches(taskLog, 2),
		time.Minute, 20*time.Millisecond, "the dependency and its dependent should both complete")

	requireDrained(t, taskLog)
}

// TestDependencyResolvedByReaperWhenNoResolverPool verifies the safety net: with
// the resolver pool disabled, the dependency completes with no inline
// resolution, and the reaper sweep alone promotes and dispatches the dependent.
func TestDependencyResolvedByReaperWhenNoResolverPool(t *testing.T) {
	ctx := context.Background()
	taskLog := memory.New(clock.System())

	settings := testSettings
	settings.ResolverPoolSize = 0

	engine := newNode(taskLog, settings, freePorts(t, 3), nil)
	require.NoError(t, engine.Start(ctx))

	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = engine.Stop(stopCtx)
	})

	recorder := newFrameRecorder()

	handle, err := engine.SpawnGateway(ctx, GatewaySession{
		SessionID:   "session-deps-reaper",
		Queues:      []string{"default"},
		Concurrency: 4,
	}, recorder)
	require.NoError(t, err)

	t.Cleanup(func() { _ = handle.Stop(ctx) })

	go func() {
		for dispatch := range recorder.dispatched {
			_ = handle.Tell(context.Background(), &conveyorv1.Result{
				TaskId:  dispatch.GetTask().GetId(),
				Outcome: conveyorv1.TaskOutcome_TASK_OUTCOME_SUCCESS,
			})
		}
	}()

	taskA := newTask("task-a", "default", "test:ok", 5)
	taskB := newTask("task-b", "default", "test:ok", 5)
	taskB.Options.DependsOn = []*conveyorv1.TaskDependency{{TaskId: "task-a"}}

	require.NoError(t, engine.Enqueue(ctx, taskA))
	require.NoError(t, engine.Enqueue(ctx, taskB))

	require.Eventually(t, completedReaches(taskLog, 2),
		time.Minute, 20*time.Millisecond, "the reaper sweep should promote the dependent without a resolver pool")

	requireDrained(t, taskLog)
}

// TestDependencyContinuesAfterDependencyFailure drives the terminal-failure
// resolution path: the dependency is archived (skip-retry), and its
// continue-on-failure dependent is resolved off the gateway's turn and runs to
// completion rather than waiting for the reaper.
func TestDependencyContinuesAfterDependencyFailure(t *testing.T) {
	ctx := context.Background()
	taskLog := memory.New(clock.System())
	engine := startEngine(t, taskLog)
	recorder := newFrameRecorder()

	handle, err := engine.SpawnGateway(ctx, GatewaySession{
		SessionID:   "session-fail-dep",
		Queues:      []string{"default"},
		Concurrency: 4,
	}, recorder)
	require.NoError(t, err)

	t.Cleanup(func() { _ = handle.Stop(ctx) })

	// Worker: archive task-a (skip-retry), succeed every other task.
	go func() {
		for dispatch := range recorder.dispatched {
			id := dispatch.GetTask().GetId()
			outcome := conveyorv1.TaskOutcome_TASK_OUTCOME_SUCCESS

			if id == "task-a" {
				outcome = conveyorv1.TaskOutcome_TASK_OUTCOME_SKIP_RETRY
			}

			_ = handle.Tell(context.Background(), &conveyorv1.Result{TaskId: id, Outcome: outcome})
		}
	}()

	taskA := newTask("task-a", "default", "test:ok", 5)
	taskB := newTask("task-b", "default", "test:ok", 5)
	taskB.Options.DependsOn = []*conveyorv1.TaskDependency{{
		TaskId:    "task-a",
		OnFailure: conveyorv1.DependencyFailurePolicy_DEPENDENCY_FAILURE_POLICY_CONTINUE,
	}}

	require.NoError(t, engine.Enqueue(ctx, taskA))
	require.NoError(t, engine.Enqueue(ctx, taskB))

	// The failed dependency does not block its continue-on-failure dependent.
	require.Eventually(t, completedReaches(taskLog, 1),
		time.Minute, 20*time.Millisecond, "the continue-on-failure dependent should complete")

	require.Eventually(t, func() bool {
		archived, archiveErr := tasksInState(taskLog, conveyorv1.TaskState_TASK_STATE_ARCHIVED)

		return archiveErr == nil && archived == 1
	}, time.Minute, 20*time.Millisecond, "the failed dependency stays archived")
}
