// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package actors

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	goakt "github.com/tochemey/goakt/v4/actor"
	goaktlog "github.com/tochemey/goakt/v4/log"

	"github.com/conveyorq/conveyor/internal/broker"
	"github.com/conveyorq/conveyor/internal/broker/memory"
	"github.com/conveyorq/conveyor/internal/clock"
	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
)

func TestDependencyResolverPreStartRequiresRuntimeExtension(t *testing.T) {
	ctx := context.Background()

	system, err := goakt.NewActorSystem("bare-resolver-system", goakt.WithLogger(goaktlog.DiscardLogger))
	require.NoError(t, err)
	require.NoError(t, system.Start(ctx))

	t.Cleanup(func() { _ = system.Stop(ctx) })

	_, err = system.Spawn(ctx, "resolver-no-runtime", NewDependencyResolver())
	require.ErrorContains(t, err, "is not registered")
}

func TestDependencyResolverIgnoresUnknownMessage(t *testing.T) {
	requireUnhandled(t, spawnIsolated(t, "extra-resolver", NewDependencyResolver()), new(conveyorv1.ReapTick))
}

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

// blockingResolveBroker blocks every ResolveDependents until released and
// tracks how many run at once.
type blockingResolveBroker struct {
	broker.Broker
	// running counts resolutions in progress.
	running atomic.Int32
	// peak is the highest number of resolutions seen in progress at once.
	peak atomic.Int32
	// calls counts resolutions started.
	calls atomic.Int32
	// release unblocks one resolution per receive.
	release chan struct{}
}

// ResolveDependents blocks until released, recording concurrency.
func (b *blockingResolveBroker) ResolveDependents(_ context.Context, _ string) ([]string, error) {
	b.calls.Add(1)
	now := b.running.Add(1)

	for {
		peak := b.peak.Load()
		if now <= peak || b.peak.CompareAndSwap(peak, now) {
			break
		}
	}

	<-b.release
	b.running.Add(-1)

	return nil, nil
}

func TestDependencyResolverRunsOneResolutionAtATime(t *testing.T) {
	ctx := context.Background()
	blocking := &blockingResolveBroker{Broker: memory.New(clock.System()), release: make(chan struct{})}
	engine := startEngine(t, blocking)

	pid, err := engine.System().Spawn(ctx, "resolver-one-at-a-time", NewDependencyResolver())
	require.NoError(t, err)

	for _, id := range []string{"task-1", "task-2", "task-3"} {
		require.NoError(t, goakt.Tell(ctx, pid, &conveyorv1.ResolveDependents{TaskId: id}))
	}

	require.Eventually(t, func() bool { return blocking.calls.Load() == 1 }, 5*time.Second, 10*time.Millisecond)
	require.Never(t, func() bool { return blocking.calls.Load() > 1 }, 200*time.Millisecond, 20*time.Millisecond,
		"a routee queues requests while one resolution is in flight")

	for range 3 {
		blocking.release <- struct{}{}
	}

	require.Eventually(t, func() bool { return blocking.calls.Load() == 3 && blocking.running.Load() == 0 },
		5*time.Second, 10*time.Millisecond, "the queued requests run in turn")
	require.EqualValues(t, 1, blocking.peak.Load())
}

func TestDependencyResolverClearsInFlightStateOnStart(t *testing.T) {
	ctx := context.Background()
	counting := &callCountingBroker{Broker: memory.New(clock.System())}
	system := startBareSystem(t, counting)

	// As a restart that lost a resolution's completion would leave it.
	resolver := &DependencyResolver{resolving: true, pending: []string{"task-stale"}}

	pid, err := system.Spawn(ctx, "resolver-restarted", resolver)
	require.NoError(t, err)
	require.NoError(t, goakt.Tell(ctx, pid, &conveyorv1.ResolveDependents{TaskId: "task-1"}))

	require.Eventually(t, func() bool { return counting.resolutions.Load() == 1 },
		5*time.Second, 10*time.Millisecond, "a request after start is resolved")
	require.Never(t, func() bool { return counting.resolutions.Load() > 1 },
		200*time.Millisecond, 20*time.Millisecond, "requests queued before the restart died with its mailbox")
}
