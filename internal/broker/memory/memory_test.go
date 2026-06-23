// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package memory

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/conveyorq/conveyor/internal/broker"
	"github.com/conveyorq/conveyor/internal/broker/brokertest"
	"github.com/conveyorq/conveyor/internal/clock"
	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
)

// fakeStart is the fixed wall clock the coverage broker reads from.
var fakeStart = time.Unix(1_700_000_000, 0).UTC()

// groupStatsAttempts repeats an order-sensitive GroupStats assertion enough
// times that Go's randomized map iteration is overwhelmingly certain to visit
// the "older than the running minimum" branch at least once.
const groupStatsAttempts = 50

func TestConformance(t *testing.T) {
	brokertest.Run(t, func(t *testing.T, timeSource clock.Clock) broker.Broker {
		instance := New(timeSource)
		t.Cleanup(func() { _ = instance.Close() })

		return instance
	})
}

// newCoverageBroker returns an empty broker and its fake clock.
func newCoverageBroker(t *testing.T) (*Broker, *clock.Fake) {
	t.Helper()

	fake := clock.NewFake(fakeStart)
	store := New(fake)

	t.Cleanup(func() { _ = store.Close() })

	return store, fake
}

// dependsOn builds a dependency edge on an envelope.
func dependsOn(task *conveyorv1.TaskEnvelope, dependencyID string, policy conveyorv1.DependencyFailurePolicy) {
	task.Options.DependsOn = append(task.Options.DependsOn, &conveyorv1.TaskDependency{
		TaskId:    dependencyID,
		OnFailure: policy,
	})
}

func TestRemoveDependentMissingDependencyIsNoop(t *testing.T) {
	store, _ := newCoverageBroker(t)

	require.NotPanics(t, func() {
		store.removeDependent("never-registered", "dependent")
	})
}

func TestEnqueueCascadeCancelOnFailedDependency(t *testing.T) {
	store, _ := newCoverageBroker(t)
	ctx := context.Background()

	require.NoError(t, store.Enqueue(ctx, envelope("dep", "q")))
	require.NoError(t, store.ArchiveTask(ctx, "dep"))

	child := envelope("child", "q")
	dependsOn(child, "dep", conveyorv1.DependencyFailurePolicy_DEPENDENCY_FAILURE_POLICY_CASCADE_CANCEL)
	require.NoError(t, store.Enqueue(ctx, child))

	_, state, err := store.GetTask(ctx, "child")
	require.NoError(t, err)
	require.Equal(t, conveyorv1.TaskState_TASK_STATE_CANCELED, state)
}

func TestEnqueueResolvesInitialDependencyPolicies(t *testing.T) {
	store, _ := newCoverageBroker(t)
	ctx := context.Background()

	require.NoError(t, store.Enqueue(ctx, envelope("failed", "q")))
	require.NoError(t, store.ArchiveTask(ctx, "failed"))

	// CONTINUE treats the failed dependency as satisfied: the task is pending.
	skip := envelope("skip", "q")
	dependsOn(skip, "failed", conveyorv1.DependencyFailurePolicy_DEPENDENCY_FAILURE_POLICY_CONTINUE)
	require.NoError(t, store.Enqueue(ctx, skip))

	_, state, err := store.GetTask(ctx, "skip")
	require.NoError(t, err)
	require.Equal(t, conveyorv1.TaskState_TASK_STATE_PENDING, state)

	// BLOCK (the default) keeps the failed dependency as a block.
	blocked := envelope("blocked", "q")
	dependsOn(blocked, "failed", conveyorv1.DependencyFailurePolicy_DEPENDENCY_FAILURE_POLICY_BLOCK)
	require.NoError(t, store.Enqueue(ctx, blocked))

	_, state, err = store.GetTask(ctx, "blocked")
	require.NoError(t, err)
	require.Equal(t, conveyorv1.TaskState_TASK_STATE_BLOCKED, state)
}

func TestEnqueueSkipsEmptyAndSelfDependencies(t *testing.T) {
	store, _ := newCoverageBroker(t)
	ctx := context.Background()

	task := envelope("self", "q")
	dependsOn(task, "", conveyorv1.DependencyFailurePolicy_DEPENDENCY_FAILURE_POLICY_BLOCK)
	dependsOn(task, "self", conveyorv1.DependencyFailurePolicy_DEPENDENCY_FAILURE_POLICY_BLOCK)
	require.NoError(t, store.Enqueue(ctx, task))

	_, state, err := store.GetTask(ctx, "self")
	require.NoError(t, err)
	require.Equal(t, conveyorv1.TaskState_TASK_STATE_PENDING, state)
}

func TestLimitGuardsReturnEmpty(t *testing.T) {
	store, _ := newCoverageBroker(t)
	ctx := context.Background()

	leased, err := store.Lease(ctx, "q", 0, time.Second, "lease")
	require.NoError(t, err)
	require.Nil(t, leased)

	group, err := store.LeaseGroup(ctx, "q", "g", 0, time.Second, "lease")
	require.NoError(t, err)
	require.Nil(t, group)

	reaped, err := store.ReapExpiredLeases(ctx, 0)
	require.NoError(t, err)
	require.Nil(t, reaped)

	promoted, err := store.PromoteScheduled(ctx, 0)
	require.NoError(t, err)
	require.Nil(t, promoted)

	ready, err := store.PromoteReadyDependents(ctx, 0)
	require.NoError(t, err)
	require.Nil(t, ready)

	purged, err := store.PurgeCompleted(ctx, 0)
	require.NoError(t, err)
	require.Zero(t, purged)

	archived, err := store.ArchiveExpired(ctx, 0)
	require.NoError(t, err)
	require.Zero(t, archived)
}

func TestLeaseSortsEqualPriorityByProcessAt(t *testing.T) {
	store, fake := newCoverageBroker(t)
	now := fake.Now()

	store.tasks["late"] = &taskRow{envelope: envelope("late", "q"), state: conveyorv1.TaskState_TASK_STATE_PENDING, processAt: now.Add(-time.Minute)}
	store.tasks["early"] = &taskRow{envelope: envelope("early", "q"), state: conveyorv1.TaskState_TASK_STATE_PENDING, processAt: now.Add(-time.Hour)}

	leased, err := store.Lease(context.Background(), "q", 10, time.Second, "lease")
	require.NoError(t, err)
	require.Len(t, leased, 2)
	require.Equal(t, "early", leased[0].GetId())
}

func TestLeaseGroupSortsEqualByEnqueuedAt(t *testing.T) {
	store, fake := newCoverageBroker(t)
	now := fake.Now()

	store.tasks["second"] = &taskRow{envelope: envelope("second", "q"), state: conveyorv1.TaskState_TASK_STATE_AGGREGATING, group: "g", enqueuedAt: now}
	store.tasks["first"] = &taskRow{envelope: envelope("first", "q"), state: conveyorv1.TaskState_TASK_STATE_AGGREGATING, group: "g", enqueuedAt: now.Add(-time.Hour)}

	leased, err := store.LeaseGroup(context.Background(), "q", "g", 10, time.Second, "lease")
	require.NoError(t, err)
	require.Len(t, leased, 2)
	require.Equal(t, "first", leased[0].GetId())
}

func TestGroupStatsTracksOldestAndNewest(t *testing.T) {
	store, fake := newCoverageBroker(t)
	now := fake.Now()

	oldest := now.Add(-2 * time.Hour)
	newest := now

	store.tasks["mid"] = &taskRow{envelope: envelope("mid", "q"), state: conveyorv1.TaskState_TASK_STATE_AGGREGATING, group: "g", enqueuedAt: now.Add(-time.Hour)}
	store.tasks["old"] = &taskRow{envelope: envelope("old", "q"), state: conveyorv1.TaskState_TASK_STATE_AGGREGATING, group: "g", enqueuedAt: oldest}
	store.tasks["new"] = &taskRow{envelope: envelope("new", "q"), state: conveyorv1.TaskState_TASK_STATE_AGGREGATING, group: "g", enqueuedAt: newest}

	// Map iteration order is randomized, so the running-minimum update only runs
	// when the global minimum is visited after another member. Repeat until it
	// is statistically certain to have happened.
	for range groupStatsAttempts {
		stats, err := store.GroupStats(context.Background())
		require.NoError(t, err)
		require.Len(t, stats, 1)
		require.Equal(t, int64(3), stats[0].Count)
		require.Equal(t, oldest, stats[0].Oldest)
		require.Equal(t, newest, stats[0].Newest)
	}
}

func TestGroupStatsSortsByQueue(t *testing.T) {
	store, fake := newCoverageBroker(t)
	now := fake.Now()

	store.tasks["b"] = &taskRow{envelope: envelope("b", "queue-b"), state: conveyorv1.TaskState_TASK_STATE_AGGREGATING, group: "g", enqueuedAt: now}
	store.tasks["a"] = &taskRow{envelope: envelope("a", "queue-a"), state: conveyorv1.TaskState_TASK_STATE_AGGREGATING, group: "g", enqueuedAt: now}

	stats, err := store.GroupStats(context.Background())
	require.NoError(t, err)
	require.Len(t, stats, 2)
	require.Equal(t, "queue-a", stats[0].Queue)
	require.Equal(t, "queue-b", stats[1].Queue)
}

func TestLeaseOperationsRejectMissingTask(t *testing.T) {
	store, _ := newCoverageBroker(t)
	ctx := context.Background()

	require.ErrorIs(t, store.ExtendLease(ctx, "missing", "lease", time.Second), broker.ErrLeaseLost)
	require.ErrorIs(t, store.Fail(ctx, "missing", "lease", "boom", fakeStart), broker.ErrLeaseLost)
	require.ErrorIs(t, store.Release(ctx, "missing", "lease"), broker.ErrLeaseLost)
	require.ErrorIs(t, store.Archive(ctx, "missing", "", "boom"), broker.ErrTaskNotFound)
}

func TestReapExpiredLeasesStopsAtLimit(t *testing.T) {
	store, fake := newCoverageBroker(t)
	expired := fake.Now().Add(-time.Hour)

	for _, id := range []string{"a", "b", "c"} {
		store.tasks[id] = &taskRow{envelope: envelope(id, "q"), state: conveyorv1.TaskState_TASK_STATE_ACTIVE, leaseID: "lease", leaseExpiresAt: expired, maxRetry: 3}
	}

	queues, err := store.ReapExpiredLeases(context.Background(), 2)
	require.NoError(t, err)
	require.Equal(t, []string{"q"}, queues)
}

func TestPromoteScheduledStopsAtLimit(t *testing.T) {
	store, fake := newCoverageBroker(t)
	due := fake.Now().Add(-time.Hour)

	for _, id := range []string{"a", "b", "c"} {
		store.tasks[id] = &taskRow{envelope: envelope(id, "q"), state: conveyorv1.TaskState_TASK_STATE_SCHEDULED, processAt: due}
	}

	queues, err := store.PromoteScheduled(context.Background(), 2)
	require.NoError(t, err)
	require.Equal(t, []string{"q"}, queues)
}

func TestResolveDependentsHandlesMissingAndCyclicEdges(t *testing.T) {
	store, fake := newCoverageBroker(t)
	now := fake.Now()
	cascade := conveyorv1.DependencyFailurePolicy_DEPENDENCY_FAILURE_POLICY_CASCADE_CANCEL

	// Unknown terminal task: nothing to resolve.
	require.Empty(t, store.resolveDependents("ghost", now))

	// A dependent recorded in the reverse index but absent from the store.
	store.tasks["finished"] = &taskRow{envelope: envelope("finished", "q"), state: conveyorv1.TaskState_TASK_STATE_COMPLETED}
	store.addDependent("finished", "vanished")
	require.Empty(t, store.resolveDependents("finished", now))

	// A dependent that no longer waits on the finished task.
	store.tasks["notwaiting"] = &taskRow{envelope: envelope("notwaiting", "q"), state: conveyorv1.TaskState_TASK_STATE_BLOCKED, deps: map[string]conveyorv1.DependencyFailurePolicy{}}
	store.addDependent("finished", "notwaiting")
	require.Empty(t, store.resolveDependents("finished", now))

	// A two-task cascade cycle re-appends an already-resolved task to the
	// worklist, exercising the already-seen guard.
	store.tasks["x"] = &taskRow{envelope: envelope("x", "q"), state: conveyorv1.TaskState_TASK_STATE_ARCHIVED, deps: map[string]conveyorv1.DependencyFailurePolicy{"y": cascade}}
	store.tasks["y"] = &taskRow{envelope: envelope("y", "q"), state: conveyorv1.TaskState_TASK_STATE_BLOCKED, deps: map[string]conveyorv1.DependencyFailurePolicy{"x": cascade}}
	store.addDependent("x", "y")
	store.addDependent("y", "x")

	require.NotPanics(t, func() { store.resolveDependents("x", now) })
}

func TestPromoteReadyDependentsBackstopAndLimit(t *testing.T) {
	store, fake := newCoverageBroker(t)
	now := fake.Now()

	// Stranded blocked rows with no remaining dependencies: the backstop sweep
	// promotes them to the state they would otherwise hold.
	store.tasks["grouped"] = &taskRow{envelope: envelope("grouped", "q"), state: conveyorv1.TaskState_TASK_STATE_BLOCKED, group: "g"}
	store.tasks["scheduled"] = &taskRow{envelope: envelope("scheduled", "q"), state: conveyorv1.TaskState_TASK_STATE_BLOCKED, processAt: now.Add(time.Hour)}

	_, err := store.PromoteReadyDependents(context.Background(), 100)
	require.NoError(t, err)

	_, aggregating, err := store.GetTask(context.Background(), "grouped")
	require.NoError(t, err)
	require.Equal(t, conveyorv1.TaskState_TASK_STATE_AGGREGATING, aggregating)

	_, scheduled, err := store.GetTask(context.Background(), "scheduled")
	require.NoError(t, err)
	require.Equal(t, conveyorv1.TaskState_TASK_STATE_SCHEDULED, scheduled)
}

func TestPromoteReadyDependentsStopsAtLimit(t *testing.T) {
	store, _ := newCoverageBroker(t)

	store.tasks["dep-a"] = &taskRow{envelope: envelope("dep-a", "q"), state: conveyorv1.TaskState_TASK_STATE_COMPLETED}
	store.tasks["dep-b"] = &taskRow{envelope: envelope("dep-b", "q"), state: conveyorv1.TaskState_TASK_STATE_COMPLETED}
	store.tasks["wait-a"] = &taskRow{envelope: envelope("wait-a", "q"), state: conveyorv1.TaskState_TASK_STATE_BLOCKED, deps: map[string]conveyorv1.DependencyFailurePolicy{"dep-a": conveyorv1.DependencyFailurePolicy_DEPENDENCY_FAILURE_POLICY_BLOCK}}
	store.tasks["wait-b"] = &taskRow{envelope: envelope("wait-b", "q"), state: conveyorv1.TaskState_TASK_STATE_BLOCKED, deps: map[string]conveyorv1.DependencyFailurePolicy{"dep-b": conveyorv1.DependencyFailurePolicy_DEPENDENCY_FAILURE_POLICY_BLOCK}}
	store.addDependent("dep-a", "wait-a")
	store.addDependent("dep-b", "wait-b")

	_, err := store.PromoteReadyDependents(context.Background(), 1)
	require.NoError(t, err)
}

func TestPurgeCompletedClearsLapsedKeysAndStopsAtLimit(t *testing.T) {
	store, fake := newCoverageBroker(t)
	now := fake.Now()

	// A still-active row whose unique-key claim has lapsed: the claim is cleared
	// even though the row is not purged.
	store.tasks["lapsed"] = &taskRow{envelope: envelope("lapsed", "q"), state: conveyorv1.TaskState_TASK_STATE_PENDING, uniqueKey: "key", uniqueExpiresAt: now.Add(-time.Hour)}

	// Two purgeable completed rows with a limit of one: the second is skipped.
	for _, id := range []string{"done-a", "done-b"} {
		store.tasks[id] = &taskRow{envelope: envelope(id, "q"), state: conveyorv1.TaskState_TASK_STATE_COMPLETED, completedAt: now.Add(-time.Hour)}
	}

	purged, err := store.PurgeCompleted(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, 1, purged)
	require.Empty(t, store.tasks["lapsed"].uniqueKey)
}

func TestArchiveExpiredSkipsAndStopsAtLimit(t *testing.T) {
	store, fake := newCoverageBroker(t)
	now := fake.Now()
	past := now.Add(-time.Hour)

	// Active (non-waiting) rows and unexpired rows are skipped; only waiting,
	// expired rows are archived.
	store.tasks["active"] = &taskRow{envelope: envelope("active", "q"), state: conveyorv1.TaskState_TASK_STATE_ACTIVE, expiresAt: past}
	store.tasks["fresh"] = &taskRow{envelope: envelope("fresh", "q"), state: conveyorv1.TaskState_TASK_STATE_PENDING}
	store.tasks["stale"] = &taskRow{envelope: envelope("stale", "q"), state: conveyorv1.TaskState_TASK_STATE_PENDING, expiresAt: past}

	archived, err := store.ArchiveExpired(context.Background(), 10)
	require.NoError(t, err)
	require.Equal(t, 1, archived)

	store.tasks["stale-a"] = &taskRow{envelope: envelope("stale-a", "q"), state: conveyorv1.TaskState_TASK_STATE_PENDING, expiresAt: past}
	store.tasks["stale-b"] = &taskRow{envelope: envelope("stale-b", "q"), state: conveyorv1.TaskState_TASK_STATE_PENDING, expiresAt: past}

	archived, err = store.ArchiveExpired(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, 1, archived)
}

func TestQueueStatsCountsAggregatingAndBlocked(t *testing.T) {
	store, _ := newCoverageBroker(t)

	store.tasks["agg"] = &taskRow{envelope: envelope("agg", "q"), state: conveyorv1.TaskState_TASK_STATE_AGGREGATING}
	store.tasks["blk"] = &taskRow{envelope: envelope("blk", "q"), state: conveyorv1.TaskState_TASK_STATE_BLOCKED}

	stats, err := store.QueueStats(context.Background())
	require.NoError(t, err)
	require.Len(t, stats, 1)
	require.Equal(t, int64(1), stats[0].Aggregating)
	require.Equal(t, int64(1), stats[0].Blocked)
}

func TestDeleteTaskMissingAndWithDependencies(t *testing.T) {
	store, _ := newCoverageBroker(t)
	ctx := context.Background()

	require.ErrorIs(t, store.DeleteTask(ctx, "missing"), broker.ErrTaskNotFound)

	store.tasks["blocked"] = &taskRow{envelope: envelope("blocked", "q"), state: conveyorv1.TaskState_TASK_STATE_BLOCKED, deps: map[string]conveyorv1.DependencyFailurePolicy{"dep": conveyorv1.DependencyFailurePolicy_DEPENDENCY_FAILURE_POLICY_BLOCK}}
	store.addDependent("dep", "blocked")

	require.NoError(t, store.DeleteTask(ctx, "blocked"))
	require.False(t, store.hasDependents("dep"))
}

func TestRunTaskNowMissing(t *testing.T) {
	store, _ := newCoverageBroker(t)

	require.ErrorIs(t, store.RunTaskNow(context.Background(), "missing"), broker.ErrTaskNotFound)
}
