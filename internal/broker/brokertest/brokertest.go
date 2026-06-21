// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

// Package brokertest is the conformance suite every Broker implementation
// must pass. It drives all time-dependent behavior through a fake clock —
// no sleeps — so leases, uniqueness TTLs, and retention are tested
// identically and deterministically on every implementation.
package brokertest

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/conveyorq/conveyor/internal/broker"
	"github.com/conveyorq/conveyor/internal/clock"
	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
)

// Factory builds a fresh, empty broker reading time from the given clock.
// Implementations register cleanup with t.Cleanup.
type Factory func(t *testing.T, timeSource clock.Clock) broker.Broker

// start is the frozen instant every test begins at.
var start = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// Default lease and batch parameters used throughout the suite.
const (
	leaseTTL   = time.Minute
	batchLimit = 100
	queueName  = "default"
)

// Run executes the full conformance suite against the factory.
func Run(t *testing.T, factory Factory) {
	t.Helper()

	suite := []struct {
		name string
		test func(*testing.T, broker.Broker, *clock.Fake)
	}{
		{"EnqueueAndGetTask", testEnqueueAndGetTask},
		{"EnqueueScheduledWhenFuture", testEnqueueScheduledWhenFuture},
		{"EnqueueIdempotentOnID", testEnqueueIdempotentOnID},
		{"GetTaskNotFound", testGetTaskNotFound},
		{"LeaseOrdersByPriorityThenFIFO", testLeaseOrdersByPriorityThenFIFO},
		{"LeaseHonorsLimitAndClaims", testLeaseHonorsLimitAndClaims},
		{"LeaseScopesToQueueAndDueTime", testLeaseScopesToQueueAndDueTime},
		{"AckCompletesTask", testAckCompletesTask},
		{"ExecutionTimestamps", testExecutionTimestamps},
		{"FailSchedulesRetry", testFailSchedulesRetry},
		{"ReleaseReturnsToPendingWithoutPenalty", testReleaseReturnsToPendingWithoutPenalty},
		{"ArchiveDeadLetters", testArchiveDeadLetters},
		{"ExtendLeaseDefersReaping", testExtendLeaseDefersReaping},
		{"ReapExpiredLeases", testReapExpiredLeases},
		{"ReapArchivesExhaustedRetries", testReapArchivesExhaustedRetries},
		{"PromoteScheduled", testPromoteScheduled},
		{"PurgeCompletedHonorsRetention", testPurgeCompletedHonorsRetention},
		{"ExpiredTaskNotLeasedAndArchived", testExpiredTaskNotLeasedAndArchived},
		{"PendingCount", testPendingCount},
		{"UniqueTasks", testUniqueTasks},
		{"UniqueKeyTTLLapses", testUniqueKeyTTLLapses},
		{"CancelTask", testCancelTask},
		{"DeleteTask", testDeleteTask},
		{"RunTaskNow", testRunTaskNow},
		{"ArchiveTask", testArchiveTask},
		{"QueuePauseFlag", testQueuePauseFlag},
		{"QueueRateLimit", testQueueRateLimit},
		{"QueueConcurrencyLimit", testQueueConcurrencyLimit},
		{"QueueStats", testQueueStats},
		{"CronEntries", testCronEntries},
		{"ListTasks", testListTasks},
		{"Info", testInfo},
		{"ConcurrentLeaseNoDoubleDelivery", testConcurrentLeaseNoDoubleDelivery},
		{"GroupedEnqueueAggregates", testGroupedEnqueueAggregates},
		{"GroupedScheduleRejected", testGroupedScheduleRejected},
		{"GroupStatsReportsMembers", testGroupStatsReportsMembers},
		{"LeaseGroupClaimsBatch", testLeaseGroupClaimsBatch},
		{"DependencyBlocksUntilResolved", testDependencyBlocksUntilResolved},
		{"DependencyAlreadyCompletedAtEnqueue", testDependencyAlreadyCompletedAtEnqueue},
		{"FanInWaitsForAllDependencies", testFanInWaitsForAllDependencies},
		{"DependencyFailurePolicyBlock", testDependencyFailurePolicyBlock},
		{"DependencyFailurePolicyContinue", testDependencyFailurePolicyContinue},
		{"DependencyFailureCascadeCancels", testDependencyFailureCascadeCancels},
		{"PromoteReadyDependentsSafetyNet", testPromoteReadyDependentsSafetyNet},
		{"ConcurrentFanInResolves", testConcurrentFanInResolves},
		{"DependencyCycleStaysBlocked", testDependencyCycleStaysBlocked},
	}

	for _, entry := range suite {
		t.Run(entry.name, func(t *testing.T) {
			fake := clock.NewFake(start)
			entry.test(t, factory(t, fake), fake)
		})
	}
}

// taskOption mutates a test envelope before enqueue.
type taskOption func(*conveyorv1.TaskEnvelope)

// withPriority sets the dispatch priority.
func withPriority(priority int32) taskOption {
	return func(task *conveyorv1.TaskEnvelope) {
		task.Options.Priority = priority
	}
}

// withProcessAt delays the task until the given time.
func withProcessAt(processAt time.Time) taskOption {
	return func(task *conveyorv1.TaskEnvelope) {
		task.Options.ProcessAt = timestamppb.New(processAt)
	}
}

// withExpiresAt sets the pre-dispatch expiry: a task still waiting past this
// time is archived instead of leased.
func withExpiresAt(expiresAt time.Time) taskOption {
	return func(task *conveyorv1.TaskEnvelope) {
		task.Options.ExpiresAt = timestamppb.New(expiresAt)
	}
}

// withUnique claims a uniqueness key; a zero ttl claims until completion.
func withUnique(key string, ttl time.Duration) taskOption {
	return func(task *conveyorv1.TaskEnvelope) {
		task.Options.UniqueKey = key

		if ttl > 0 {
			task.Options.UniqueTtl = durationpb.New(ttl)
		}
	}
}

// withRetention keeps the completed task row for the given duration.
func withRetention(retention time.Duration) taskOption {
	return func(task *conveyorv1.TaskEnvelope) {
		task.Options.Retention = durationpb.New(retention)
	}
}

// withMaxRetry caps the retry counter.
func withMaxRetry(maxRetry int32) taskOption {
	return func(task *conveyorv1.TaskEnvelope) {
		task.Options.MaxRetry = maxRetry
	}
}

// withQueue overrides the queue.
func withQueue(queue string) taskOption {
	return func(task *conveyorv1.TaskEnvelope) {
		task.Queue = queue
	}
}

// withGroup makes the task a member of the named aggregation group.
func withGroup(group string) taskOption {
	return func(task *conveyorv1.TaskEnvelope) {
		task.Options.Group = group
	}
}

// withEnqueuedAt overrides the committed enqueue time, ordering group members
// and setting the group's oldest/newest thresholds.
func withEnqueuedAt(at time.Time) taskOption {
	return func(task *conveyorv1.TaskEnvelope) {
		task.EnqueuedAt = timestamppb.New(at)
	}
}

// withDependsOn declares the tasks this task waits for; until each reaches a
// terminal success the task stays blocked.
func withDependsOn(deps ...*conveyorv1.TaskDependency) taskOption {
	return func(task *conveyorv1.TaskEnvelope) {
		task.Options.DependsOn = deps
	}
}

// dependsOn builds a dependency edge carrying the block-on-failure default.
func dependsOn(taskID string) *conveyorv1.TaskDependency {
	return &conveyorv1.TaskDependency{TaskId: taskID}
}

// dependsOnFailing builds a dependency edge with an explicit failure policy.
func dependsOnFailing(taskID string, policy conveyorv1.DependencyFailurePolicy) *conveyorv1.TaskDependency {
	return &conveyorv1.TaskDependency{TaskId: taskID, OnFailure: policy}
}

// newTask builds a minimal valid envelope; ids must sort lexicographically
// in creation order, so callers use zero-padded sequence numbers.
func newTask(id string, options ...taskOption) *conveyorv1.TaskEnvelope {
	task := &conveyorv1.TaskEnvelope{
		Id:          id,
		Queue:       queueName,
		Type:        "test:task",
		Payload:     []byte(`{"n":1}`),
		ContentType: "application/json",
		Metadata:    map[string]string{"origin": "brokertest"},
		Options:     &conveyorv1.TaskOptions{MaxRetry: 25},
		EnqueuedAt:  timestamppb.New(start),
	}

	for _, option := range options {
		option(task)
	}

	return task
}

// mustEnqueue enqueues or fails the test.
func mustEnqueue(t *testing.T, b broker.Broker, task *conveyorv1.TaskEnvelope) {
	t.Helper()

	if err := b.Enqueue(context.Background(), task); err != nil {
		t.Fatalf("Enqueue(%s): %v", task.GetId(), err)
	}
}

// mustLease leases or fails the test.
func mustLease(t *testing.T, b broker.Broker, queue string, limit int, leaseID string) []*conveyorv1.TaskEnvelope {
	t.Helper()

	leased, err := b.Lease(context.Background(), queue, limit, leaseTTL, leaseID)
	if err != nil {
		t.Fatalf("Lease(%s): %v", leaseID, err)
	}

	return leased
}

// leasedIDs projects the ids of leased tasks.
func leasedIDs(tasks []*conveyorv1.TaskEnvelope) []string {
	ids := make([]string, 0, len(tasks))
	for _, task := range tasks {
		ids = append(ids, task.GetId())
	}

	return ids
}

// mustState asserts a task's current state.
func mustState(t *testing.T, b broker.Broker, id string, want conveyorv1.TaskState) {
	t.Helper()

	_, state, err := b.GetTask(context.Background(), id)
	if err != nil {
		t.Fatalf("GetTask(%s): %v", id, err)
	}

	if state != want {
		t.Fatalf("task %s state = %s, want %s", id, state, want)
	}
}

func testEnqueueAndGetTask(t *testing.T, b broker.Broker, _ *clock.Fake) {
	original := newTask("task-001")
	mustEnqueue(t, b, original)

	stored, state, err := b.GetTask(context.Background(), "task-001")
	if err != nil {
		t.Fatal(err)
	}

	if state != conveyorv1.TaskState_TASK_STATE_PENDING {
		t.Fatalf("state = %s, want PENDING", state)
	}

	if stored.GetType() != original.GetType() || stored.GetQueue() != original.GetQueue() {
		t.Fatalf("envelope identity not round-tripped: %v", stored)
	}

	if string(stored.GetPayload()) != string(original.GetPayload()) {
		t.Fatalf("payload = %q, want %q", stored.GetPayload(), original.GetPayload())
	}

	if stored.GetContentType() != original.GetContentType() {
		t.Fatalf("content type = %q, want %q", stored.GetContentType(), original.GetContentType())
	}

	if stored.GetMetadata()["origin"] != "brokertest" {
		t.Fatalf("metadata not round-tripped: %v", stored.GetMetadata())
	}
}

func testEnqueueScheduledWhenFuture(t *testing.T, b broker.Broker, fake *clock.Fake) {
	mustEnqueue(t, b, newTask("task-001", withProcessAt(start.Add(time.Hour))))
	mustState(t, b, "task-001", conveyorv1.TaskState_TASK_STATE_SCHEDULED)

	if leased := mustLease(t, b, queueName, batchLimit, "lease-1"); len(leased) != 0 {
		t.Fatalf("scheduled task leased early: %v", leasedIDs(leased))
	}

	fake.Advance(time.Hour)

	queues, err := b.PromoteScheduled(context.Background(), batchLimit)
	if err != nil {
		t.Fatal(err)
	}

	if !slices.Contains(queues, queueName) {
		t.Fatalf("PromoteScheduled queues = %v, want to contain %q", queues, queueName)
	}

	mustState(t, b, "task-001", conveyorv1.TaskState_TASK_STATE_PENDING)
}

func testEnqueueIdempotentOnID(t *testing.T, b broker.Broker, _ *clock.Fake) {
	mustEnqueue(t, b, newTask("task-001"))
	mustEnqueue(t, b, newTask("task-001"))

	tasks, err := b.ListTasks(context.Background(), broker.TaskQuery{})
	if err != nil {
		t.Fatal(err)
	}

	if len(tasks) != 1 {
		t.Fatalf("duplicate id produced %d rows, want 1", len(tasks))
	}
}

func testGetTaskNotFound(t *testing.T, b broker.Broker, _ *clock.Fake) {
	if _, _, err := b.GetTask(context.Background(), "absent"); !errors.Is(err, broker.ErrTaskNotFound) {
		t.Fatalf("err = %v, want ErrTaskNotFound", err)
	}
}

func testLeaseOrdersByPriorityThenFIFO(t *testing.T, b broker.Broker, _ *clock.Fake) {
	mustEnqueue(t, b, newTask("task-001", withPriority(4)))
	mustEnqueue(t, b, newTask("task-002", withPriority(7)))
	mustEnqueue(t, b, newTask("task-003", withPriority(4)))

	got := leasedIDs(mustLease(t, b, queueName, batchLimit, "lease-1"))
	want := []string{"task-002", "task-001", "task-003"}

	if !slices.Equal(got, want) {
		t.Fatalf("lease order = %v, want %v", got, want)
	}
}

func testLeaseHonorsLimitAndClaims(t *testing.T, b broker.Broker, _ *clock.Fake) {
	mustEnqueue(t, b, newTask("task-001"))
	mustEnqueue(t, b, newTask("task-002"))

	first := mustLease(t, b, queueName, 1, "lease-1")
	second := mustLease(t, b, queueName, 1, "lease-2")

	if len(first) != 1 || len(second) != 1 {
		t.Fatalf("lease sizes = %d, %d, want 1, 1", len(first), len(second))
	}

	if first[0].GetId() == second[0].GetId() {
		t.Fatalf("task %s leased twice", first[0].GetId())
	}

	if third := mustLease(t, b, queueName, 1, "lease-3"); len(third) != 0 {
		t.Fatalf("third lease claimed %v, want nothing", leasedIDs(third))
	}
}

func testLeaseScopesToQueueAndDueTime(t *testing.T, b broker.Broker, _ *clock.Fake) {
	mustEnqueue(t, b, newTask("task-001", withQueue("other")))
	mustEnqueue(t, b, newTask("task-002", withProcessAt(start.Add(time.Minute))))

	if leased := mustLease(t, b, queueName, batchLimit, "lease-1"); len(leased) != 0 {
		t.Fatalf("leased %v, want nothing from %q", leasedIDs(leased), queueName)
	}

	if leased := mustLease(t, b, "other", batchLimit, "lease-2"); len(leased) != 1 {
		t.Fatalf("leased %d from other queue, want 1", len(leased))
	}
}

func testAckCompletesTask(t *testing.T, b broker.Broker, _ *clock.Fake) {
	mustEnqueue(t, b, newTask("task-001", withRetention(time.Hour)))
	mustLease(t, b, queueName, 1, "lease-1")

	if err := b.Ack(context.Background(), "task-001", "wrong-lease", nil); !errors.Is(err, broker.ErrLeaseLost) {
		t.Fatalf("Ack with wrong lease: err = %v, want ErrLeaseLost", err)
	}

	if err := b.Ack(context.Background(), "task-001", "lease-1", []byte("done")); err != nil {
		t.Fatal(err)
	}

	mustState(t, b, "task-001", conveyorv1.TaskState_TASK_STATE_COMPLETED)

	if err := b.Ack(context.Background(), "task-001", "lease-1", nil); !errors.Is(err, broker.ErrLeaseLost) {
		t.Fatalf("double Ack: err = %v, want ErrLeaseLost", err)
	}
}

// testExecutionTimestamps verifies that the broker overlays started_at at
// lease time and completed_at at terminal time, so the admin surface can
// report a finished task's execution duration.
func testExecutionTimestamps(t *testing.T, b broker.Broker, fake *clock.Fake) {
	mustEnqueue(t, b, newTask("task-001", withRetention(time.Hour)))

	enqueued, _, err := b.GetTask(context.Background(), "task-001")
	if err != nil {
		t.Fatal(err)
	}

	if enqueued.GetStartedAt() != nil || enqueued.GetCompletedAt() != nil {
		t.Fatalf("before dispatch: started_at = %v, completed_at = %v; want both unset",
			enqueued.GetStartedAt().AsTime(), enqueued.GetCompletedAt().AsTime())
	}

	mustLease(t, b, queueName, 1, "lease-1")

	leased, _, err := b.GetTask(context.Background(), "task-001")
	if err != nil {
		t.Fatal(err)
	}

	if got := leased.GetStartedAt(); got == nil || !got.AsTime().Equal(start) {
		t.Fatalf("after lease: started_at = %v, want %v", got.AsTime(), start)
	}

	if leased.GetCompletedAt() != nil {
		t.Fatalf("after lease: completed_at = %v, want unset", leased.GetCompletedAt().AsTime())
	}

	fake.Advance(3 * time.Second)
	finishedAt := start.Add(3 * time.Second)

	if err := b.Ack(context.Background(), "task-001", "lease-1", []byte("done")); err != nil {
		t.Fatal(err)
	}

	completed, _, err := b.GetTask(context.Background(), "task-001")
	if err != nil {
		t.Fatal(err)
	}

	if got := completed.GetStartedAt(); got == nil || !got.AsTime().Equal(start) {
		t.Fatalf("after ack: started_at = %v, want %v", got.AsTime(), start)
	}

	if got := completed.GetCompletedAt(); got == nil || !got.AsTime().Equal(finishedAt) {
		t.Fatalf("after ack: completed_at = %v, want %v", got.AsTime(), finishedAt)
	}
}

func testFailSchedulesRetry(t *testing.T, b broker.Broker, fake *clock.Fake) {
	mustEnqueue(t, b, newTask("task-001"))
	mustLease(t, b, queueName, 1, "lease-1")

	retryAt := start.Add(5 * time.Minute)
	if err := b.Fail(context.Background(), "task-001", "lease-1", "boom", retryAt); err != nil {
		t.Fatal(err)
	}

	mustState(t, b, "task-001", conveyorv1.TaskState_TASK_STATE_RETRY)

	if leased := mustLease(t, b, queueName, batchLimit, "lease-2"); len(leased) != 0 {
		t.Fatalf("retry leased before backoff elapsed: %v", leasedIDs(leased))
	}

	fake.Advance(5 * time.Minute)

	leased := mustLease(t, b, queueName, batchLimit, "lease-3")
	if len(leased) != 1 {
		t.Fatalf("leased %d after backoff, want 1", len(leased))
	}

	if leased[0].GetRetried() != 1 || leased[0].GetLastError() != "boom" {
		t.Fatalf("retried = %d, lastError = %q; want 1, boom", leased[0].GetRetried(), leased[0].GetLastError())
	}
}

func testReleaseReturnsToPendingWithoutPenalty(t *testing.T, b broker.Broker, _ *clock.Fake) {
	mustEnqueue(t, b, newTask("task-001"))
	mustLease(t, b, queueName, 1, "lease-1")

	if err := b.Release(context.Background(), "task-001", "lease-1"); err != nil {
		t.Fatal(err)
	}

	mustState(t, b, "task-001", conveyorv1.TaskState_TASK_STATE_PENDING)

	leased := mustLease(t, b, queueName, 1, "lease-2")
	if len(leased) != 1 || leased[0].GetRetried() != 0 {
		t.Fatalf("release must redeliver immediately with retried = 0, got %v", leased)
	}
}

func testArchiveDeadLetters(t *testing.T, b broker.Broker, _ *clock.Fake) {
	mustEnqueue(t, b, newTask("task-001"))
	mustLease(t, b, queueName, 1, "lease-1")

	if err := b.Archive(context.Background(), "task-001", "wrong-lease", "dead"); !errors.Is(err, broker.ErrLeaseLost) {
		t.Fatalf("Archive with wrong lease: err = %v, want ErrLeaseLost", err)
	}

	if err := b.Archive(context.Background(), "task-001", "lease-1", "dead"); err != nil {
		t.Fatal(err)
	}

	mustState(t, b, "task-001", conveyorv1.TaskState_TASK_STATE_ARCHIVED)

	// The lease-less path archives any non-completed task.
	mustEnqueue(t, b, newTask("task-002"))

	if err := b.Archive(context.Background(), "task-002", "", "admin"); err != nil {
		t.Fatal(err)
	}

	mustState(t, b, "task-002", conveyorv1.TaskState_TASK_STATE_ARCHIVED)

	// Completed tasks are not archivable.
	mustEnqueue(t, b, newTask("task-003", withRetention(time.Hour)))
	mustLease(t, b, queueName, 1, "lease-2")

	if err := b.Ack(context.Background(), "task-003", "lease-2", nil); err != nil {
		t.Fatal(err)
	}

	if err := b.Archive(context.Background(), "task-003", "", "late"); !errors.Is(err, broker.ErrInvalidState) {
		t.Fatalf("Archive completed: err = %v, want ErrInvalidState", err)
	}
}

func testExtendLeaseDefersReaping(t *testing.T, b broker.Broker, fake *clock.Fake) {
	mustEnqueue(t, b, newTask("task-001"))
	mustLease(t, b, queueName, 1, "lease-1")

	if err := b.ExtendLease(context.Background(), "task-001", "wrong-lease", leaseTTL); !errors.Is(err, broker.ErrLeaseLost) {
		t.Fatalf("ExtendLease with wrong lease: err = %v, want ErrLeaseLost", err)
	}

	fake.Advance(40 * time.Second)

	if err := b.ExtendLease(context.Background(), "task-001", "lease-1", leaseTTL); err != nil {
		t.Fatal(err)
	}

	// 80s after lease: past the original expiry, inside the extension.
	fake.Advance(40 * time.Second)

	queues, err := b.ReapExpiredLeases(context.Background(), batchLimit)
	if err != nil {
		t.Fatal(err)
	}

	if len(queues) != 0 {
		t.Fatalf("extended lease reaped early: %v", queues)
	}

	mustState(t, b, "task-001", conveyorv1.TaskState_TASK_STATE_ACTIVE)
}

func testReapExpiredLeases(t *testing.T, b broker.Broker, fake *clock.Fake) {
	mustEnqueue(t, b, newTask("task-001"))
	mustLease(t, b, queueName, 1, "lease-1")

	fake.Advance(leaseTTL + time.Second)

	queues, err := b.ReapExpiredLeases(context.Background(), batchLimit)
	if err != nil {
		t.Fatal(err)
	}

	if !slices.Contains(queues, queueName) {
		t.Fatalf("reap queues = %v, want to contain %q", queues, queueName)
	}

	mustState(t, b, "task-001", conveyorv1.TaskState_TASK_STATE_RETRY)

	leased := mustLease(t, b, queueName, 1, "lease-2")
	if len(leased) != 1 || leased[0].GetRetried() != 1 {
		t.Fatalf("reaped task must redeliver with retried = 1, got %v", leased)
	}

	// The stale lease can no longer act on the task.
	if err := b.Ack(context.Background(), "task-001", "lease-1", nil); !errors.Is(err, broker.ErrLeaseLost) {
		t.Fatalf("stale lease Ack: err = %v, want ErrLeaseLost", err)
	}
}

func testReapArchivesExhaustedRetries(t *testing.T, b broker.Broker, fake *clock.Fake) {
	mustEnqueue(t, b, newTask("task-001", withMaxRetry(1)))
	mustLease(t, b, queueName, 1, "lease-1")

	if err := b.Fail(context.Background(), "task-001", "lease-1", "boom", fake.Now()); err != nil {
		t.Fatal(err)
	}

	mustLease(t, b, queueName, 1, "lease-2")
	fake.Advance(leaseTTL + time.Second)

	if _, err := b.ReapExpiredLeases(context.Background(), batchLimit); err != nil {
		t.Fatal(err)
	}

	mustState(t, b, "task-001", conveyorv1.TaskState_TASK_STATE_ARCHIVED)
}

func testPromoteScheduled(t *testing.T, b broker.Broker, fake *clock.Fake) {
	mustEnqueue(t, b, newTask("task-001", withProcessAt(start.Add(time.Minute))))
	mustEnqueue(t, b, newTask("task-002", withProcessAt(start.Add(time.Hour))))

	fake.Advance(time.Minute)

	if _, err := b.PromoteScheduled(context.Background(), batchLimit); err != nil {
		t.Fatal(err)
	}

	mustState(t, b, "task-001", conveyorv1.TaskState_TASK_STATE_PENDING)
	mustState(t, b, "task-002", conveyorv1.TaskState_TASK_STATE_SCHEDULED)
}

func testPurgeCompletedHonorsRetention(t *testing.T, b broker.Broker, fake *clock.Fake) {
	mustEnqueue(t, b, newTask("task-001"))
	mustEnqueue(t, b, newTask("task-002", withRetention(time.Hour)))

	for _, id := range []string{"task-001", "task-002"} {
		leased := mustLease(t, b, queueName, 1, "lease-"+id)
		if err := b.Ack(context.Background(), leased[0].GetId(), "lease-"+id, nil); err != nil {
			t.Fatal(err)
		}
	}

	purged, err := b.PurgeCompleted(context.Background(), batchLimit)
	if err != nil {
		t.Fatal(err)
	}

	if purged != 1 {
		t.Fatalf("purged %d with zero retention, want 1", purged)
	}

	mustState(t, b, "task-002", conveyorv1.TaskState_TASK_STATE_COMPLETED)
	fake.Advance(time.Hour)

	purged, err = b.PurgeCompleted(context.Background(), batchLimit)
	if err != nil {
		t.Fatal(err)
	}

	if purged != 1 {
		t.Fatalf("purged %d after retention lapsed, want 1", purged)
	}

	if _, _, err := b.GetTask(context.Background(), "task-002"); !errors.Is(err, broker.ErrTaskNotFound) {
		t.Fatalf("purged task still present: %v", err)
	}
}

// testExpiredTaskNotLeasedAndArchived proves the pre-dispatch TTL: a task is
// leasable before its expiry, is never leased after it, and the sweep archives
// the lapsed task rather than running it.
func testExpiredTaskNotLeasedAndArchived(t *testing.T, b broker.Broker, fake *clock.Fake) {
	mustEnqueue(t, b, newTask("task-001", withExpiresAt(start.Add(time.Hour))))

	// Before expiry the task is due and leasable.
	leased := mustLease(t, b, queueName, batchLimit, "lease-early")
	if got := leasedIDs(leased); len(got) != 1 || got[0] != "task-001" {
		t.Fatalf("before expiry leased %v, want [task-001]", got)
	}

	if err := b.Release(context.Background(), "task-001", "lease-early"); err != nil {
		t.Fatal(err)
	}

	// Past the expiry the task must not be leased.
	fake.Advance(2 * time.Hour)

	if leased = mustLease(t, b, queueName, batchLimit, "lease-late"); len(leased) != 0 {
		t.Fatalf("expired task was leased: %v", leasedIDs(leased))
	}

	// The sweep archives the lapsed task instead of running it.
	archived, err := b.ArchiveExpired(context.Background(), batchLimit)
	if err != nil {
		t.Fatal(err)
	}

	if archived != 1 {
		t.Fatalf("archived %d expired tasks, want 1", archived)
	}

	mustState(t, b, "task-001", conveyorv1.TaskState_TASK_STATE_ARCHIVED)

	stored, _, err := b.GetTask(context.Background(), "task-001")
	if err != nil {
		t.Fatal(err)
	}

	if stored.GetLastError() != broker.TaskExpiredMessage {
		t.Fatalf("last error = %q, want %q", stored.GetLastError(), broker.TaskExpiredMessage)
	}
}

func testPendingCount(t *testing.T, b broker.Broker, _ *clock.Fake) {
	mustEnqueue(t, b, newTask("task-001"))
	mustEnqueue(t, b, newTask("task-002"))
	mustEnqueue(t, b, newTask("task-003", withQueue("other")))
	mustEnqueue(t, b, newTask("task-004", withProcessAt(start.Add(time.Hour))))
	mustLease(t, b, "other", 1, "lease-1")

	counts, err := b.PendingCount(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if counts[queueName] != 2 || counts["other"] != 0 {
		t.Fatalf("counts = %v, want default:2 other:0", counts)
	}
}

func testUniqueTasks(t *testing.T, b broker.Broker, _ *clock.Fake) {
	mustEnqueue(t, b, newTask("task-001", withUnique("user:42", 0)))

	err := b.Enqueue(context.Background(), newTask("task-002", withUnique("user:42", 0)))
	if !errors.Is(err, broker.ErrDuplicateTask) {
		t.Fatalf("duplicate unique key: err = %v, want ErrDuplicateTask", err)
	}

	// A different key is unrelated.
	mustEnqueue(t, b, newTask("task-003", withUnique("user:43", 0)))

	// Completion releases the claim.
	mustLease(t, b, queueName, 1, "lease-1")

	if err := b.Ack(context.Background(), "task-001", "lease-1", nil); err != nil {
		t.Fatal(err)
	}

	mustEnqueue(t, b, newTask("task-004", withUnique("user:42", 0)))
}

func testUniqueKeyTTLLapses(t *testing.T, b broker.Broker, fake *clock.Fake) {
	mustEnqueue(t, b, newTask("task-001", withUnique("report:daily", time.Hour), withProcessAt(start.Add(24*time.Hour))))

	err := b.Enqueue(context.Background(), newTask("task-002", withUnique("report:daily", time.Hour)))
	if !errors.Is(err, broker.ErrDuplicateTask) {
		t.Fatalf("duplicate inside TTL: err = %v, want ErrDuplicateTask", err)
	}

	fake.Advance(2 * time.Hour)

	// The claim lapsed even though task-001 is still incomplete.
	mustEnqueue(t, b, newTask("task-002", withUnique("report:daily", time.Hour)))
}

func testCancelTask(t *testing.T, b broker.Broker, _ *clock.Fake) {
	mustEnqueue(t, b, newTask("task-001"))

	if err := b.CancelTask(context.Background(), "task-001"); err != nil {
		t.Fatal(err)
	}

	mustState(t, b, "task-001", conveyorv1.TaskState_TASK_STATE_CANCELED)

	if leased := mustLease(t, b, queueName, batchLimit, "lease-1"); len(leased) != 0 {
		t.Fatalf("canceled task leased: %v", leasedIDs(leased))
	}

	mustEnqueue(t, b, newTask("task-002"))
	mustLease(t, b, queueName, 1, "lease-2")

	if err := b.CancelTask(context.Background(), "task-002"); !errors.Is(err, broker.ErrInvalidState) {
		t.Fatalf("cancel active: err = %v, want ErrInvalidState", err)
	}

	if err := b.CancelTask(context.Background(), "absent"); !errors.Is(err, broker.ErrTaskNotFound) {
		t.Fatalf("cancel absent: err = %v, want ErrTaskNotFound", err)
	}
}

func testDeleteTask(t *testing.T, b broker.Broker, _ *clock.Fake) {
	mustEnqueue(t, b, newTask("task-001"))
	mustEnqueue(t, b, newTask("task-002"))
	mustLease(t, b, queueName, 1, "lease-1")

	// task-001 is active now (lowest id leased first).
	if err := b.DeleteTask(context.Background(), "task-001"); !errors.Is(err, broker.ErrInvalidState) {
		t.Fatalf("delete active: err = %v, want ErrInvalidState", err)
	}

	if err := b.DeleteTask(context.Background(), "task-002"); err != nil {
		t.Fatal(err)
	}

	if _, _, err := b.GetTask(context.Background(), "task-002"); !errors.Is(err, broker.ErrTaskNotFound) {
		t.Fatalf("deleted task still present: %v", err)
	}
}

func testRunTaskNow(t *testing.T, b broker.Broker, _ *clock.Fake) {
	mustEnqueue(t, b, newTask("task-001", withProcessAt(start.Add(time.Hour))))

	if err := b.RunTaskNow(context.Background(), "task-001"); err != nil {
		t.Fatal(err)
	}

	leased := mustLease(t, b, queueName, 1, "lease-1")
	if len(leased) != 1 {
		t.Fatal("RunTaskNow must make the task immediately leasable")
	}

	if err := b.RunTaskNow(context.Background(), "task-001"); !errors.Is(err, broker.ErrInvalidState) {
		t.Fatalf("run-now active: err = %v, want ErrInvalidState", err)
	}
}

// testArchiveTask covers the admin archive of a waiting task and reviving a
// dead-lettered task back into the dispatch path.
func testArchiveTask(t *testing.T, b broker.Broker, _ *clock.Fake) {
	mustEnqueue(t, b, newTask("task-001"))

	if err := b.ArchiveTask(context.Background(), "task-001"); err != nil {
		t.Fatal(err)
	}

	mustState(t, b, "task-001", conveyorv1.TaskState_TASK_STATE_ARCHIVED)

	if err := b.ArchiveTask(context.Background(), "task-001"); !errors.Is(err, broker.ErrInvalidState) {
		t.Fatalf("archive already-archived: err = %v, want ErrInvalidState", err)
	}

	if err := b.ArchiveTask(context.Background(), "absent"); !errors.Is(err, broker.ErrTaskNotFound) {
		t.Fatalf("archive absent: err = %v, want ErrTaskNotFound", err)
	}

	// Reviving an archived task makes it leasable again.
	if err := b.RunTaskNow(context.Background(), "task-001"); err != nil {
		t.Fatalf("run-now archived: %v", err)
	}

	mustState(t, b, "task-001", conveyorv1.TaskState_TASK_STATE_PENDING)

	if leased := mustLease(t, b, queueName, 1, "lease-1"); len(leased) != 1 {
		t.Fatal("revived archived task must be leasable")
	}
}

func testQueuePauseFlag(t *testing.T, b broker.Broker, _ *clock.Fake) {
	paused, err := b.QueuePaused(context.Background(), queueName)
	if err != nil || paused {
		t.Fatalf("unknown queue paused = %v, %v; want false, nil", paused, err)
	}

	if err := b.SetQueuePaused(context.Background(), queueName, true); err != nil {
		t.Fatal(err)
	}

	if paused, _ = b.QueuePaused(context.Background(), queueName); !paused {
		t.Fatal("pause flag not persisted")
	}

	if err := b.SetQueuePaused(context.Background(), queueName, false); err != nil {
		t.Fatal(err)
	}

	if paused, _ = b.QueuePaused(context.Background(), queueName); paused {
		t.Fatal("resume not persisted")
	}
}

func testQueueRateLimit(t *testing.T, b broker.Broker, _ *clock.Fake) {
	ctx := context.Background()

	if _, ok, err := b.QueueRateLimit(ctx, queueName); err != nil || ok {
		t.Fatalf("unset queue rate limit = ok %v, err %v; want false, nil", ok, err)
	}

	if err := b.SetQueueRateLimit(ctx, queueName, 50, 10); err != nil {
		t.Fatal(err)
	}

	limit, ok, err := b.QueueRateLimit(ctx, queueName)
	if err != nil || !ok {
		t.Fatalf("rate limit not persisted: ok %v, err %v", ok, err)
	}

	if limit.RatePerSec != 50 || limit.Burst != 10 || limit.Queue != queueName {
		t.Fatalf("rate limit = %+v; want {%s 50 10}", limit, queueName)
	}

	// Overwrite replaces in place.
	if err := b.SetQueueRateLimit(ctx, queueName, 100, 20); err != nil {
		t.Fatal(err)
	}

	if limit, _, _ = b.QueueRateLimit(ctx, queueName); limit.RatePerSec != 100 || limit.Burst != 20 {
		t.Fatalf("overwrite not applied: %+v", limit)
	}

	// A second queue, then list is ordered by queue name.
	if err := b.SetQueueRateLimit(ctx, "alpha", 5, 1); err != nil {
		t.Fatal(err)
	}

	all, err := b.QueueRateLimits(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if len(all) != 2 || all[0].Queue != "alpha" || all[1].Queue != queueName {
		t.Fatalf("list = %+v; want [alpha %s] ordered", all, queueName)
	}

	// Delete reverts to the default (no override).
	if err := b.DeleteQueueRateLimit(ctx, queueName); err != nil {
		t.Fatal(err)
	}

	if _, ok, _ := b.QueueRateLimit(ctx, queueName); ok {
		t.Fatal("delete did not remove the override")
	}

	// Deleting a missing override is not an error.
	if err := b.DeleteQueueRateLimit(ctx, "nonexistent"); err != nil {
		t.Fatalf("delete of missing override: %v", err)
	}
}

func testQueueStats(t *testing.T, b broker.Broker, _ *clock.Fake) {
	ctx := context.Background()

	// Drive one task of the default queue into each lifecycle state.
	mustEnqueue(t, b, newTask("task-001"))
	mustLease(t, b, queueName, 1, "lease-1")

	if err := b.Ack(ctx, "task-001", "lease-1", nil); err != nil {
		t.Fatal(err)
	}

	mustEnqueue(t, b, newTask("task-002"))
	mustLease(t, b, queueName, 1, "lease-2")

	if err := b.Fail(ctx, "task-002", "lease-2", "boom", start.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}

	mustEnqueue(t, b, newTask("task-003"))
	mustLease(t, b, queueName, 1, "lease-3")

	if err := b.Archive(ctx, "task-003", "lease-3", "dead"); err != nil {
		t.Fatal(err)
	}

	mustEnqueue(t, b, newTask("task-004"))
	mustLease(t, b, queueName, 1, "lease-4")
	mustEnqueue(t, b, newTask("task-005"))
	mustEnqueue(t, b, newTask("task-006", withProcessAt(start.Add(time.Hour))))
	mustEnqueue(t, b, newTask("task-007", withQueue("other")))

	// A paused queue with no tasks must still appear.
	if err := b.SetQueuePaused(ctx, "idle", true); err != nil {
		t.Fatal(err)
	}

	if err := b.SetQueuePaused(ctx, "other", true); err != nil {
		t.Fatal(err)
	}

	stats, err := b.QueueStats(ctx)
	if err != nil {
		t.Fatal(err)
	}

	want := []broker.QueueStat{
		{Queue: queueName, Scheduled: 1, Pending: 1, Active: 1, Retry: 1, Completed: 1, Archived: 1},
		{Queue: "idle", Paused: true},
		{Queue: "other", Paused: true, Pending: 1},
	}

	if !slices.Equal(stats, want) {
		t.Fatalf("QueueStats = %+v, want %+v", stats, want)
	}
}

func testCronEntries(t *testing.T, b broker.Broker, _ *clock.Fake) {
	entry := &broker.CronEntry{
		ID:          "cron-b",
		Spec:        "0 * * * * *",
		TaskType:    "report:hourly",
		Queue:       queueName,
		Payload:     []byte(`{}`),
		ContentType: "application/json",
		Options:     &conveyorv1.TaskOptions{MaxRetry: 3, Priority: 7},
	}

	if err := b.UpsertCronEntry(context.Background(), entry); err != nil {
		t.Fatal(err)
	}

	if err := b.UpsertCronEntry(context.Background(), &broker.CronEntry{ID: "cron-a", Spec: "0 0 * * * *", TaskType: "report:daily", Queue: queueName}); err != nil {
		t.Fatal(err)
	}

	entries, err := b.ListCronEntries(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if len(entries) != 2 || entries[0].ID != "cron-a" || entries[1].ID != "cron-b" {
		t.Fatalf("entries = %v, want cron-a then cron-b", entries)
	}

	if entries[1].Options.GetPriority() != 7 {
		t.Fatalf("options not round-tripped: %v", entries[1].Options)
	}

	if err := b.SetCronPaused(context.Background(), "cron-b", true); err != nil {
		t.Fatal(err)
	}

	entries, _ = b.ListCronEntries(context.Background())
	if !entries[1].Paused {
		t.Fatal("pause flag not persisted")
	}

	if err := b.SetCronPaused(context.Background(), "absent", true); !errors.Is(err, broker.ErrTaskNotFound) {
		t.Fatalf("pause absent entry: err = %v, want ErrTaskNotFound", err)
	}

	// The next-run cursor is a compare-and-set: it advances only from the
	// expected prior value, so a stale writer never moves it backward. cron-b
	// starts unarmed (zero cursor).
	nextRun := time.Date(2026, 6, 14, 12, 0, 0, 0, time.UTC)
	if err := b.UpdateCronNextRun(context.Background(), "cron-b", time.Time{}, nextRun); err != nil {
		t.Fatal(err)
	}

	entries, _ = b.ListCronEntries(context.Background())
	if !entries[1].NextRunAt.Equal(nextRun) {
		t.Fatalf("next run = %v, want %v", entries[1].NextRunAt, nextRun)
	}

	// A mismatched expected is a no-op: the cursor is nextRun, not zero.
	if err := b.UpdateCronNextRun(context.Background(), "cron-b", time.Time{}, nextRun.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}

	entries, _ = b.ListCronEntries(context.Background())
	if !entries[1].NextRunAt.Equal(nextRun) {
		t.Fatalf("mismatched compare-and-set must not move the cursor, got %v", entries[1].NextRunAt)
	}

	// A matching expected advances the cursor.
	later := nextRun.Add(time.Minute)
	if err := b.UpdateCronNextRun(context.Background(), "cron-b", nextRun, later); err != nil {
		t.Fatal(err)
	}

	entries, _ = b.ListCronEntries(context.Background())
	if !entries[1].NextRunAt.Equal(later) {
		t.Fatalf("matched compare-and-set must advance, got %v", entries[1].NextRunAt)
	}

	// Re-upserting clears the cursor so the scheduler re-arms from the spec.
	if err := b.UpsertCronEntry(context.Background(), entry); err != nil {
		t.Fatal(err)
	}

	entries, _ = b.ListCronEntries(context.Background())
	if !entries[1].NextRunAt.IsZero() {
		t.Fatalf("re-upsert must clear next run, got %v", entries[1].NextRunAt)
	}

	// Both entries are now unarmed (zero cursor) and unpaused, so both are due.
	now := time.Date(2026, 6, 14, 13, 0, 0, 0, time.UTC)
	if due, err := b.ListDueCronEntries(context.Background(), now); err != nil {
		t.Fatal(err)
	} else if len(due) != 2 {
		t.Fatalf("unarmed entries are due: got %d, want 2", len(due))
	}

	// A paused entry and a future-dated entry are not due.
	if err := b.SetCronPaused(context.Background(), "cron-a", true); err != nil {
		t.Fatal(err)
	}

	if err := b.UpdateCronNextRun(context.Background(), "cron-b", time.Time{}, now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}

	if due, _ := b.ListDueCronEntries(context.Background(), now); len(due) != 0 {
		t.Fatalf("paused and future entries are not due, got %d", len(due))
	}

	if err := b.DeleteCronEntry(context.Background(), "cron-a"); err != nil {
		t.Fatal(err)
	}

	entries, _ = b.ListCronEntries(context.Background())
	if len(entries) != 1 || entries[0].ID != "cron-b" {
		t.Fatalf("entries after delete = %v, want only cron-b", entries)
	}
}

func testListTasks(t *testing.T, b broker.Broker, _ *clock.Fake) {
	for sequence := 1; sequence <= 5; sequence++ {
		mustEnqueue(t, b, newTask(fmt.Sprintf("task-%03d", sequence)))
	}

	mustEnqueue(t, b, newTask("task-006", withQueue("other")))
	mustLease(t, b, queueName, 1, "lease-1")

	// Newest first.
	records, err := b.ListTasks(context.Background(), broker.TaskQuery{Queue: queueName})
	if err != nil {
		t.Fatal(err)
	}

	if len(records) != 5 || records[0].Envelope.GetId() != "task-005" {
		t.Fatalf("list = %v, want 5 tasks newest first", recordIDs(records))
	}

	// Every record carries its current state.
	if records[4].State != conveyorv1.TaskState_TASK_STATE_ACTIVE || records[0].State != conveyorv1.TaskState_TASK_STATE_PENDING {
		t.Fatalf("record states = %v/%v, want active/pending", records[4].State, records[0].State)
	}

	// State filter.
	records, _ = b.ListTasks(context.Background(), broker.TaskQuery{State: conveyorv1.TaskState_TASK_STATE_ACTIVE})
	if len(records) != 1 || records[0].Envelope.GetId() != "task-001" {
		t.Fatalf("active filter = %v, want [task-001]", recordIDs(records))
	}

	// Keyset pagination.
	records, _ = b.ListTasks(context.Background(), broker.TaskQuery{Queue: queueName, Limit: 2})
	records, _ = b.ListTasks(context.Background(), broker.TaskQuery{Queue: queueName, Limit: 2, AfterID: records[1].Envelope.GetId()})

	if len(records) != 2 || records[0].Envelope.GetId() != "task-003" {
		t.Fatalf("page 2 = %v, want [task-003 task-002]", recordIDs(records))
	}
}

// recordIDs projects the ids of listed task records.
func recordIDs(records []broker.TaskRecord) []string {
	ids := make([]string, 0, len(records))
	for _, record := range records {
		ids = append(ids, record.Envelope.GetId())
	}

	return ids
}

// testInfo checks that Info reports a non-empty driver and includes a task
// count that tracks committed work.
func testInfo(t *testing.T, b broker.Broker, _ *clock.Fake) {
	info, err := b.Info(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if info.Driver == "" {
		t.Fatal("Info must report a non-empty driver")
	}

	if _, ok := info.Metrics["tasks"]; !ok {
		t.Fatalf("Info metrics missing tasks count: %v", info.Metrics)
	}

	mustEnqueue(t, b, newTask("task-001"))

	info, err = b.Info(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if info.Metrics["tasks"] != "1" {
		t.Fatalf("Info tasks = %q after one enqueue, want 1", info.Metrics["tasks"])
	}
}

func testConcurrentLeaseNoDoubleDelivery(t *testing.T, b broker.Broker, _ *clock.Fake) {
	const (
		taskCount  = 200
		workers    = 8
		batchSize  = 10
		maxRounds  = taskCount
		leasePerID = "concurrent-lease-%d-%d"
	)

	for sequence := range taskCount {
		mustEnqueue(t, b, newTask(fmt.Sprintf("task-%05d", sequence)))
	}

	var (
		mutex sync.Mutex
		seen  = make(map[string]string, taskCount)
		group sync.WaitGroup
	)

	for worker := range workers {
		group.Add(1)

		go func() {
			defer group.Done()

			for round := range maxRounds {
				leaseID := fmt.Sprintf(leasePerID, worker, round)

				leased, err := b.Lease(context.Background(), queueName, batchSize, leaseTTL, leaseID)
				if err != nil {
					t.Errorf("Lease: %v", err)

					return
				}

				if len(leased) == 0 {
					return
				}

				mutex.Lock()

				for _, task := range leased {
					if previous, duplicated := seen[task.GetId()]; duplicated {
						t.Errorf("task %s leased by both %s and %s", task.GetId(), previous, leaseID)
					}

					seen[task.GetId()] = leaseID
				}

				mutex.Unlock()
			}
		}()
	}

	group.Wait()

	if len(seen) != taskCount {
		t.Fatalf("leased %d distinct tasks, want %d", len(seen), taskCount)
	}
}

// testGroupedEnqueueAggregates verifies a grouped task lands aggregating and
// stays off the singleton lease path, while an ungrouped task is unaffected.
func testGroupedEnqueueAggregates(t *testing.T, b broker.Broker, _ *clock.Fake) {
	mustEnqueue(t, b, newTask("task-001", withGroup("digest")))
	mustEnqueue(t, b, newTask("task-002"))

	mustState(t, b, "task-001", conveyorv1.TaskState_TASK_STATE_AGGREGATING)
	mustState(t, b, "task-002", conveyorv1.TaskState_TASK_STATE_PENDING)

	leased := mustLease(t, b, queueName, 10, "lease-1")
	if ids := leasedIDs(leased); !slices.Equal(ids, []string{"task-002"}) {
		t.Fatalf("Lease returned %v, want only the ungrouped task-002", ids)
	}
}

// testGroupedScheduleRejected verifies group + future process_at is rejected.
func testGroupedScheduleRejected(t *testing.T, b broker.Broker, _ *clock.Fake) {
	err := b.Enqueue(context.Background(),
		newTask("task-001", withGroup("digest"), withProcessAt(start.Add(time.Hour))))
	if !errors.Is(err, broker.ErrGroupedSchedule) {
		t.Fatalf("Enqueue(group+process_at) error = %v, want ErrGroupedSchedule", err)
	}
}

// testGroupStatsReportsMembers verifies GroupStats reports per-group count and
// the oldest/newest member times, scoped to aggregating members.
func testGroupStatsReportsMembers(t *testing.T, b broker.Broker, _ *clock.Fake) {
	late := start.Add(2 * time.Second)
	mid := start.Add(time.Second)

	mustEnqueue(t, b, newTask("task-001", withGroup("a"), withEnqueuedAt(start)))
	mustEnqueue(t, b, newTask("task-002", withGroup("a"), withEnqueuedAt(late)))
	mustEnqueue(t, b, newTask("task-003", withGroup("a"), withEnqueuedAt(mid)))
	mustEnqueue(t, b, newTask("task-004", withGroup("b")))
	mustEnqueue(t, b, newTask("task-005"))

	stats, err := b.GroupStats(context.Background())
	if err != nil {
		t.Fatalf("GroupStats: %v", err)
	}

	if len(stats) != 2 {
		t.Fatalf("GroupStats returned %d groups, want 2 (a, b)", len(stats))
	}

	groupA := stats[0]
	if groupA.Group != "a" || groupA.Count != 3 {
		t.Fatalf("group a = %+v, want group=a count=3", groupA)
	}

	if !groupA.Oldest.Equal(start) || !groupA.Newest.Equal(late) {
		t.Fatalf("group a oldest=%s newest=%s, want %s / %s", groupA.Oldest, groupA.Newest, start, late)
	}
}

// testLeaseGroupClaimsBatch verifies LeaseGroup leases one group's members as a
// batch under a single lease, honors the limit, orders by (enqueued_at, id),
// and leaves other groups and over-limit members aggregating.
func testLeaseGroupClaimsBatch(t *testing.T, b broker.Broker, _ *clock.Fake) {
	for _, id := range []string{"task-001", "task-002", "task-003"} {
		mustEnqueue(t, b, newTask(id, withGroup("a"), withEnqueuedAt(start)))
	}

	mustEnqueue(t, b, newTask("task-004", withGroup("b")))

	leased, err := b.LeaseGroup(context.Background(), queueName, "a", 2, leaseTTL, "batch-1")
	if err != nil {
		t.Fatalf("LeaseGroup: %v", err)
	}

	if ids := leasedIDs(leased); !slices.Equal(ids, []string{"task-001", "task-002"}) {
		t.Fatalf("LeaseGroup returned %v, want [task-001 task-002]", ids)
	}

	mustState(t, b, "task-001", conveyorv1.TaskState_TASK_STATE_ACTIVE)
	mustState(t, b, "task-002", conveyorv1.TaskState_TASK_STATE_ACTIVE)
	mustState(t, b, "task-003", conveyorv1.TaskState_TASK_STATE_AGGREGATING)
	mustState(t, b, "task-004", conveyorv1.TaskState_TASK_STATE_AGGREGATING)

	// The batch shares one lease id: each member acks under it.
	if err := b.Ack(context.Background(), "task-001", "batch-1", nil); err != nil {
		t.Fatalf("Ack member under batch lease: %v", err)
	}
}

// mustAck acks a task under its lease or fails the test.
func mustAck(t *testing.T, b broker.Broker, id, leaseID string) {
	t.Helper()

	if err := b.Ack(context.Background(), id, leaseID, nil); err != nil {
		t.Fatalf("Ack(%s): %v", id, err)
	}
}

// mustResolve reconciles a terminal task's dependents and returns the woken
// queues, or fails the test.
func mustResolve(t *testing.T, b broker.Broker, id string) []string {
	t.Helper()

	queues, err := b.ResolveDependents(context.Background(), id)
	if err != nil {
		t.Fatalf("ResolveDependents(%s): %v", id, err)
	}

	return queues
}

// completeOnly leases the single due task, asserts it is the expected id, acks
// it, then resolves its dependents and returns the woken queues. It is the
// success path for a dependency that has no due siblings.
func completeOnly(t *testing.T, b broker.Broker, id string) []string {
	t.Helper()

	leaseID := "lease-" + id

	leased := mustLease(t, b, queueName, batchLimit, leaseID)
	if ids := leasedIDs(leased); !slices.Equal(ids, []string{id}) {
		t.Fatalf("complete: leased %v, want only [%s]", ids, id)
	}

	mustAck(t, b, id, leaseID)

	return mustResolve(t, b, id)
}

// testDependencyBlocksUntilResolved verifies a task that depends on another is
// held blocked and unleasable until its dependency completes, then promoted.
func testDependencyBlocksUntilResolved(t *testing.T, b broker.Broker, _ *clock.Fake) {
	mustEnqueue(t, b, newTask("task-001"))
	mustEnqueue(t, b, newTask("task-002", withDependsOn(dependsOn("task-001"))))

	mustState(t, b, "task-002", conveyorv1.TaskState_TASK_STATE_BLOCKED)

	queues := completeOnly(t, b, "task-001")
	if !slices.Contains(queues, queueName) {
		t.Fatalf("ResolveDependents woke %v, want to include %q", queues, queueName)
	}

	mustState(t, b, "task-002", conveyorv1.TaskState_TASK_STATE_PENDING)

	leased := mustLease(t, b, queueName, batchLimit, "lease-final")
	if ids := leasedIDs(leased); !slices.Equal(ids, []string{"task-002"}) {
		t.Fatalf("unblocked dependent not leasable: leased %v", ids)
	}
}

// testDependencyAlreadyCompletedAtEnqueue verifies a task whose dependency
// already completed before it was enqueued is immediately eligible, not blocked.
func testDependencyAlreadyCompletedAtEnqueue(t *testing.T, b broker.Broker, _ *clock.Fake) {
	mustEnqueue(t, b, newTask("task-001"))
	completeOnly(t, b, "task-001")

	mustEnqueue(t, b, newTask("task-002", withDependsOn(dependsOn("task-001"))))

	mustState(t, b, "task-002", conveyorv1.TaskState_TASK_STATE_PENDING)
}

// testFanInWaitsForAllDependencies verifies a continuation that depends on a
// fan-out batch stays blocked until every member completes, then unblocks.
func testFanInWaitsForAllDependencies(t *testing.T, b broker.Broker, _ *clock.Fake) {
	mustEnqueue(t, b, newTask("task-001"))
	mustEnqueue(t, b, newTask("task-002"))
	mustEnqueue(t, b, newTask("task-003"))
	mustEnqueue(t, b, newTask("task-004",
		withDependsOn(dependsOn("task-001"), dependsOn("task-002"), dependsOn("task-003"))))

	mustState(t, b, "task-004", conveyorv1.TaskState_TASK_STATE_BLOCKED)

	leased := mustLease(t, b, queueName, batchLimit, "lease-1")
	if ids := leasedIDs(leased); !slices.Equal(ids, []string{"task-001", "task-002", "task-003"}) {
		t.Fatalf("fan-out leased %v, want the three independent members", ids)
	}

	mustAck(t, b, "task-001", "lease-1")
	mustResolve(t, b, "task-001")
	mustState(t, b, "task-004", conveyorv1.TaskState_TASK_STATE_BLOCKED)

	mustAck(t, b, "task-002", "lease-1")
	mustResolve(t, b, "task-002")
	mustState(t, b, "task-004", conveyorv1.TaskState_TASK_STATE_BLOCKED)

	mustAck(t, b, "task-003", "lease-1")
	mustResolve(t, b, "task-003")
	mustState(t, b, "task-004", conveyorv1.TaskState_TASK_STATE_PENDING)
}

// testDependencyFailurePolicyBlock verifies the default policy keeps a dependent
// blocked when its dependency fails terminally.
func testDependencyFailurePolicyBlock(t *testing.T, b broker.Broker, _ *clock.Fake) {
	mustEnqueue(t, b, newTask("task-001"))
	mustEnqueue(t, b, newTask("task-002", withDependsOn(dependsOn("task-001"))))

	mustLease(t, b, queueName, batchLimit, "lease-1")

	if err := b.Archive(context.Background(), "task-001", "lease-1", "boom"); err != nil {
		t.Fatalf("Archive(task-001): %v", err)
	}

	mustResolve(t, b, "task-001")
	mustState(t, b, "task-002", conveyorv1.TaskState_TASK_STATE_BLOCKED)
}

// testDependencyFailurePolicyContinue verifies the continue policy promotes a
// dependent even though its dependency failed terminally.
func testDependencyFailurePolicyContinue(t *testing.T, b broker.Broker, _ *clock.Fake) {
	mustEnqueue(t, b, newTask("task-001"))
	mustEnqueue(t, b, newTask("task-002",
		withDependsOn(dependsOnFailing("task-001", conveyorv1.DependencyFailurePolicy_DEPENDENCY_FAILURE_POLICY_CONTINUE))))

	mustLease(t, b, queueName, batchLimit, "lease-1")

	if err := b.Archive(context.Background(), "task-001", "lease-1", "boom"); err != nil {
		t.Fatalf("Archive(task-001): %v", err)
	}

	mustResolve(t, b, "task-001")
	mustState(t, b, "task-002", conveyorv1.TaskState_TASK_STATE_PENDING)
}

// testDependencyFailureCascadeCancels verifies the cascade-cancel policy cancels
// a dependent on dependency failure and propagates through its own dependents.
func testDependencyFailureCascadeCancels(t *testing.T, b broker.Broker, _ *clock.Fake) {
	cascade := conveyorv1.DependencyFailurePolicy_DEPENDENCY_FAILURE_POLICY_CASCADE_CANCEL

	mustEnqueue(t, b, newTask("task-001"))
	mustEnqueue(t, b, newTask("task-002", withDependsOn(dependsOnFailing("task-001", cascade))))
	mustEnqueue(t, b, newTask("task-003", withDependsOn(dependsOnFailing("task-002", cascade))))

	mustLease(t, b, queueName, batchLimit, "lease-1")

	if err := b.Archive(context.Background(), "task-001", "lease-1", "boom"); err != nil {
		t.Fatalf("Archive(task-001): %v", err)
	}

	mustResolve(t, b, "task-001")
	mustState(t, b, "task-002", conveyorv1.TaskState_TASK_STATE_CANCELED)
	mustState(t, b, "task-003", conveyorv1.TaskState_TASK_STATE_CANCELED)
}

// testPromoteReadyDependentsSafetyNet verifies the reaper sweep promotes a
// dependent whose dependency completed without an inline ResolveDependents.
func testPromoteReadyDependentsSafetyNet(t *testing.T, b broker.Broker, _ *clock.Fake) {
	mustEnqueue(t, b, newTask("task-001"))
	mustEnqueue(t, b, newTask("task-002", withDependsOn(dependsOn("task-001"))))

	mustLease(t, b, queueName, batchLimit, "lease-1")
	mustAck(t, b, "task-001", "lease-1")

	// No inline ResolveDependents: the dependent is still blocked.
	mustState(t, b, "task-002", conveyorv1.TaskState_TASK_STATE_BLOCKED)

	queues, err := b.PromoteReadyDependents(context.Background(), batchLimit)
	if err != nil {
		t.Fatalf("PromoteReadyDependents: %v", err)
	}

	if !slices.Contains(queues, queueName) {
		t.Fatalf("PromoteReadyDependents woke %v, want to include %q", queues, queueName)
	}

	mustState(t, b, "task-002", conveyorv1.TaskState_TASK_STATE_PENDING)
}

// testConcurrentFanInResolves drives the fan-in join under concurrency: many
// sibling dependencies of one continuation complete and resolve at the same
// time. Each resolver deletes only its own edge, so a broker that gates
// promotion on a stale "no edges remain" snapshot would let every resolver miss
// the others' deletes and strand the continuation blocked forever. The join must
// end pending no matter how the resolutions interleave. Run under -race.
func testConcurrentFanInResolves(t *testing.T, b broker.Broker, _ *clock.Fake) {
	const siblings = 8

	deps := make([]*conveyorv1.TaskDependency, siblings)

	for i := range siblings {
		id := fmt.Sprintf("task-%03d", i+1)
		mustEnqueue(t, b, newTask(id))
		deps[i] = dependsOn(id)
	}

	const joinID = "task-099"

	mustEnqueue(t, b, newTask(joinID, withDependsOn(deps...)))
	mustState(t, b, joinID, conveyorv1.TaskState_TASK_STATE_BLOCKED)

	leased := mustLease(t, b, queueName, batchLimit, "lease-1")
	if len(leased) != siblings {
		t.Fatalf("leased %d sibling tasks, want %d", len(leased), siblings)
	}

	var waitGroup sync.WaitGroup

	for i := range siblings {
		id := fmt.Sprintf("task-%03d", i+1)

		waitGroup.Add(1)

		go func() {
			defer waitGroup.Done()

			if err := b.Ack(context.Background(), id, "lease-1", nil); err != nil {
				t.Errorf("Ack(%s): %v", id, err)

				return
			}

			if _, err := b.ResolveDependents(context.Background(), id); err != nil {
				t.Errorf("ResolveDependents(%s): %v", id, err)
			}
		}()
	}

	waitGroup.Wait()

	mustState(t, b, joinID, conveyorv1.TaskState_TASK_STATE_PENDING)
}

// testDependencyCycleStaysBlocked verifies a dependency cycle is handled
// gracefully: two tasks that depend on each other can never satisfy their
// dependency, so both stay blocked, and resolution neither loops nor hangs.
func testDependencyCycleStaysBlocked(t *testing.T, b broker.Broker, _ *clock.Fake) {
	mustEnqueue(t, b, newTask("task-001", withDependsOn(dependsOn("task-002"))))
	mustEnqueue(t, b, newTask("task-002", withDependsOn(dependsOn("task-001"))))

	mustState(t, b, "task-001", conveyorv1.TaskState_TASK_STATE_BLOCKED)
	mustState(t, b, "task-002", conveyorv1.TaskState_TASK_STATE_BLOCKED)

	// Neither dependency is terminal, so an inline resolve and the sweep are
	// both no-ops that must return without looping.
	mustResolve(t, b, "task-001")

	if _, err := b.PromoteReadyDependents(context.Background(), batchLimit); err != nil {
		t.Fatalf("PromoteReadyDependents: %v", err)
	}

	mustState(t, b, "task-001", conveyorv1.TaskState_TASK_STATE_BLOCKED)
	mustState(t, b, "task-002", conveyorv1.TaskState_TASK_STATE_BLOCKED)
}

func testQueueConcurrencyLimit(t *testing.T, b broker.Broker, _ *clock.Fake) {
	ctx := context.Background()

	if _, ok, err := b.QueueConcurrencyLimit(ctx, queueName); err != nil || ok {
		t.Fatalf("unset queue concurrency limit = ok %v, err %v; want false, nil", ok, err)
	}

	if err := b.SetQueueConcurrencyLimit(ctx, queueName, 5); err != nil {
		t.Fatal(err)
	}

	limit, ok, err := b.QueueConcurrencyLimit(ctx, queueName)
	if err != nil || !ok {
		t.Fatalf("concurrency limit not persisted: ok %v, err %v", ok, err)
	}

	if limit.MaxActive != 5 || limit.Queue != queueName {
		t.Fatalf("concurrency limit = %+v; want {%s 5}", limit, queueName)
	}

	// Overwrite replaces in place.
	if err := b.SetQueueConcurrencyLimit(ctx, queueName, 10); err != nil {
		t.Fatal(err)
	}

	if limit, _, _ = b.QueueConcurrencyLimit(ctx, queueName); limit.MaxActive != 10 {
		t.Fatalf("overwrite not applied: %+v", limit)
	}

	// A second queue, then list is ordered by queue name.
	if err := b.SetQueueConcurrencyLimit(ctx, "alpha", 1); err != nil {
		t.Fatal(err)
	}

	all, err := b.QueueConcurrencyLimits(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if len(all) != 2 || all[0].Queue != "alpha" || all[1].Queue != queueName {
		t.Fatalf("list = %+v; want [alpha %s] ordered", all, queueName)
	}

	// Delete leaves the queue's keys unbounded (no limit).
	if err := b.DeleteQueueConcurrencyLimit(ctx, queueName); err != nil {
		t.Fatal(err)
	}

	if _, ok, _ := b.QueueConcurrencyLimit(ctx, queueName); ok {
		t.Fatal("delete did not remove the limit")
	}
}
