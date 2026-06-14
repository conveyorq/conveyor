// MIT License
//
// Copyright (c) 2026 ConveyorQ
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

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
		{"FailSchedulesRetry", testFailSchedulesRetry},
		{"ReleaseReturnsToPendingWithoutPenalty", testReleaseReturnsToPendingWithoutPenalty},
		{"ArchiveDeadLetters", testArchiveDeadLetters},
		{"ExtendLeaseDefersReaping", testExtendLeaseDefersReaping},
		{"ReapExpiredLeases", testReapExpiredLeases},
		{"ReapArchivesExhaustedRetries", testReapArchivesExhaustedRetries},
		{"PromoteScheduled", testPromoteScheduled},
		{"PurgeCompletedHonorsRetention", testPurgeCompletedHonorsRetention},
		{"PendingCount", testPendingCount},
		{"UniqueTasks", testUniqueTasks},
		{"UniqueKeyTTLLapses", testUniqueKeyTTLLapses},
		{"CancelTask", testCancelTask},
		{"DeleteTask", testDeleteTask},
		{"RunTaskNow", testRunTaskNow},
		{"QueuePauseFlag", testQueuePauseFlag},
		{"QueueStats", testQueueStats},
		{"CronEntries", testCronEntries},
		{"ListTasks", testListTasks},
		{"ConcurrentLeaseNoDoubleDelivery", testConcurrentLeaseNoDoubleDelivery},
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
