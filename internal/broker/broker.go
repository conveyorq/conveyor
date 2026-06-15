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

// Package broker defines the durable task log: the only stateful layer of
// the system and the source of truth every other component rebuilds from.
// Actors and API services never touch storage directly; everything goes
// through the Broker interface.
package broker

import (
	"context"
	"errors"
	"time"

	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
)

// Sentinel errors returned by Broker implementations. Match with errors.Is.
var (
	// ErrDuplicateTask is returned by Enqueue when an incomplete task
	// already holds the same unique key.
	ErrDuplicateTask = errors.New("broker: duplicate task")

	// ErrTaskNotFound is returned when the referenced task id is unknown.
	ErrTaskNotFound = errors.New("broker: task not found")

	// ErrLeaseLost is returned when a lease-scoped operation names a task
	// that is no longer held under the given lease id: the lease expired
	// and another delivery owns the task, or the task already completed.
	ErrLeaseLost = errors.New("broker: lease lost")

	// ErrInvalidState is returned when an admin operation is not legal in
	// the task's current state, e.g. canceling an active task.
	ErrInvalidState = errors.New("broker: operation invalid in current task state")
)

// ListTasks pagination bounds.
const (
	// DefaultListLimit applies when TaskQuery.Limit is zero.
	DefaultListLimit = 50
	// MaxListLimit caps TaskQuery.Limit.
	MaxListLimit = 1000
)

// EffectiveListLimit resolves a requested ListTasks limit to the one a
// broker actually applies. Implementations and pagination logic above the
// broker must both use it so "page is full" stays a reliable
// has-more-results signal.
func EffectiveListLimit(limit int) int {
	if limit <= 0 {
		return DefaultListLimit
	}

	if limit > MaxListLimit {
		return MaxListLimit
	}

	return limit
}

// LeaseExpiredMessage is recorded as the task's last error when the reaper
// reclaims an expired lease.
const LeaseExpiredMessage = "lease expired"

// CronEntry is a persisted cron schedule from which the scheduler
// materializes tasks.
type CronEntry struct {
	// ID uniquely identifies the entry.
	ID string
	// Spec is a 6-field cron expression.
	Spec string
	// TaskType is the handler routing key of materialized tasks.
	TaskType string
	// Queue is the queue materialized tasks are enqueued on.
	Queue string
	// Payload is the opaque task payload.
	Payload []byte
	// ContentType describes the payload encoding.
	ContentType string
	// Options are the execution options applied to materialized tasks.
	Options *conveyorv1.TaskOptions
	// Paused suspends materialization without deleting the entry.
	Paused bool
	// NextRunAt is the next time the entry is due to fire. The scheduler owns
	// it: it is zero on a freshly upserted entry (the scheduler computes the
	// first fire time from the spec), then advances after each materialization.
	// Persisting it keeps cron firing correct across singleton failover.
	NextRunAt time.Time
}

// Info reports the storage engine backing a broker for the dashboard's
// broker-info view, the analog of a backing-store health page.
type Info struct {
	// Driver names the storage engine, e.g. "memory" or "postgres".
	Driver string
	// Metrics carries freeform engine statistics (pool counters, row counts,
	// server version) as display-ready key/value strings.
	Metrics map[string]string
}

// QueueStat reports one queue's persisted pause flag and its task counts
// per lifecycle state.
type QueueStat struct {
	// Queue is the queue name.
	Queue string
	// Paused is the persisted pause flag.
	Paused bool
	// Scheduled counts tasks waiting for their process_at.
	Scheduled int64
	// Pending counts tasks due for dispatch.
	Pending int64
	// Active counts tasks executing under a lease.
	Active int64
	// Retry counts tasks awaiting a retry attempt.
	Retry int64
	// Completed counts retained completed tasks.
	Completed int64
	// Archived counts dead-lettered tasks.
	Archived int64
}

// TaskRecord pairs a task envelope with its current lifecycle state in
// ListTasks results.
type TaskRecord struct {
	// Envelope is the stored task with execution fields overlaid.
	Envelope *conveyorv1.TaskEnvelope
	// State is the task's current lifecycle state.
	State conveyorv1.TaskState
}

// TaskQuery filters ListTasks. Zero values mean "no filter".
type TaskQuery struct {
	// Queue restricts results to one queue.
	Queue string
	// State restricts results to one lifecycle state.
	State conveyorv1.TaskState
	// Limit caps the result size; zero applies DefaultListLimit.
	Limit int
	// AfterID returns tasks with ids strictly smaller than this value
	// (keyset pagination; results are ordered by id descending, which is
	// newest-first for ULID ids). Empty starts from the newest task.
	AfterID string
}

// Broker is the durable task log. Implementations must be safe for
// concurrent use and must derive every notion of "now" from the clock they
// were constructed with, never from the system clock or the database clock.
//
// Mutable execution fields (state, retried, last error, lease, timestamps)
// are authoritative in the store; implementations overlay them onto the
// returned TaskEnvelope so callers always observe current values.
type Broker interface {
	// Enqueue durably commits a task. The task lands in the pending state,
	// or scheduled when its process_at lies in the future. Committing the
	// same id again is a no-op (idempotent client retries). If the task
	// carries a unique key, Enqueue returns ErrDuplicateTask while an
	// incomplete task (scheduled, pending, active, or retry) holds the
	// same key and the key's TTL has not lapsed.
	Enqueue(ctx context.Context, task *conveyorv1.TaskEnvelope) error

	// Lease atomically claims up to limit due tasks (pending or retry,
	// process_at reached) from the queue, ordered by priority descending,
	// then process_at, then id. Claimed tasks become active under leaseID
	// until the TTL elapses. Concurrent calls never claim the same task.
	// A non-positive limit claims nothing.
	Lease(ctx context.Context, queue string, limit int, ttl time.Duration, leaseID string) ([]*conveyorv1.TaskEnvelope, error)

	// ExtendLease pushes the lease expiry to now+ttl. It returns
	// ErrLeaseLost when the task is not active under leaseID.
	ExtendLease(ctx context.Context, taskID, leaseID string, ttl time.Duration) error

	// Ack marks an active task completed, releasing its unique key and
	// retaining the row (with the optional result) until its retention
	// lapses. It returns ErrLeaseLost when the task is not active under
	// leaseID.
	Ack(ctx context.Context, taskID, leaseID string, result []byte) error

	// Fail records a failed attempt: state becomes retry, the retry
	// counter increments, errMsg is stored, and the task becomes due again
	// at processAt. It returns ErrLeaseLost when the task is not active
	// under leaseID. Whether to Fail or Archive is the caller's decision;
	// Fail does not inspect max_retry.
	Fail(ctx context.Context, taskID, leaseID, errMsg string, processAt time.Time) error

	// Release returns an active task to pending, due immediately, without
	// incrementing the retry counter (graceful worker disconnect). It
	// returns ErrLeaseLost when the task is not active under leaseID.
	Release(ctx context.Context, taskID, leaseID string) error

	// Archive dead-letters a task: state becomes archived and errMsg is
	// stored. With a non-empty leaseID the task must be active under that
	// lease; with an empty leaseID any non-completed task may be archived
	// (reaper and admin paths).
	Archive(ctx context.Context, taskID, leaseID, errMsg string) error

	// ReapExpiredLeases scans up to limit active tasks whose lease expired
	// and returns them to retry with an incremented retry counter — or
	// archives them when the counter has already reached max_retry, so a
	// perpetually stalling task cannot loop forever. It returns the
	// distinct queues that received work. A non-positive limit reaps
	// nothing.
	ReapExpiredLeases(ctx context.Context, limit int) (queues []string, err error)

	// PromoteScheduled moves up to limit due scheduled tasks to pending
	// and returns the distinct queues that received work. A non-positive
	// limit promotes nothing.
	PromoteScheduled(ctx context.Context, limit int) (queues []string, err error)

	// PurgeCompleted deletes up to limit completed tasks whose retention
	// has lapsed and releases lapsed unique-key claims. It returns the
	// number of rows deleted. A non-positive limit purges nothing.
	PurgeCompleted(ctx context.Context, limit int) (int, error)

	// PendingCount returns, per queue, the number of tasks that are due
	// for dispatch right now (pending or retry with process_at reached).
	PendingCount(ctx context.Context) (map[string]int64, error)

	// QueueStats returns one entry per known queue — any queue holding
	// tasks or a persisted pause flag — ordered by queue name.
	QueueStats(ctx context.Context) ([]QueueStat, error)

	// SetQueuePaused persists the paused flag for a queue.
	SetQueuePaused(ctx context.Context, queue string, paused bool) error

	// QueuePaused reports the persisted paused flag; unknown queues are
	// not paused.
	QueuePaused(ctx context.Context, queue string) (bool, error)

	// Info reports the storage engine's driver and runtime statistics for
	// the dashboard's broker-info view.
	Info(ctx context.Context) (Info, error)

	// GetTask returns the task and its current state, or ErrTaskNotFound.
	GetTask(ctx context.Context, id string) (*conveyorv1.TaskEnvelope, conveyorv1.TaskState, error)

	// ListTasks returns tasks matching the query, ordered by id
	// descending (newest first for ULID ids).
	ListTasks(ctx context.Context, query TaskQuery) ([]TaskRecord, error)

	// CancelTask cancels a scheduled, pending, or retry task. It returns
	// ErrInvalidState in any other state; canceling an executing task is
	// a cooperative concern above the broker.
	CancelTask(ctx context.Context, id string) error

	// DeleteTask removes a task in any state except active, for which it
	// returns ErrInvalidState.
	DeleteTask(ctx context.Context, id string) error

	// RunTaskNow makes a scheduled, pending, retry, or archived task due
	// immediately; re-running an archived task revives it for another
	// attempt. It returns ErrInvalidState in any other state.
	RunTaskNow(ctx context.Context, id string) error

	// ArchiveTask dead-letters a waiting task: a scheduled, pending, or
	// retry task becomes archived. It returns ErrInvalidState in any other
	// state (an active task is dead-lettered through its lease instead) and
	// ErrTaskNotFound when the id is unknown.
	ArchiveTask(ctx context.Context, id string) error

	// UpsertCronEntry creates or replaces a cron entry by id.
	UpsertCronEntry(ctx context.Context, entry *CronEntry) error

	// ListCronEntries returns all cron entries ordered by id.
	ListCronEntries(ctx context.Context) ([]*CronEntry, error)

	// ListDueCronEntries returns the non-paused entries due to fire — those
	// with no next run yet (awaiting their first arming) or a next run at or
	// before now — ordered by id. The scheduler reads only these each tick.
	ListDueCronEntries(ctx context.Context, now time.Time) ([]*CronEntry, error)

	// SetCronPaused persists the paused flag of one entry, or returns
	// ErrTaskNotFound when the entry does not exist.
	SetCronPaused(ctx context.Context, id string, paused bool) error

	// UpdateCronNextRun advances one entry's next fire time, but only when its
	// stored next run still equals expected (a compare-and-set). This keeps a
	// slow or relocating scheduler from moving the cursor backward and
	// re-firing a slot. A mismatch — another scheduler already advanced, or the
	// entry is gone — is a no-op, not an error.
	UpdateCronNextRun(ctx context.Context, id string, expected, next time.Time) error

	// DeleteCronEntry removes an entry; deleting an absent id is a no-op.
	DeleteCronEntry(ctx context.Context, id string) error

	// Close releases the broker's resources.
	Close() error
}
