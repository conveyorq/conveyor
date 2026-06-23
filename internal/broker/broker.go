// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

// Package broker defines the durable task log: the only stateful layer of
// the system and the source of truth every other component rebuilds from.
// Actors and API services never touch storage directly; everything goes
// through the Broker interface.
package broker

import (
	"context"
	"errors"
	"time"

	"github.com/conveyorq/conveyor/internal/events"
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

	// ErrGroupedSchedule is returned by Enqueue when a task carries both a
	// group and a future process_at: aggregation and scheduling are mutually
	// exclusive in v1.
	ErrGroupedSchedule = errors.New("broker: a grouped task cannot be scheduled")
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

// TaskExpiredMessage is recorded as the task's last error when the reaper
// archives a task whose pre-dispatch expiry (expires_at) lapsed before it was
// ever dispatched.
const TaskExpiredMessage = "task expired before dispatch"

// CascadeCanceledMessage is recorded as the task's last error when it is
// canceled because a dependency it declared with the cascade-cancel policy
// failed terminally.
const CascadeCanceledMessage = "canceled by failed dependency"

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
	// Aggregating counts group members accumulating before their group fires.
	Aggregating int64
	// Blocked counts tasks held until their dependencies reach terminal success.
	Blocked int64
}

// GroupStat summarizes one aggregation group's accumulating members. It is the
// firing input the queue's group sweep reads: the count and the oldest/newest
// member timestamps decide whether a size, max-delay, or grace threshold trips.
type GroupStat struct {
	// Queue is the queue the group belongs to.
	Queue string
	// Group is the aggregation group key.
	Group string
	// Type is the members' task type; a group is single-type, so this is the
	// handler routing key the fired batch dispatches to.
	Type string
	// Count is the number of members currently aggregating.
	Count int64
	// Oldest is the enqueue time of the earliest member (drives max-delay).
	Oldest time.Time
	// Newest is the enqueue time of the latest member (drives grace period).
	Newest time.Time
}

// RateLimit is a per-queue dispatch-rate override: the token-bucket parameters
// a queue uses instead of the server's global default. It is config only — the
// live bucket state lives in the dispatching queue grain, not the broker.
type RateLimit struct {
	// Queue is the queue the override applies to.
	Queue string
	// RatePerSec is the sustained dispatch rate in tasks per second (> 0).
	RatePerSec float64
	// Burst is the token-bucket depth: the most tasks dispatchable in an
	// instantaneous burst before the rate applies (>= 1).
	Burst int
	// UpdatedAt is when the override was last written.
	UpdatedAt time.Time
}

// ConcurrencyLimit is a per-queue per-key concurrency cap: the most tasks
// sharing a concurrency key the queue runs at once. It is config only — the live
// in-flight count lives in the dispatching queue grain, not the broker.
type ConcurrencyLimit struct {
	// Queue is the queue the limit applies to.
	Queue string
	// MaxActive is the most tasks sharing a concurrency key that may be active
	// at once (>= 1).
	MaxActive int
	// UpdatedAt is when the limit was last written.
	UpdatedAt time.Time
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

	// LeaseGroup atomically claims up to limit aggregating members of a single
	// (queue, group), ordered by enqueue time then id, leasing them together as
	// one batch: they become active under leaseID until the TTL elapses. It is
	// how a fired group is dispatched — members never pass through pending, so
	// grouped work never reaches the singleton lease path. A non-positive limit
	// claims nothing.
	LeaseGroup(ctx context.Context, queue, group string, limit int, ttl time.Duration, leaseID string) ([]*conveyorv1.TaskEnvelope, error)

	// GroupStats reports one entry per aggregation group, across all queues,
	// that has members accumulating: the member count, the group's task type,
	// and the oldest/newest member enqueue times. It is the firing input for the
	// group sweep and scans only aggregating rows (a partial index), so it stays
	// cheap regardless of total task volume. Groups with no members are omitted.
	GroupStats(ctx context.Context) ([]GroupStat, error)

	// SetProgress records a running task's latest progress (percent 0..100 and
	// an optional message). It is lease-scoped: it matches only a task active
	// under leaseID and returns ErrLeaseLost otherwise, so a stale delivery
	// cannot overwrite a newer one's progress. Progress is advisory and never
	// gates execution.
	SetProgress(ctx context.Context, taskID, leaseID string, percent uint32, message string) error

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

	// ResolveDependents reconciles the dependency edges that point at a task
	// which has just reached a terminal state (completed, archived, or
	// canceled). A completed dependency satisfies its edges; a terminally
	// failed one applies each edge's on-failure policy (block keeps the
	// dependent waiting, continue treats the dependency as satisfied, and
	// cascade-cancel cancels the dependent and, transitively, its own
	// dependents). Every dependent left with no unresolved dependency is
	// promoted to pending (or scheduled, aggregating, when it is also delayed
	// or grouped). It returns the distinct queues whose tasks became newly
	// eligible so the caller can wake them. The task must already hold its
	// terminal state; an unknown or non-terminal id is a no-op.
	ResolveDependents(ctx context.Context, taskID string) (queues []string, err error)

	// PromoteReadyDependents is the reaper safety net for ResolveDependents:
	// it scans up to limit blocked tasks whose dependencies have since reached
	// a terminal state — work an inline ResolveDependents missed or never ran
	// for (an admin cancel, a lost wake) — reconciles them, and returns the
	// distinct queues that received work. A non-positive limit promotes
	// nothing.
	PromoteReadyDependents(ctx context.Context, limit int) (queues []string, err error)

	// PurgeCompleted deletes up to limit completed tasks whose retention
	// has lapsed and releases lapsed unique-key claims. It returns the
	// number of rows deleted. A non-positive limit purges nothing.
	PurgeCompleted(ctx context.Context, limit int) (int, error)

	// ArchiveExpired archives up to limit still-waiting tasks (scheduled,
	// pending, or retry) whose expires_at has passed, so a task that was
	// never dispatched before its expiry is dead-lettered rather than run.
	// It returns the number of rows archived. A non-positive limit archives
	// nothing.
	ArchiveExpired(ctx context.Context, limit int) (int, error)

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

	// SetQueueRateLimit persists a per-queue dispatch-rate override,
	// replacing the server's global default for that queue. ratePerSec must
	// be > 0 and burst >= 1.
	SetQueueRateLimit(ctx context.Context, queue string, ratePerSec float64, burst int) error

	// DeleteQueueRateLimit removes a queue's override, reverting it to the
	// global default. Removing a missing override is not an error.
	DeleteQueueRateLimit(ctx context.Context, queue string) error

	// QueueRateLimit returns a queue's override and whether one is set; an
	// unset queue returns ok=false. The queue grain reads this at activation.
	QueueRateLimit(ctx context.Context, queue string) (RateLimit, bool, error)

	// QueueRateLimits returns every persisted override, ordered by queue
	// name, for the management API and dashboard.
	QueueRateLimits(ctx context.Context) ([]RateLimit, error)

	// SetQueueConcurrencyLimit persists a per-queue per-key concurrency cap.
	// maxActive must be >= 1.
	SetQueueConcurrencyLimit(ctx context.Context, queue string, maxActive int) error

	// DeleteQueueConcurrencyLimit removes a queue's concurrency limit, leaving
	// its keys unbounded. Removing a missing limit is not an error.
	DeleteQueueConcurrencyLimit(ctx context.Context, queue string) error

	// QueueConcurrencyLimit returns a queue's limit and whether one is set; an
	// unset queue returns ok=false. The queue grain reads this at activation.
	QueueConcurrencyLimit(ctx context.Context, queue string) (ConcurrencyLimit, bool, error)

	// QueueConcurrencyLimits returns every persisted limit, ordered by queue
	// name, for the management API and dashboard.
	QueueConcurrencyLimits(ctx context.Context) ([]ConcurrencyLimit, error)

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

	// RescheduleTask moves a waiting task's due time to processAt. A
	// scheduled, pending, or retry task is accepted: it becomes scheduled
	// when processAt is in the future and pending when it is now or in the
	// past. It returns ErrInvalidState in any other state and ErrTaskNotFound
	// when the id is unknown.
	RescheduleTask(ctx context.Context, id string, processAt time.Time) error

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

	// SetEventSink wires the lifecycle-event sink that receives a TaskEvent on
	// every state transition. It is set once at startup before the broker serves
	// traffic; a nil sink disables emission. The sink must be non-blocking.
	SetEventSink(sink events.Sink)

	// Close releases the broker's resources.
	Close() error
}
