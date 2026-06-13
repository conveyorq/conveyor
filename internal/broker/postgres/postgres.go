// Package postgres provides the durable Postgres Broker, the production
// source of truth. Leasing is a single FOR UPDATE SKIP LOCKED statement;
// every time-dependent statement takes "now" as a bind parameter from the
// injected clock — never the database clock — so lease expiry, unique
// TTLs, and retention behave identically under the conformance suite's
// fake clock and are immune to app/db clock skew.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/conveyorq/conveyor/internal/broker"
	"github.com/conveyorq/conveyor/internal/clock"
	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
)

// Task state column values, mirroring conveyor.v1.TaskState.
const (
	stateScheduled = int16(conveyorv1.TaskState_TASK_STATE_SCHEDULED)
	statePending   = int16(conveyorv1.TaskState_TASK_STATE_PENDING)
	stateActive    = int16(conveyorv1.TaskState_TASK_STATE_ACTIVE)
	stateRetry     = int16(conveyorv1.TaskState_TASK_STATE_RETRY)
	stateCompleted = int16(conveyorv1.TaskState_TASK_STATE_COMPLETED)
	stateArchived  = int16(conveyorv1.TaskState_TASK_STATE_ARCHIVED)
	stateCanceled  = int16(conveyorv1.TaskState_TASK_STATE_CANCELED)
)

// uniqueIndexName is the partial unique index enforcing task uniqueness;
// a 23505 on it maps to broker.ErrDuplicateTask.
const uniqueIndexName = "conveyor_tasks_unique_idx"

// uniqueViolationCode is the Postgres SQLSTATE for unique violations.
const uniqueViolationCode = "23505"

// leaseQuery claims due tasks in dispatch order. The trailing SELECT
// re-orders the UPDATE's output, which carries no ordering guarantee.
var leaseQuery = fmt.Sprintf(`WITH due AS (
  SELECT id, priority, process_at FROM conveyor_tasks
  WHERE queue = $1 AND state IN (%d, %d) AND process_at <= $4
  ORDER BY priority DESC, process_at, id
  LIMIT $2
  FOR UPDATE SKIP LOCKED
), claimed AS (
  UPDATE conveyor_tasks t
  SET state = %d, lease_id = $3, lease_expires_at = $5, updated_at = $4
  FROM due WHERE t.id = due.id
  RETURNING t.id, t.payload, t.retried, t.last_error
)
SELECT c.payload, c.retried, c.last_error
FROM claimed c JOIN due d ON d.id = c.id
ORDER BY d.priority DESC, d.process_at, d.id`,
	statePending, stateRetry, stateActive)

// reapQuery reclaims expired leases: tasks with retries left return to
// retry with an incremented counter; exhausted ones are archived.
var reapQuery = fmt.Sprintf(`WITH expired AS (
  SELECT id FROM conveyor_tasks
  WHERE state = %d AND lease_expires_at <= $2
  ORDER BY lease_expires_at
  LIMIT $1
  FOR UPDATE SKIP LOCKED
)
UPDATE conveyor_tasks t SET
  state = CASE WHEN t.retried >= t.max_retry THEN %d ELSE %d END,
  retried = CASE WHEN t.retried >= t.max_retry THEN t.retried ELSE t.retried + 1 END,
  process_at = CASE WHEN t.retried >= t.max_retry THEN t.process_at ELSE $2 END,
  completed_at = CASE WHEN t.retried >= t.max_retry THEN $2 ELSE t.completed_at END,
  last_error = $3, lease_id = NULL, lease_expires_at = NULL, updated_at = $2
FROM expired WHERE t.id = expired.id
RETURNING t.queue, t.state`,
	stateActive, stateArchived, stateRetry)

// promoteQuery moves due scheduled tasks to pending.
var promoteQuery = fmt.Sprintf(`WITH due AS (
  SELECT id FROM conveyor_tasks
  WHERE state = %d AND process_at <= $2
  ORDER BY process_at
  LIMIT $1
  FOR UPDATE SKIP LOCKED
)
UPDATE conveyor_tasks t SET state = %d, updated_at = $2
FROM due WHERE t.id = due.id
RETURNING t.queue`,
	stateScheduled, statePending)

// insertTaskQuery commits one task row; the id conflict target makes
// client retries idempotent.
const insertTaskQuery = `INSERT INTO conveyor_tasks (
  id, queue, type, state, priority, payload, unique_key, unique_expires_at,
  process_at, deadline, max_retry, retried, last_error, retention,
  enqueued_at, updated_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
ON CONFLICT (id) DO NOTHING`

// releaseLapsedUniqueQuery frees a unique-key claim whose TTL has lapsed
// so the partial unique index admits a new claimant for the same key.
const releaseLapsedUniqueQuery = `UPDATE conveyor_tasks
  SET unique_key = NULL, unique_expires_at = NULL, updated_at = $2
  WHERE unique_key = $1 AND unique_expires_at IS NOT NULL AND unique_expires_at <= $2`

// extendLeaseQuery pushes an active task's lease expiry forward.
var extendLeaseQuery = fmt.Sprintf(`UPDATE conveyor_tasks
  SET lease_expires_at = $3, updated_at = $4
  WHERE id = $1 AND state = %d AND lease_id = $2`, stateActive)

// ackQuery completes an active task.
var ackQuery = fmt.Sprintf(`UPDATE conveyor_tasks
  SET state = %d, result = $3, completed_at = $4, lease_id = NULL,
    lease_expires_at = NULL, updated_at = $4
  WHERE id = $1 AND state = %d AND lease_id = $2`, stateCompleted, stateActive)

// failQuery records a failed attempt and schedules the retry.
var failQuery = fmt.Sprintf(`UPDATE conveyor_tasks
  SET state = %d, retried = retried + 1, last_error = $3, process_at = $4,
    lease_id = NULL, lease_expires_at = NULL, updated_at = $5
  WHERE id = $1 AND state = %d AND lease_id = $2`, stateRetry, stateActive)

// releaseQuery returns an active task to pending without a retry penalty.
var releaseQuery = fmt.Sprintf(`UPDATE conveyor_tasks
  SET state = %d, process_at = $3, lease_id = NULL, lease_expires_at = NULL,
    updated_at = $3
  WHERE id = $1 AND state = %d AND lease_id = $2`, statePending, stateActive)

// archiveLeasedQuery dead-letters a task held under the caller's lease.
var archiveLeasedQuery = fmt.Sprintf(`UPDATE conveyor_tasks
  SET state = %d, last_error = $3, completed_at = $4, lease_id = NULL,
    lease_expires_at = NULL, updated_at = $4
  WHERE id = $1 AND state = %d AND lease_id = $2`, stateArchived, stateActive)

// archiveAnyQuery dead-letters any non-completed task (reaper and admin).
var archiveAnyQuery = fmt.Sprintf(`UPDATE conveyor_tasks
  SET state = %d, last_error = $2, completed_at = $3, lease_id = NULL,
    lease_expires_at = NULL, updated_at = $3
  WHERE id = $1 AND state <> %d`, stateArchived, stateCompleted)

// cancelTaskQuery cancels a task that is not yet running.
var cancelTaskQuery = fmt.Sprintf(`UPDATE conveyor_tasks
  SET state = %d, completed_at = $2, updated_at = $2
  WHERE id = $1 AND state IN (%d, %d, %d)`,
	stateCanceled, stateScheduled, statePending, stateRetry)

// deleteTaskQuery removes a task unless it is actively executing.
var deleteTaskQuery = fmt.Sprintf(
	"DELETE FROM conveyor_tasks WHERE id = $1 AND state <> %d", stateActive)

// runTaskNowQuery makes a waiting task due immediately.
var runTaskNowQuery = fmt.Sprintf(`UPDATE conveyor_tasks
  SET state = %d, process_at = $2, updated_at = $2
  WHERE id = $1 AND state IN (%d, %d, %d)`,
	statePending, stateScheduled, statePending, stateRetry)

// pendingCountQuery counts due tasks per queue.
var pendingCountQuery = fmt.Sprintf(`SELECT queue, count(*) FROM conveyor_tasks
  WHERE state IN (%d, %d) AND process_at <= $1
  GROUP BY queue`, statePending, stateRetry)

// purgeCompletedQuery deletes completed tasks whose retention lapsed.
var purgeCompletedQuery = fmt.Sprintf(`WITH expired AS (
  SELECT id FROM conveyor_tasks
  WHERE state = %d AND completed_at + retention <= $1
  ORDER BY completed_at
  LIMIT $2
  FOR UPDATE SKIP LOCKED
)
DELETE FROM conveyor_tasks t USING expired WHERE t.id = expired.id`, stateCompleted)

// Broker is the Postgres broker.Broker implementation.
type Broker struct {
	// pool is the pgx connection pool.
	pool *pgxpool.Pool
	// clock supplies the current time for every statement.
	clock clock.Clock
}

// enforce interface compliance at compile time.
var _ broker.Broker = (*Broker)(nil)

// New connects to the database at dsn, applies any pending embedded
// migrations, and returns the broker reading time from the given clock.
func New(ctx context.Context, dsn string, timeSource clock.Clock) (*Broker, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres: connect: %w", err)
	}

	if err = pool.Ping(ctx); err != nil {
		pool.Close()

		return nil, fmt.Errorf("postgres: ping: %w", err)
	}

	if err = migrate(ctx, pool); err != nil {
		pool.Close()

		return nil, err
	}

	return &Broker{pool: pool, clock: timeSource}, nil
}

// Enqueue durably commits a task; see broker.Broker.
func (b *Broker) Enqueue(ctx context.Context, task *conveyorv1.TaskEnvelope) error {
	now := b.clock.Now()
	options := task.GetOptions()

	processAt := now
	if options.GetProcessAt() != nil {
		processAt = options.GetProcessAt().AsTime()
	}

	state := statePending
	if processAt.After(now) {
		state = stateScheduled
	}

	var uniqueKey *string
	if key := options.GetUniqueKey(); key != "" {
		uniqueKey = &key
	}

	var uniqueExpiresAt *time.Time
	if uniqueKey != nil && options.GetUniqueTtl() != nil {
		expiry := now.Add(options.GetUniqueTtl().AsDuration())
		uniqueExpiresAt = &expiry
	}

	enqueuedAt := now
	if task.GetEnqueuedAt() != nil {
		enqueuedAt = task.GetEnqueuedAt().AsTime()
	}

	payload, err := proto.Marshal(task)
	if err != nil {
		return fmt.Errorf("postgres: marshal envelope: %w", err)
	}

	arguments := []any{
		task.GetId(), task.GetQueue(), task.GetType(), state, options.GetPriority(),
		payload, uniqueKey, uniqueExpiresAt, processAt, protoTime(options.GetDeadline()),
		options.GetMaxRetry(), task.GetRetried(), task.GetLastError(),
		pgInterval(options.GetRetention().AsDuration()), enqueuedAt, now,
	}

	if uniqueKey == nil {
		// Common path: one round trip. The lapsed-claim release is only
		// needed when a unique key may collide with an expired claim.
		if _, err = b.pool.Exec(ctx, insertTaskQuery, arguments...); err != nil {
			return fmt.Errorf("postgres: insert task: %w", err)
		}

		return nil
	}

	transaction, err := b.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("postgres: begin enqueue: %w", err)
	}
	defer func() { _ = transaction.Rollback(ctx) }()

	if _, err = transaction.Exec(ctx, releaseLapsedUniqueQuery, *uniqueKey, now); err != nil {
		return fmt.Errorf("postgres: release lapsed unique claim: %w", err)
	}

	if _, err = transaction.Exec(ctx, insertTaskQuery, arguments...); err != nil {
		if isUniqueViolation(err) {
			return broker.ErrDuplicateTask
		}

		return fmt.Errorf("postgres: insert task: %w", err)
	}

	if err = transaction.Commit(ctx); err != nil {
		if isUniqueViolation(err) {
			return broker.ErrDuplicateTask
		}

		return fmt.Errorf("postgres: commit enqueue: %w", err)
	}

	return nil
}

// Lease atomically claims up to limit due tasks; see broker.Broker.
func (b *Broker) Lease(ctx context.Context, queue string, limit int, ttl time.Duration, leaseID string) ([]*conveyorv1.TaskEnvelope, error) {
	if limit <= 0 {
		return nil, nil
	}

	now := b.clock.Now()

	rows, err := b.pool.Query(ctx, leaseQuery, queue, limit, leaseID, now, now.Add(ttl))
	if err != nil {
		return nil, fmt.Errorf("postgres: lease: %w", err)
	}
	defer rows.Close()

	return scanEnvelopes(rows)
}

// ExtendLease pushes the lease expiry forward; see broker.Broker.
func (b *Broker) ExtendLease(ctx context.Context, taskID, leaseID string, ttl time.Duration) error {
	now := b.clock.Now()

	return b.leaseScopedExec(ctx, extendLeaseQuery, taskID, leaseID, now.Add(ttl), now)
}

// Ack completes an active task; see broker.Broker.
func (b *Broker) Ack(ctx context.Context, taskID, leaseID string, result []byte) error {
	return b.leaseScopedExec(ctx, ackQuery, taskID, leaseID, result, b.clock.Now())
}

// Fail records a failed attempt and schedules the retry; see broker.Broker.
func (b *Broker) Fail(ctx context.Context, taskID, leaseID, errMsg string, processAt time.Time) error {
	return b.leaseScopedExec(ctx, failQuery, taskID, leaseID, errMsg, processAt, b.clock.Now())
}

// Release returns an active task to pending without a retry penalty; see
// broker.Broker.
func (b *Broker) Release(ctx context.Context, taskID, leaseID string) error {
	return b.leaseScopedExec(ctx, releaseQuery, taskID, leaseID, b.clock.Now())
}

// leaseScopedExec runs a statement that must match exactly one task active
// under the caller's lease, mapping a miss to broker.ErrLeaseLost.
func (b *Broker) leaseScopedExec(ctx context.Context, statement string, arguments ...any) error {
	tag, err := b.pool.Exec(ctx, statement, arguments...)
	if err != nil {
		return fmt.Errorf("postgres: lease-scoped update: %w", err)
	}

	if tag.RowsAffected() == 0 {
		return broker.ErrLeaseLost
	}

	return nil
}

// Archive dead-letters a task; see broker.Broker.
func (b *Broker) Archive(ctx context.Context, taskID, leaseID, errMsg string) error {
	now := b.clock.Now()

	if leaseID != "" {
		return b.leaseScopedExec(ctx, archiveLeasedQuery, taskID, leaseID, errMsg, now)
	}

	tag, err := b.pool.Exec(ctx, archiveAnyQuery, taskID, errMsg, now)
	if err != nil {
		return fmt.Errorf("postgres: archive: %w", err)
	}

	if tag.RowsAffected() == 0 {
		return b.explainMiss(ctx, taskID)
	}

	return nil
}

// explainMiss distinguishes a missing task from one in an ineligible state
// after a guarded update matched no row.
func (b *Broker) explainMiss(ctx context.Context, taskID string) error {
	var state int16

	err := b.pool.QueryRow(ctx, "SELECT state FROM conveyor_tasks WHERE id = $1", taskID).Scan(&state)
	if errors.Is(err, pgx.ErrNoRows) {
		return broker.ErrTaskNotFound
	}

	if err != nil {
		return fmt.Errorf("postgres: look up task state: %w", err)
	}

	return broker.ErrInvalidState
}

// ReapExpiredLeases reclaims lapsed leases; see broker.Broker.
func (b *Broker) ReapExpiredLeases(ctx context.Context, limit int) ([]string, error) {
	if limit <= 0 {
		return nil, nil
	}

	rows, err := b.pool.Query(ctx, reapQuery, limit, b.clock.Now(), broker.LeaseExpiredMessage)
	if err != nil {
		return nil, fmt.Errorf("postgres: reap expired leases: %w", err)
	}
	defer rows.Close()

	queues := make(map[string]struct{})

	for rows.Next() {
		var (
			queue string
			state int16
		)

		if err = rows.Scan(&queue, &state); err != nil {
			return nil, fmt.Errorf("postgres: scan reaped task: %w", err)
		}

		if state == stateRetry {
			queues[queue] = struct{}{}
		}
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: reap expired leases: %w", err)
	}

	return slices.Collect(maps.Keys(queues)), nil
}

// PromoteScheduled moves due scheduled tasks to pending; see broker.Broker.
func (b *Broker) PromoteScheduled(ctx context.Context, limit int) ([]string, error) {
	if limit <= 0 {
		return nil, nil
	}

	rows, err := b.pool.Query(ctx, promoteQuery, limit, b.clock.Now())
	if err != nil {
		return nil, fmt.Errorf("postgres: promote scheduled: %w", err)
	}
	defer rows.Close()

	queues := make(map[string]struct{})

	for rows.Next() {
		var queue string
		if err = rows.Scan(&queue); err != nil {
			return nil, fmt.Errorf("postgres: scan promoted task: %w", err)
		}

		queues[queue] = struct{}{}
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: promote scheduled: %w", err)
	}

	return slices.Collect(maps.Keys(queues)), nil
}

// PurgeCompleted removes retention-expired completed tasks and lapsed
// unique-key claims; see broker.Broker.
func (b *Broker) PurgeCompleted(ctx context.Context, limit int) (int, error) {
	if limit <= 0 {
		return 0, nil
	}

	now := b.clock.Now()

	const releaseAllLapsed = `UPDATE conveyor_tasks
		SET unique_key = NULL, unique_expires_at = NULL, updated_at = $1
		WHERE unique_key IS NOT NULL AND unique_expires_at IS NOT NULL AND unique_expires_at <= $1`
	if _, err := b.pool.Exec(ctx, releaseAllLapsed, now); err != nil {
		return 0, fmt.Errorf("postgres: release lapsed unique claims: %w", err)
	}

	tag, err := b.pool.Exec(ctx, purgeCompletedQuery, now, limit)
	if err != nil {
		return 0, fmt.Errorf("postgres: purge completed: %w", err)
	}

	return int(tag.RowsAffected()), nil
}

// PendingCount counts due tasks per queue; see broker.Broker.
func (b *Broker) PendingCount(ctx context.Context) (map[string]int64, error) {
	rows, err := b.pool.Query(ctx, pendingCountQuery, b.clock.Now())
	if err != nil {
		return nil, fmt.Errorf("postgres: pending count: %w", err)
	}
	defer rows.Close()

	counts := make(map[string]int64)

	for rows.Next() {
		var (
			queue string
			count int64
		)

		if err = rows.Scan(&queue, &count); err != nil {
			return nil, fmt.Errorf("postgres: scan pending count: %w", err)
		}

		counts[queue] = count
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: pending count: %w", err)
	}

	return counts, nil
}

// queueStatsQuery aggregates task counts per (queue, state) and joins the
// persisted pause flags, so queues that only hold a pause flag still
// appear with zero counts.
const queueStatsQuery = `SELECT
  COALESCE(t.queue, s.queue) AS queue,
  COALESCE(s.paused, FALSE) AS paused,
  t.state,
  t.task_count
FROM (
  SELECT queue, state, COUNT(*) AS task_count
  FROM conveyor_tasks
  GROUP BY queue, state
) t
FULL OUTER JOIN conveyor_queue_state s ON s.queue = t.queue
ORDER BY 1`

// QueueStats aggregates task counts and pause flags per queue; see
// broker.Broker.
func (b *Broker) QueueStats(ctx context.Context) ([]broker.QueueStat, error) {
	rows, err := b.pool.Query(ctx, queueStatsQuery)
	if err != nil {
		return nil, fmt.Errorf("postgres: queue stats: %w", err)
	}
	defer rows.Close()

	var stats []broker.QueueStat

	for rows.Next() {
		var (
			queue     string
			paused    bool
			state     pgtype.Int2
			taskCount pgtype.Int8
		)

		if err = rows.Scan(&queue, &paused, &state, &taskCount); err != nil {
			return nil, fmt.Errorf("postgres: scan queue stats: %w", err)
		}

		if len(stats) == 0 || stats[len(stats)-1].Queue != queue {
			stats = append(stats, broker.QueueStat{Queue: queue, Paused: paused})
		}

		stat := &stats[len(stats)-1]

		switch conveyorv1.TaskState(state.Int16) {
		case conveyorv1.TaskState_TASK_STATE_SCHEDULED:
			stat.Scheduled = taskCount.Int64
		case conveyorv1.TaskState_TASK_STATE_PENDING:
			stat.Pending = taskCount.Int64
		case conveyorv1.TaskState_TASK_STATE_ACTIVE:
			stat.Active = taskCount.Int64
		case conveyorv1.TaskState_TASK_STATE_RETRY:
			stat.Retry = taskCount.Int64
		case conveyorv1.TaskState_TASK_STATE_COMPLETED:
			stat.Completed = taskCount.Int64
		case conveyorv1.TaskState_TASK_STATE_ARCHIVED:
			stat.Archived = taskCount.Int64
		}
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: queue stats: %w", err)
	}

	return stats, nil
}

// SetQueuePaused persists the queue pause flag; see broker.Broker.
func (b *Broker) SetQueuePaused(ctx context.Context, queue string, paused bool) error {
	const upsertPause = `INSERT INTO conveyor_queue_state (queue, paused, updated_at)
		VALUES ($1, $2, $3)
		ON CONFLICT (queue) DO UPDATE SET paused = EXCLUDED.paused, updated_at = EXCLUDED.updated_at`

	if _, err := b.pool.Exec(ctx, upsertPause, queue, paused, b.clock.Now()); err != nil {
		return fmt.Errorf("postgres: set queue paused: %w", err)
	}

	return nil
}

// QueuePaused reports the queue pause flag; see broker.Broker.
func (b *Broker) QueuePaused(ctx context.Context, queue string) (bool, error) {
	var paused bool

	err := b.pool.QueryRow(ctx, "SELECT paused FROM conveyor_queue_state WHERE queue = $1", queue).Scan(&paused)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}

	if err != nil {
		return false, fmt.Errorf("postgres: queue paused: %w", err)
	}

	return paused, nil
}

// GetTask returns one task and its state; see broker.Broker.
func (b *Broker) GetTask(ctx context.Context, id string) (*conveyorv1.TaskEnvelope, conveyorv1.TaskState, error) {
	var (
		payload   []byte
		state     int16
		retried   int32
		lastError string
	)

	err := b.pool.QueryRow(ctx,
		"SELECT payload, state, retried, last_error FROM conveyor_tasks WHERE id = $1", id,
	).Scan(&payload, &state, &retried, &lastError)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, conveyorv1.TaskState_TASK_STATE_UNSPECIFIED, broker.ErrTaskNotFound
	}

	if err != nil {
		return nil, conveyorv1.TaskState_TASK_STATE_UNSPECIFIED, fmt.Errorf("postgres: get task: %w", err)
	}

	envelope, err := unmarshalEnvelope(payload, retried, lastError)
	if err != nil {
		return nil, conveyorv1.TaskState_TASK_STATE_UNSPECIFIED, err
	}

	return envelope, conveyorv1.TaskState(state), nil
}

// ListTasks returns tasks matching the query; see broker.Broker.
func (b *Broker) ListTasks(ctx context.Context, query broker.TaskQuery) ([]broker.TaskRecord, error) {
	limit := broker.EffectiveListLimit(query.Limit)

	var (
		conditions []string
		arguments  []any
	)

	addCondition := func(column, operator string, value any) {
		arguments = append(arguments, value)
		conditions = append(conditions, column+" "+operator+" $"+strconv.Itoa(len(arguments)))
	}

	if query.Queue != "" {
		addCondition("queue", "=", query.Queue)
	}

	if query.State != conveyorv1.TaskState_TASK_STATE_UNSPECIFIED {
		addCondition("state", "=", int16(query.State))
	}

	if query.AfterID != "" {
		addCondition("id", "<", query.AfterID)
	}

	listTasks := "SELECT payload, retried, last_error, state FROM conveyor_tasks"
	if len(conditions) > 0 {
		listTasks += " WHERE " + strings.Join(conditions, " AND ")
	}

	arguments = append(arguments, limit)
	listTasks += " ORDER BY id DESC LIMIT $" + strconv.Itoa(len(arguments))

	rows, err := b.pool.Query(ctx, listTasks, arguments...)
	if err != nil {
		return nil, fmt.Errorf("postgres: list tasks: %w", err)
	}
	defer rows.Close()

	var records []broker.TaskRecord

	for rows.Next() {
		var (
			payload   []byte
			retried   int32
			lastError string
			state     int16
		)

		if err = rows.Scan(&payload, &retried, &lastError, &state); err != nil {
			return nil, fmt.Errorf("postgres: scan task: %w", err)
		}

		envelope, err := unmarshalEnvelope(payload, retried, lastError)
		if err != nil {
			return nil, err
		}

		records = append(records, broker.TaskRecord{Envelope: envelope, State: conveyorv1.TaskState(state)})
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: read tasks: %w", err)
	}

	return records, nil
}

// CancelTask cancels a not-yet-running task; see broker.Broker.
func (b *Broker) CancelTask(ctx context.Context, id string) error {
	tag, err := b.pool.Exec(ctx, cancelTaskQuery, id, b.clock.Now())
	if err != nil {
		return fmt.Errorf("postgres: cancel task: %w", err)
	}

	if tag.RowsAffected() == 0 {
		return b.explainMiss(ctx, id)
	}

	return nil
}

// DeleteTask removes a non-active task; see broker.Broker.
func (b *Broker) DeleteTask(ctx context.Context, id string) error {
	tag, err := b.pool.Exec(ctx, deleteTaskQuery, id)
	if err != nil {
		return fmt.Errorf("postgres: delete task: %w", err)
	}

	if tag.RowsAffected() == 0 {
		return b.explainMiss(ctx, id)
	}

	return nil
}

// RunTaskNow makes a task due immediately; see broker.Broker.
func (b *Broker) RunTaskNow(ctx context.Context, id string) error {
	tag, err := b.pool.Exec(ctx, runTaskNowQuery, id, b.clock.Now())
	if err != nil {
		return fmt.Errorf("postgres: run task now: %w", err)
	}

	if tag.RowsAffected() == 0 {
		return b.explainMiss(ctx, id)
	}

	return nil
}

// UpsertCronEntry creates or replaces a cron entry; see broker.Broker.
func (b *Broker) UpsertCronEntry(ctx context.Context, entry *broker.CronEntry) error {
	options, err := proto.Marshal(entry.Options)
	if err != nil {
		return fmt.Errorf("postgres: marshal cron options: %w", err)
	}

	// Nil slices would encode as NULL and violate the NOT NULL columns.
	if options == nil {
		options = []byte{}
	}

	payload := entry.Payload
	if payload == nil {
		payload = []byte{}
	}

	const upsertEntry = `INSERT INTO conveyor_cron_entries
		(id, spec, task_type, queue, payload, content_type, options, paused, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (id) DO UPDATE SET
			spec = EXCLUDED.spec, task_type = EXCLUDED.task_type, queue = EXCLUDED.queue,
			payload = EXCLUDED.payload, content_type = EXCLUDED.content_type,
			options = EXCLUDED.options, paused = EXCLUDED.paused, updated_at = EXCLUDED.updated_at`

	_, err = b.pool.Exec(ctx, upsertEntry,
		entry.ID, entry.Spec, entry.TaskType, entry.Queue, payload,
		entry.ContentType, options, entry.Paused, b.clock.Now(),
	)
	if err != nil {
		return fmt.Errorf("postgres: upsert cron entry: %w", err)
	}

	return nil
}

// ListCronEntries returns all cron entries ordered by id; see broker.Broker.
func (b *Broker) ListCronEntries(ctx context.Context) ([]*broker.CronEntry, error) {
	const listEntries = `SELECT id, spec, task_type, queue, payload, content_type, options, paused
		FROM conveyor_cron_entries ORDER BY id`

	rows, err := b.pool.Query(ctx, listEntries)
	if err != nil {
		return nil, fmt.Errorf("postgres: list cron entries: %w", err)
	}
	defer rows.Close()

	var entries []*broker.CronEntry

	for rows.Next() {
		var (
			entry       broker.CronEntry
			optionBytes []byte
		)

		err = rows.Scan(&entry.ID, &entry.Spec, &entry.TaskType, &entry.Queue,
			&entry.Payload, &entry.ContentType, &optionBytes, &entry.Paused)
		if err != nil {
			return nil, fmt.Errorf("postgres: scan cron entry: %w", err)
		}

		if len(optionBytes) > 0 {
			options := &conveyorv1.TaskOptions{}
			if err = proto.Unmarshal(optionBytes, options); err != nil {
				return nil, fmt.Errorf("postgres: unmarshal cron options: %w", err)
			}

			entry.Options = options
		}

		entries = append(entries, &entry)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: list cron entries: %w", err)
	}

	return entries, nil
}

// SetCronPaused persists the entry pause flag; see broker.Broker.
func (b *Broker) SetCronPaused(ctx context.Context, id string, paused bool) error {
	const pauseEntry = "UPDATE conveyor_cron_entries SET paused = $2, updated_at = $3 WHERE id = $1"

	tag, err := b.pool.Exec(ctx, pauseEntry, id, paused, b.clock.Now())
	if err != nil {
		return fmt.Errorf("postgres: set cron paused: %w", err)
	}

	if tag.RowsAffected() == 0 {
		return broker.ErrTaskNotFound
	}

	return nil
}

// DeleteCronEntry removes a cron entry; see broker.Broker.
func (b *Broker) DeleteCronEntry(ctx context.Context, id string) error {
	if _, err := b.pool.Exec(ctx, "DELETE FROM conveyor_cron_entries WHERE id = $1", id); err != nil {
		return fmt.Errorf("postgres: delete cron entry: %w", err)
	}

	return nil
}

// Close releases the connection pool.
func (b *Broker) Close() error {
	b.pool.Close()

	return nil
}

// scanEnvelopes drains rows of (payload, retried, last_error) into
// envelopes with the mutable fields overlaid.
func scanEnvelopes(rows pgx.Rows) ([]*conveyorv1.TaskEnvelope, error) {
	var envelopes []*conveyorv1.TaskEnvelope

	for rows.Next() {
		var (
			payload   []byte
			retried   int32
			lastError string
		)

		if err := rows.Scan(&payload, &retried, &lastError); err != nil {
			return nil, fmt.Errorf("postgres: scan task: %w", err)
		}

		envelope, err := unmarshalEnvelope(payload, retried, lastError)
		if err != nil {
			return nil, err
		}

		envelopes = append(envelopes, envelope)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: read tasks: %w", err)
	}

	return envelopes, nil
}

// unmarshalEnvelope decodes a stored envelope and stamps the authoritative
// column values onto it.
func unmarshalEnvelope(payload []byte, retried int32, lastError string) (*conveyorv1.TaskEnvelope, error) {
	envelope := &conveyorv1.TaskEnvelope{}
	if err := proto.Unmarshal(payload, envelope); err != nil {
		return nil, fmt.Errorf("postgres: unmarshal envelope: %w", err)
	}

	envelope.Retried = retried
	envelope.LastError = lastError

	return envelope, nil
}

// isUniqueViolation reports whether err is a 23505 on the index enforcing
// task uniqueness.
func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError

	return errors.As(err, &pgErr) &&
		pgErr.Code == uniqueViolationCode &&
		pgErr.ConstraintName == uniqueIndexName
}

// protoTime converts an optional proto timestamp to a nullable time.
func protoTime(timestamp *timestamppb.Timestamp) *time.Time {
	if timestamp == nil {
		return nil
	}

	converted := timestamp.AsTime()

	return &converted
}

// pgInterval converts a duration to a Postgres interval bind value.
func pgInterval(d time.Duration) pgtype.Interval {
	return pgtype.Interval{Microseconds: d.Microseconds(), Valid: true}
}
