// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

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

	"github.com/conveyorq/conveyor/internal/clock"
	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"

	"github.com/conveyorq/conveyor/internal/broker"
)

// Task state column values, mirroring conveyor.v1.TaskState.
const (
	stateScheduled   = int16(conveyorv1.TaskState_TASK_STATE_SCHEDULED)
	statePending     = int16(conveyorv1.TaskState_TASK_STATE_PENDING)
	stateActive      = int16(conveyorv1.TaskState_TASK_STATE_ACTIVE)
	stateRetry       = int16(conveyorv1.TaskState_TASK_STATE_RETRY)
	stateCompleted   = int16(conveyorv1.TaskState_TASK_STATE_COMPLETED)
	stateArchived    = int16(conveyorv1.TaskState_TASK_STATE_ARCHIVED)
	stateCanceled    = int16(conveyorv1.TaskState_TASK_STATE_CANCELED)
	stateAggregating = int16(conveyorv1.TaskState_TASK_STATE_AGGREGATING)
	stateBlocked     = int16(conveyorv1.TaskState_TASK_STATE_BLOCKED)
)

// uniqueIndexName is the partial unique index enforcing task uniqueness;
// a 23505 on it maps to broker.ErrDuplicateTask.
const uniqueIndexName = "conveyor_tasks_unique_idx"

// uniqueViolationCode is the Postgres SQLSTATE for unique violations.
const uniqueViolationCode = "23505"

// insertTaskQuery commits one task row; the id conflict target makes
// client retries idempotent.
const insertTaskQuery = `INSERT INTO conveyor_tasks (
  id, queue, type, state, priority, payload, unique_key, unique_expires_at,
  process_at, deadline, max_retry, retried, last_error, retention,
  enqueued_at, updated_at, group_key, expires_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
ON CONFLICT (id) DO NOTHING`

// releaseLapsedUniqueQuery frees a unique-key claim whose TTL has lapsed
// so the partial unique index admits a new claimant for the same key.
const releaseLapsedUniqueQuery = `UPDATE conveyor_tasks
  SET unique_key = NULL, unique_expires_at = NULL, updated_at = $2
  WHERE unique_key = $1 AND unique_expires_at IS NOT NULL AND unique_expires_at <= $2`

// insertEdgeQuery records one unresolved dependency edge of a blocked task.
const insertEdgeQuery = `INSERT INTO conveyor_task_deps (dependent_id, dependency_id, on_failure)
  VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`

// dependencyStatesQuery reads the current state of each candidate dependency at
// enqueue time, so an already-finished dependency is reconciled immediately.
const dependencyStatesQuery = "SELECT id, state FROM conveyor_tasks WHERE id = ANY($1)"

// edgesForDependencyQuery lists the edges waiting on a terminal task, ordered by
// dependent id so concurrent resolvers lock dependent rows in a consistent
// order, avoiding deadlocks when their dependency sets overlap.
const edgesForDependencyQuery = "SELECT dependent_id, on_failure FROM conveyor_task_deps WHERE dependency_id = $1 ORDER BY dependent_id"

// dropEdgeQuery removes one satisfied edge.
const dropEdgeQuery = "DELETE FROM conveyor_task_deps WHERE dependent_id = $1 AND dependency_id = $2"

// dropDependentEdgesQuery removes every edge of a dependent, used when it is
// cascade-canceled (it no longer waits on anything).
const dropDependentEdgesQuery = "DELETE FROM conveyor_task_deps WHERE dependent_id = $1"

// hasDependentsQuery reports whether any edge waits on the given task.
const hasDependentsQuery = "SELECT EXISTS (SELECT 1 FROM conveyor_task_deps WHERE dependency_id = $1)"

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

// cronColumns is the cron-entry projection shared by the list queries.
const cronColumns = "id, spec, task_type, queue, payload, content_type, options, paused, next_run_at"

// leaseQuery claims due tasks in dispatch order. The trailing SELECT
// re-orders the UPDATE's output, which carries no ordering guarantee.
var leaseQuery = fmt.Sprintf(`WITH due AS (
  SELECT id, priority, process_at FROM conveyor_tasks
  WHERE queue = $1 AND state IN (%d, %d) AND process_at <= $4
    AND (expires_at IS NULL OR expires_at > $4)
  ORDER BY priority DESC, process_at, id
  LIMIT $2
  FOR UPDATE SKIP LOCKED
), claimed AS (
  UPDATE conveyor_tasks t
  SET state = %d, lease_id = $3, lease_expires_at = $5, started_at = $4, updated_at = $4
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
  WHERE id = $1 AND state IN (%d, %d, %d, %d, %d)`,
	stateCanceled, stateScheduled, statePending, stateRetry, stateAggregating, stateBlocked)

// deleteTaskQuery removes a task unless it is actively executing.
var deleteTaskQuery = fmt.Sprintf(
	"DELETE FROM conveyor_tasks WHERE id = $1 AND state <> %d", stateActive)

// runTaskNowQuery makes a waiting or archived task due immediately; re-running
// an archived task clears its completion stamp so it lives again.
var runTaskNowQuery = fmt.Sprintf(`UPDATE conveyor_tasks
  SET state = %d, process_at = $2, completed_at = NULL, updated_at = $2
  WHERE id = $1 AND state IN (%d, %d, %d, %d)`,
	statePending, stateScheduled, statePending, stateRetry, stateArchived)

// archiveWaitingQuery dead-letters a scheduled, pending, retry, or blocked task.
var archiveWaitingQuery = fmt.Sprintf(`UPDATE conveyor_tasks
  SET state = %d, completed_at = $2, updated_at = $2
  WHERE id = $1 AND state IN (%d, %d, %d, %d)`,
	stateArchived, stateScheduled, statePending, stateRetry, stateBlocked)

// pendingCountQuery counts due tasks per queue.
var pendingCountQuery = fmt.Sprintf(`SELECT queue, count(*) FROM conveyor_tasks
  WHERE state IN (%d, %d) AND process_at <= $1
  GROUP BY queue`, statePending, stateRetry)

// purgeCompletedQuery deletes completed tasks whose retention lapsed. It skips a
// task that other tasks still depend on, so a dependency is never purged out
// from under a dependent that has yet to resolve against it.
var purgeCompletedQuery = fmt.Sprintf(`WITH expired AS (
  SELECT id FROM conveyor_tasks t
  WHERE state = %d AND completed_at + retention <= $1
    AND NOT EXISTS (SELECT 1 FROM conveyor_task_deps d WHERE d.dependency_id = t.id)
  ORDER BY completed_at
  LIMIT $2
  FOR UPDATE SKIP LOCKED
)
DELETE FROM conveyor_tasks t USING expired WHERE t.id = expired.id`, stateCompleted)

// archiveExpiredQuery dead-letters still-waiting tasks (scheduled, pending, or
// retry) whose pre-dispatch expiry lapsed, so a task never dispatched in time
// is archived rather than run.
var archiveExpiredQuery = fmt.Sprintf(`WITH expired AS (
  SELECT id FROM conveyor_tasks
  WHERE state IN (%d, %d, %d, %d) AND expires_at IS NOT NULL AND expires_at <= $1
  ORDER BY expires_at
  LIMIT $2
  FOR UPDATE SKIP LOCKED
)
UPDATE conveyor_tasks t
SET state = %d, last_error = $3, completed_at = $1,
    lease_id = NULL, lease_expires_at = NULL, updated_at = $1
FROM expired WHERE t.id = expired.id`,
	stateScheduled, statePending, stateRetry, stateBlocked, stateArchived)

// stampCanceledQuery marks a task canceled at enqueue time, used only when a
// task declares a cascade-cancel dependency that has already failed.
var stampCanceledQuery = fmt.Sprintf(
	"UPDATE conveyor_tasks SET completed_at = $2 WHERE id = $1 AND state = %d", stateCanceled)

// cascadeCancelQuery cancels a blocked dependent whose dependency failed under
// the cascade-cancel policy.
var cascadeCancelQuery = fmt.Sprintf(`UPDATE conveyor_tasks
  SET state = %d, completed_at = $2, last_error = $3, updated_at = $2
  WHERE id = $1 AND state = %d`, stateCanceled, stateBlocked)

// lockBlockedDependentQuery takes a row lock on a still-blocked dependent so
// concurrent resolvers of its sibling dependencies serialize: each waits its
// turn and then observes the others' committed edge deletes, so the resolver
// that clears the final edge is the one that promotes. Without this, every
// concurrent resolver's NOT EXISTS check reads a snapshot taken before the
// others committed and none promotes — a lost wakeup leaving the task blocked.
var lockBlockedDependentQuery = fmt.Sprintf(
	"SELECT id FROM conveyor_tasks WHERE id = $1 AND state = %d FOR UPDATE", stateBlocked)

// promoteDependentQuery promotes a blocked task to the state it would have held
// without dependencies — aggregating when grouped, scheduled when still delayed,
// otherwise pending — but only once no edge remains. It returns the queue so the
// caller can wake it.
var promoteDependentQuery = fmt.Sprintf(`UPDATE conveyor_tasks t
  SET state = CASE WHEN t.group_key <> '' THEN %d WHEN t.process_at > $2 THEN %d ELSE %d END,
    updated_at = $2
  WHERE t.id = $1 AND t.state = %d
    AND NOT EXISTS (SELECT 1 FROM conveyor_task_deps d WHERE d.dependent_id = t.id)
  RETURNING t.queue`,
	stateAggregating, stateScheduled, statePending, stateBlocked)

// readyDependenciesQuery finds dependency ids that have reached a terminal state
// but still carry edges — the resolutions the inline path missed.
var readyDependenciesQuery = fmt.Sprintf(`SELECT DISTINCT d.dependency_id
  FROM conveyor_task_deps d
  JOIN conveyor_tasks t ON t.id = d.dependency_id
  WHERE t.state IN (%d, %d, %d)
  LIMIT $1`, stateCompleted, stateArchived, stateCanceled)

// promoteOrphanBlockedQuery promotes blocked tasks that already hold no edges —
// the backstop for a dependent left blocked by an inline resolve that committed
// its edge deletes but was then aborted (e.g. a deadlock) before promoting.
var promoteOrphanBlockedQuery = fmt.Sprintf(`WITH ready AS (
  SELECT id FROM conveyor_tasks t
  WHERE t.state = %d
    AND NOT EXISTS (SELECT 1 FROM conveyor_task_deps d WHERE d.dependent_id = t.id)
  LIMIT $1
  FOR UPDATE SKIP LOCKED
)
UPDATE conveyor_tasks t
SET state = CASE WHEN t.group_key <> '' THEN %d WHEN t.process_at > $2 THEN %d ELSE %d END,
    updated_at = $2
FROM ready WHERE t.id = ready.id
RETURNING t.queue`,
	stateBlocked, stateAggregating, stateScheduled, statePending)

// enforce interface compliance at compile time.
var _ broker.Broker = (*Broker)(nil)

// leaseGroupQuery claims a (queue, group)'s aggregating members as one batch,
// ordered by enqueue time then id; it mirrors leaseQuery's CTE pattern.
var leaseGroupQuery = fmt.Sprintf(`WITH due AS (
  SELECT id, enqueued_at FROM conveyor_tasks
  WHERE queue = $1 AND group_key = $2 AND state = %d
  ORDER BY enqueued_at, id
  LIMIT $3
  FOR UPDATE SKIP LOCKED
), claimed AS (
  UPDATE conveyor_tasks t
  SET state = %d, lease_id = $4, lease_expires_at = $6, started_at = $5, updated_at = $5
  FROM due WHERE t.id = due.id
  RETURNING t.id, t.payload, t.retried, t.last_error
)
SELECT c.payload, c.retried, c.last_error
FROM claimed c JOIN due d ON d.id = c.id
ORDER BY d.enqueued_at, d.id`, stateAggregating, stateActive)

// groupStatsQuery summarizes the aggregating members per (queue, group) across
// all queues. The WHERE state filter rides the partial aggregating index, so it
// scans only aggregating rows. MIN(type) names the group's task type; groups
// are single-type, so any member's type is the batch's handler routing key.
var groupStatsQuery = fmt.Sprintf(`SELECT queue, group_key, MIN(type), COUNT(*), MIN(enqueued_at), MAX(enqueued_at)
  FROM conveyor_tasks
  WHERE state = %d
  GROUP BY queue, group_key
  ORDER BY queue, group_key`, stateAggregating)

// dependencyEdge is one waiting edge: the dependent task and its on-failure
// policy for the dependency being resolved.
type dependencyEdge struct {
	dependent string
	policy    int16
}

// Broker is the Postgres broker.Broker implementation.
type Broker struct {
	// pool is the pgx connection pool.
	pool *pgxpool.Pool
	// clock supplies the current time for every statement.
	clock clock.Clock
}

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
func (b *Broker) Enqueue(ctx context.Context, task *conveyorv1.TaskEnvelope) (err error) {
	now := b.clock.Now()
	options := task.GetOptions()

	processAt := now
	if options.GetProcessAt() != nil {
		processAt = options.GetProcessAt().AsTime()
	}

	group := options.GetGroup()
	if group != "" && processAt.After(now) {
		return broker.ErrGroupedSchedule
	}

	state := statePending

	switch {
	case group != "":
		state = stateAggregating
	case processAt.After(now):
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

	buildArguments := func(state int16) []any {
		return []any{
			task.GetId(), task.GetQueue(), task.GetType(), state, options.GetPriority(),
			payload, uniqueKey, uniqueExpiresAt, processAt, protoTime(options.GetDeadline()),
			options.GetMaxRetry(), task.GetRetried(), task.GetLastError(),
			pgInterval(options.GetRetention().AsDuration()), enqueuedAt, now, group,
			protoTime(options.GetExpiresAt()),
		}
	}

	edges := options.GetDependsOn()

	if uniqueKey == nil && len(edges) == 0 {
		// Common path: one round trip. The lapsed-claim release is only
		// needed when a unique key may collide with an expired claim.
		if _, err = b.pool.Exec(ctx, insertTaskQuery, buildArguments(state)...); err != nil {
			return fmt.Errorf("postgres: insert task: %w", err)
		}

		return nil
	}

	transaction, err := b.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("postgres: begin enqueue: %w", err)
	}

	defer func() {
		err = rollback(ctx, transaction, err)
	}()

	if uniqueKey != nil {
		if _, err = transaction.Exec(ctx, releaseLapsedUniqueQuery, *uniqueKey, now); err != nil {
			return fmt.Errorf("postgres: release lapsed unique claim: %w", err)
		}
	}

	deps, cancel, err := b.resolveInitialDeps(ctx, transaction, task.GetId(), edges)
	if err != nil {
		return err
	}

	switch {
	case cancel:
		state = stateCanceled
	case len(deps) > 0:
		state = stateBlocked
	}

	tag, err := transaction.Exec(ctx, insertTaskQuery, buildArguments(state)...)
	if err != nil {
		if isUniqueViolation(err) {
			return broker.ErrDuplicateTask
		}

		return fmt.Errorf("postgres: insert task: %w", err)
	}

	// A conflict on the id means the task is already committed: this is an
	// idempotent client retry, so leave its existing dependency edges untouched
	// rather than re-inserting edges a prior resolution may already have drained.
	if tag.RowsAffected() == 0 {
		if err = transaction.Commit(ctx); err != nil {
			return fmt.Errorf("postgres: commit enqueue: %w", err)
		}

		return nil
	}

	if state == stateCanceled {
		if _, err = transaction.Exec(ctx, stampCanceledQuery, task.GetId(), now); err != nil {
			return fmt.Errorf("postgres: stamp canceled task: %w", err)
		}
	}

	for depID, policy := range deps {
		if _, err = transaction.Exec(ctx, insertEdgeQuery, task.GetId(), depID, policy); err != nil {
			return fmt.Errorf("postgres: insert dependency edge: %w", err)
		}
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

// LeaseGroup claims a group's aggregating members as one batch; see broker.Broker.
func (b *Broker) LeaseGroup(ctx context.Context, queue, group string, limit int, ttl time.Duration, leaseID string) ([]*conveyorv1.TaskEnvelope, error) {
	if limit <= 0 {
		return nil, nil
	}

	now := b.clock.Now()

	rows, err := b.pool.Query(ctx, leaseGroupQuery, queue, group, limit, leaseID, now, now.Add(ttl))
	if err != nil {
		return nil, fmt.Errorf("postgres: lease group: %w", err)
	}
	defer rows.Close()

	return scanEnvelopes(rows)
}

// GroupStats summarizes the aggregating groups across all queues; see
// broker.Broker.
func (b *Broker) GroupStats(ctx context.Context) ([]broker.GroupStat, error) {
	rows, err := b.pool.Query(ctx, groupStatsQuery)
	if err != nil {
		return nil, fmt.Errorf("postgres: group stats: %w", err)
	}
	defer rows.Close()

	var stats []broker.GroupStat

	for rows.Next() {
		var (
			queue    string
			group    string
			taskType string
			count    pgtype.Int8
			oldest   pgtype.Timestamptz
			newest   pgtype.Timestamptz
		)

		if err = rows.Scan(&queue, &group, &taskType, &count, &oldest, &newest); err != nil {
			return nil, fmt.Errorf("postgres: scan group stats: %w", err)
		}

		stats = append(stats, broker.GroupStat{
			Queue:  queue,
			Group:  group,
			Type:   taskType,
			Count:  count.Int64,
			Oldest: oldest.Time,
			Newest: newest.Time,
		})
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: group stats: %w", err)
	}

	return stats, nil
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

// ResolveDependents reconciles edges pointing at a terminal task; see
// broker.Broker.
func (b *Broker) ResolveDependents(ctx context.Context, taskID string) (woken []string, err error) {
	// Fast path: most finished tasks have no dependents. One indexed lookup
	// keeps depless completions — the overwhelming majority — off the
	// transaction path entirely. It is a hint, not authoritative: a dependent
	// committing its edge just after this read is handled at its own enqueue
	// (which reconciles an already-terminal dependency) or by the reaper sweep.
	var hasDependents bool
	if err = b.pool.QueryRow(ctx, hasDependentsQuery, taskID).Scan(&hasDependents); err != nil {
		return nil, fmt.Errorf("postgres: check dependents: %w", err)
	}

	if !hasDependents {
		return nil, nil
	}

	transaction, err := b.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("postgres: begin resolve dependents: %w", err)
	}
	defer func() { err = rollback(ctx, transaction, err) }()

	queues, err := b.resolveWithin(ctx, transaction, []string{taskID})
	if err != nil {
		return nil, err
	}

	if err = transaction.Commit(ctx); err != nil {
		return nil, fmt.Errorf("postgres: commit resolve dependents: %w", err)
	}

	return slices.Collect(maps.Keys(queues)), nil
}

// PromoteReadyDependents reconciles blocked tasks whose dependencies have since
// gone terminal; see broker.Broker.
func (b *Broker) PromoteReadyDependents(ctx context.Context, limit int) (woken []string, err error) {
	if limit <= 0 {
		return nil, nil
	}

	transaction, err := b.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("postgres: begin promote ready dependents: %w", err)
	}
	defer func() {
		err = rollback(ctx, transaction, err)
	}()

	rows, err := transaction.Query(ctx, readyDependenciesQuery, limit)
	if err != nil {
		return nil, fmt.Errorf("postgres: read ready dependencies: %w", err)
	}

	var seed []string

	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()

			return nil, fmt.Errorf("postgres: scan ready dependency: %w", err)
		}

		seed = append(seed, id)
	}

	rows.Close()

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: read ready dependencies: %w", err)
	}

	queues, err := b.resolveWithin(ctx, transaction, seed)
	if err != nil {
		return nil, err
	}

	orphanRows, err := transaction.Query(ctx, promoteOrphanBlockedQuery, limit, b.clock.Now())
	if err != nil {
		return nil, fmt.Errorf("postgres: promote orphaned blocked tasks: %w", err)
	}

	for orphanRows.Next() {
		var queue string
		if err = orphanRows.Scan(&queue); err != nil {
			orphanRows.Close()
			return nil, fmt.Errorf("postgres: scan promoted orphan: %w", err)
		}

		queues[queue] = struct{}{}
	}

	orphanRows.Close()

	if err = orphanRows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: promote orphaned blocked tasks: %w", err)
	}

	if err = transaction.Commit(ctx); err != nil {
		return nil, fmt.Errorf("postgres: commit promote ready dependents: %w", err)
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

// ArchiveExpired dead-letters still-waiting tasks past their expiry; see broker.Broker.
func (b *Broker) ArchiveExpired(ctx context.Context, limit int) (int, error) {
	if limit <= 0 {
		return 0, nil
	}

	tag, err := b.pool.Exec(ctx, archiveExpiredQuery, b.clock.Now(), limit, broker.TaskExpiredMessage)
	if err != nil {
		return 0, fmt.Errorf("postgres: archive expired tasks: %w", err)
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
		case conveyorv1.TaskState_TASK_STATE_AGGREGATING:
			stat.Aggregating = taskCount.Int64
		case conveyorv1.TaskState_TASK_STATE_BLOCKED:
			stat.Blocked = taskCount.Int64
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

// SetQueueRateLimit persists a per-queue dispatch-rate override; see broker.Broker.
func (b *Broker) SetQueueRateLimit(ctx context.Context, queue string, ratePerSec float64, burst int) error {
	const upsert = `INSERT INTO conveyor_rate_limits (queue, rate_per_sec, burst, updated_at)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (queue) DO UPDATE SET
			rate_per_sec = EXCLUDED.rate_per_sec, burst = EXCLUDED.burst, updated_at = EXCLUDED.updated_at`

	if _, err := b.pool.Exec(ctx, upsert, queue, ratePerSec, burst, b.clock.Now()); err != nil {
		return fmt.Errorf("postgres: set queue rate limit: %w", err)
	}

	return nil
}

// DeleteQueueRateLimit removes a queue's override; see broker.Broker.
func (b *Broker) DeleteQueueRateLimit(ctx context.Context, queue string) error {
	if _, err := b.pool.Exec(ctx, "DELETE FROM conveyor_rate_limits WHERE queue = $1", queue); err != nil {
		return fmt.Errorf("postgres: delete queue rate limit: %w", err)
	}

	return nil
}

// QueueRateLimit returns a queue's override and whether one is set; see broker.Broker.
func (b *Broker) QueueRateLimit(ctx context.Context, queue string) (broker.RateLimit, bool, error) {
	limit := broker.RateLimit{Queue: queue}

	err := b.pool.QueryRow(ctx,
		"SELECT rate_per_sec, burst, updated_at FROM conveyor_rate_limits WHERE queue = $1", queue).
		Scan(&limit.RatePerSec, &limit.Burst, &limit.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return broker.RateLimit{}, false, nil
	}

	if err != nil {
		return broker.RateLimit{}, false, fmt.Errorf("postgres: queue rate limit: %w", err)
	}

	return limit, true, nil
}

// QueueRateLimits returns every override ordered by queue name; see broker.Broker.
func (b *Broker) QueueRateLimits(ctx context.Context) ([]broker.RateLimit, error) {
	rows, err := b.pool.Query(ctx,
		"SELECT queue, rate_per_sec, burst, updated_at FROM conveyor_rate_limits ORDER BY queue")
	if err != nil {
		return nil, fmt.Errorf("postgres: queue rate limits: %w", err)
	}

	defer rows.Close()

	var limits []broker.RateLimit

	for rows.Next() {
		var limit broker.RateLimit

		if err := rows.Scan(&limit.Queue, &limit.RatePerSec, &limit.Burst, &limit.UpdatedAt); err != nil {
			return nil, fmt.Errorf("postgres: scan queue rate limit: %w", err)
		}

		limits = append(limits, limit)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: queue rate limits: %w", err)
	}

	return limits, nil
}

// SetQueueConcurrencyLimit persists a per-queue per-key concurrency cap; see broker.Broker.
func (b *Broker) SetQueueConcurrencyLimit(ctx context.Context, queue string, maxActive int) error {
	const upsert = `INSERT INTO conveyor_concurrency_limits (queue, max_active, updated_at)
		VALUES ($1, $2, $3)
		ON CONFLICT (queue) DO UPDATE SET
			max_active = EXCLUDED.max_active, updated_at = EXCLUDED.updated_at`

	if _, err := b.pool.Exec(ctx, upsert, queue, maxActive, b.clock.Now()); err != nil {
		return fmt.Errorf("postgres: set queue concurrency limit: %w", err)
	}

	return nil
}

// DeleteQueueConcurrencyLimit removes a queue's concurrency limit; see broker.Broker.
func (b *Broker) DeleteQueueConcurrencyLimit(ctx context.Context, queue string) error {
	if _, err := b.pool.Exec(ctx, "DELETE FROM conveyor_concurrency_limits WHERE queue = $1", queue); err != nil {
		return fmt.Errorf("postgres: delete queue concurrency limit: %w", err)
	}

	return nil
}

// QueueConcurrencyLimit returns a queue's limit and whether one is set; see broker.Broker.
func (b *Broker) QueueConcurrencyLimit(ctx context.Context, queue string) (broker.ConcurrencyLimit, bool, error) {
	limit := broker.ConcurrencyLimit{Queue: queue}

	err := b.pool.QueryRow(ctx,
		"SELECT max_active, updated_at FROM conveyor_concurrency_limits WHERE queue = $1", queue).
		Scan(&limit.MaxActive, &limit.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return broker.ConcurrencyLimit{}, false, nil
	}

	if err != nil {
		return broker.ConcurrencyLimit{}, false, fmt.Errorf("postgres: queue concurrency limit: %w", err)
	}

	return limit, true, nil
}

// QueueConcurrencyLimits returns every limit ordered by queue name; see broker.Broker.
func (b *Broker) QueueConcurrencyLimits(ctx context.Context) ([]broker.ConcurrencyLimit, error) {
	rows, err := b.pool.Query(ctx,
		"SELECT queue, max_active, updated_at FROM conveyor_concurrency_limits ORDER BY queue")
	if err != nil {
		return nil, fmt.Errorf("postgres: queue concurrency limits: %w", err)
	}

	defer rows.Close()

	var limits []broker.ConcurrencyLimit

	for rows.Next() {
		var limit broker.ConcurrencyLimit

		if err := rows.Scan(&limit.Queue, &limit.MaxActive, &limit.UpdatedAt); err != nil {
			return nil, fmt.Errorf("postgres: scan queue concurrency limit: %w", err)
		}

		limits = append(limits, limit)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: queue concurrency limits: %w", err)
	}

	return limits, nil
}

// Info reports the Postgres engine's driver, connection-pool counters, and
// table row counts; see broker.Broker.
func (b *Broker) Info(ctx context.Context) (broker.Info, error) {
	metrics := map[string]string{}

	stat := b.pool.Stat()
	metrics["pool_total_conns"] = strconv.FormatInt(int64(stat.TotalConns()), 10)
	metrics["pool_acquired_conns"] = strconv.FormatInt(int64(stat.AcquiredConns()), 10)
	metrics["pool_idle_conns"] = strconv.FormatInt(int64(stat.IdleConns()), 10)
	metrics["pool_max_conns"] = strconv.FormatInt(int64(stat.MaxConns()), 10)

	var (
		tasks   int64
		entries int64
		version string
	)

	if err := b.pool.QueryRow(ctx, "SELECT count(*) FROM conveyor_tasks").Scan(&tasks); err != nil {
		return broker.Info{}, fmt.Errorf("postgres: count tasks: %w", err)
	}

	if err := b.pool.QueryRow(ctx, "SELECT count(*) FROM conveyor_cron_entries").Scan(&entries); err != nil {
		return broker.Info{}, fmt.Errorf("postgres: count cron entries: %w", err)
	}

	if err := b.pool.QueryRow(ctx, "SHOW server_version").Scan(&version); err == nil {
		metrics["server_version"] = version
	}

	metrics["tasks"] = strconv.FormatInt(tasks, 10)
	metrics["cron_entries"] = strconv.FormatInt(entries, 10)

	return broker.Info{Driver: "postgres", Metrics: metrics}, nil
}

// GetTask returns one task and its state; see broker.Broker.
func (b *Broker) GetTask(ctx context.Context, id string) (*conveyorv1.TaskEnvelope, conveyorv1.TaskState, error) {
	var (
		payload     []byte
		state       int16
		retried     int32
		lastError   string
		startedAt   *time.Time
		completedAt *time.Time
	)

	err := b.pool.QueryRow(ctx,
		"SELECT payload, state, retried, last_error, started_at, completed_at FROM conveyor_tasks WHERE id = $1", id,
	).Scan(&payload, &state, &retried, &lastError, &startedAt, &completedAt)
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

	stampExecutionTimes(envelope, startedAt, completedAt)

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

	listTasks := "SELECT payload, retried, last_error, state, started_at, completed_at FROM conveyor_tasks"
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
			payload     []byte
			retried     int32
			lastError   string
			state       int16
			startedAt   *time.Time
			completedAt *time.Time
		)

		if err = rows.Scan(&payload, &retried, &lastError, &state, &startedAt, &completedAt); err != nil {
			return nil, fmt.Errorf("postgres: scan task: %w", err)
		}

		envelope, err := unmarshalEnvelope(payload, retried, lastError)
		if err != nil {
			return nil, err
		}

		stampExecutionTimes(envelope, startedAt, completedAt)

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

	if _, err = b.pool.Exec(ctx, dropDependentEdgesQuery, id); err != nil {
		return fmt.Errorf("postgres: drop deleted task edges: %w", err)
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

// ArchiveTask dead-letters a waiting task; see broker.Broker.
func (b *Broker) ArchiveTask(ctx context.Context, id string) error {
	tag, err := b.pool.Exec(ctx, archiveWaitingQuery, id, b.clock.Now())
	if err != nil {
		return fmt.Errorf("postgres: archive task: %w", err)
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

	// next_run_at resets to NULL on every upsert: the scheduler re-arms the
	// entry from its (possibly changed) spec on the next tick.
	const upsertEntry = `INSERT INTO conveyor_cron_entries
		(id, spec, task_type, queue, payload, content_type, options, paused, next_run_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (id) DO UPDATE SET
			spec = EXCLUDED.spec, task_type = EXCLUDED.task_type, queue = EXCLUDED.queue,
			payload = EXCLUDED.payload, content_type = EXCLUDED.content_type,
			options = EXCLUDED.options, paused = EXCLUDED.paused,
			next_run_at = EXCLUDED.next_run_at, updated_at = EXCLUDED.updated_at`

	_, err = b.pool.Exec(ctx, upsertEntry,
		entry.ID, entry.Spec, entry.TaskType, entry.Queue, payload,
		entry.ContentType, options, entry.Paused, nullableTime(entry.NextRunAt), b.clock.Now(),
	)
	if err != nil {
		return fmt.Errorf("postgres: upsert cron entry: %w", err)
	}

	return nil
}

// UpdateCronNextRun compare-and-sets one entry's next fire time; see
// broker.Broker. IS NOT DISTINCT FROM matches a NULL cursor against the zero
// expected time, so arming a freshly upserted entry works the same way.
func (b *Broker) UpdateCronNextRun(ctx context.Context, id string, expected, next time.Time) error {
	const update = `UPDATE conveyor_cron_entries SET next_run_at = $3
		WHERE id = $1 AND next_run_at IS NOT DISTINCT FROM $2`

	if _, err := b.pool.Exec(ctx, update, id, nullableTime(expected), next); err != nil {
		return fmt.Errorf("postgres: update cron next run: %w", err)
	}

	return nil
}

// ListCronEntries returns all cron entries ordered by id; see broker.Broker.
func (b *Broker) ListCronEntries(ctx context.Context) ([]*broker.CronEntry, error) {
	rows, err := b.pool.Query(ctx, "SELECT "+cronColumns+" FROM conveyor_cron_entries ORDER BY id")
	if err != nil {
		return nil, fmt.Errorf("postgres: list cron entries: %w", err)
	}
	defer rows.Close()

	return scanCronEntries(rows)
}

// ListDueCronEntries returns the non-paused entries due to fire; see
// broker.Broker.
func (b *Broker) ListDueCronEntries(ctx context.Context, now time.Time) ([]*broker.CronEntry, error) {
	const query = "SELECT " + cronColumns + ` FROM conveyor_cron_entries
		WHERE NOT paused AND (next_run_at IS NULL OR next_run_at <= $1) ORDER BY id`

	rows, err := b.pool.Query(ctx, query, now)
	if err != nil {
		return nil, fmt.Errorf("postgres: list due cron entries: %w", err)
	}
	defer rows.Close()

	return scanCronEntries(rows)
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

// rollback rolls a transaction back during deferred cleanup. A rollback after a
// successful commit reports pgx.ErrTxClosed — the expected no-op, which is
// dropped. Any other rollback failure is joined onto the in-flight error so a
// genuine cleanup failure is never silently swallowed. Use it as
// `defer func() { err = rollback(ctx, tx, err) }()` with a named error return.
func rollback(ctx context.Context, tx pgx.Tx, err error) error {
	rollbackErr := tx.Rollback(ctx)
	if rollbackErr == nil || errors.Is(rollbackErr, pgx.ErrTxClosed) {
		return err
	}

	return errors.Join(err, fmt.Errorf("postgres: rollback: %w", rollbackErr))
}

// resolveInitialDeps reads the current state of each declared dependency within
// the enqueue transaction and returns the still-unsatisfied edges as a map of
// dependency id to on-failure policy. A dependency already completed is dropped;
// one already failed terminally is applied through its policy (continue drops
// it, block keeps it, cascade-cancel signals that the task should be canceled
// outright); every other dependency is retained as a block.
func (b *Broker) resolveInitialDeps(ctx context.Context, tx pgx.Tx, taskID string, edges []*conveyorv1.TaskDependency) (map[string]int16, bool, error) {
	if len(edges) == 0 {
		return nil, false, nil
	}

	policies := make(map[string]int16, len(edges))

	for _, edge := range edges {
		depTaskID := edge.GetTaskId()
		if depTaskID == "" || depTaskID == taskID {
			continue
		}

		policies[depTaskID] = dependencyPolicy(edge.GetOnFailure())
	}

	if len(policies) == 0 {
		return nil, false, nil
	}

	states, err := b.dependencyStates(ctx, tx, slices.Collect(maps.Keys(policies)))
	if err != nil {
		return nil, false, err
	}

	deps := make(map[string]int16)

	for policyID, policy := range policies {
		state, known := states[policyID]

		switch {
		case !known:
			deps[policyID] = policy
		case state == stateCompleted:
			// already satisfied
		case state == stateArchived || state == stateCanceled:
			switch policy {
			case int16(conveyorv1.DependencyFailurePolicy_DEPENDENCY_FAILURE_POLICY_CONTINUE):
				// failed dependency treated as satisfied
			case int16(conveyorv1.DependencyFailurePolicy_DEPENDENCY_FAILURE_POLICY_CASCADE_CANCEL):
				return nil, true, nil
			default:
				deps[policyID] = policy
			}
		default:
			deps[policyID] = policy
		}
	}

	if len(deps) == 0 {
		return nil, false, nil
	}

	return deps, false, nil
}

// dependencyStates reads the state column of each given task id.
func (b *Broker) dependencyStates(ctx context.Context, tx pgx.Tx, ids []string) (map[string]int16, error) {
	rows, err := tx.Query(ctx, dependencyStatesQuery, ids)
	if err != nil {
		return nil, fmt.Errorf("postgres: read dependency states: %w", err)
	}
	defer rows.Close()

	states := make(map[string]int16)

	for rows.Next() {
		var (
			id    string
			state int16
		)

		if err = rows.Scan(&id, &state); err != nil {
			return nil, fmt.Errorf("postgres: scan dependency state: %w", err)
		}

		states[id] = state
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: read dependency states: %w", err)
	}

	return states, nil
}

// dependencyPolicy resolves the unspecified policy to its block default.
func dependencyPolicy(policy conveyorv1.DependencyFailurePolicy) int16 {
	if policy == conveyorv1.DependencyFailurePolicy_DEPENDENCY_FAILURE_POLICY_UNSPECIFIED {
		return int16(conveyorv1.DependencyFailurePolicy_DEPENDENCY_FAILURE_POLICY_BLOCK)
	}

	return int16(policy)
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

// resolveWithin drains the dependency edges that point at each seed task,
// applying each dependent's policy and promoting any dependent left unblocked.
// Cascade-cancel propagates through the worklist so a canceled dependent
// resolves its own dependents in turn. It returns the set of queues whose tasks
// became newly eligible. Every statement runs inside the caller's transaction so
// a partial reconciliation never commits.
func (b *Broker) resolveWithin(ctx context.Context, tx pgx.Tx, seed []string) (map[string]struct{}, error) {
	now := b.clock.Now()
	queues := make(map[string]struct{})
	seen := make(map[string]struct{})
	worklist := slices.Clone(seed)

	for len(worklist) > 0 {
		finishedID := worklist[0]
		worklist = worklist[1:]

		if _, done := seen[finishedID]; done {
			continue
		}

		seen[finishedID] = struct{}{}

		terminal, err := b.terminalState(ctx, tx, finishedID)
		if err != nil {
			return nil, err
		}

		succeeded := terminal == stateCompleted
		failed := terminal == stateArchived || terminal == stateCanceled

		if !succeeded && !failed {
			continue
		}

		edges, err := b.edgesWaitingOn(ctx, tx, finishedID)
		if err != nil {
			return nil, err
		}

		for _, edge := range edges {
			canceled, err := b.applyEdge(ctx, tx, finishedID, edge, failed, now)
			if err != nil {
				return nil, err
			}

			if canceled {
				worklist = append(worklist, edge.dependent)
				continue
			}

			queue, promoted, err := b.promoteDependent(ctx, tx, edge.dependent, now)
			if err != nil {
				return nil, err
			}

			if promoted {
				queues[queue] = struct{}{}
			}
		}
	}

	return queues, nil
}

// applyEdge reconciles one edge against a finished dependency. On dependency
// success, or failure under the continue policy, it drops the edge; under the
// block policy it leaves the dependent waiting; under cascade-cancel it cancels
// the dependent and reports that so the caller can propagate. It returns whether
// the dependent was canceled.
func (b *Broker) applyEdge(ctx context.Context, tx pgx.Tx, dependencyID string, edge dependencyEdge, failed bool, now time.Time) (bool, error) {
	if failed && edge.policy == int16(conveyorv1.DependencyFailurePolicy_DEPENDENCY_FAILURE_POLICY_BLOCK) {
		return false, nil
	}

	if failed && edge.policy == int16(conveyorv1.DependencyFailurePolicy_DEPENDENCY_FAILURE_POLICY_CASCADE_CANCEL) {
		if _, err := tx.Exec(ctx, cascadeCancelQuery, edge.dependent, now, broker.CascadeCanceledMessage); err != nil {
			return false, fmt.Errorf("postgres: cascade-cancel dependent: %w", err)
		}

		if _, err := tx.Exec(ctx, dropDependentEdgesQuery, edge.dependent); err != nil {
			return false, fmt.Errorf("postgres: drop canceled dependent edges: %w", err)
		}

		return true, nil
	}

	if _, err := tx.Exec(ctx, dropEdgeQuery, edge.dependent, dependencyID); err != nil {
		return false, fmt.Errorf("postgres: drop satisfied edge: %w", err)
	}

	return false, nil
}

// terminalState returns the task's current state, or stateBlocked's zero analog
// when the task is gone. An unknown task reports an unspecified state so the
// caller skips it.
func (b *Broker) terminalState(ctx context.Context, tx pgx.Tx, taskID string) (int16, error) {
	var state int16

	err := tx.QueryRow(ctx, "SELECT state FROM conveyor_tasks WHERE id = $1", taskID).Scan(&state)
	if errors.Is(err, pgx.ErrNoRows) {
		return int16(conveyorv1.TaskState_TASK_STATE_UNSPECIFIED), nil
	}

	if err != nil {
		return 0, fmt.Errorf("postgres: read task state: %w", err)
	}

	return state, nil
}

// edgesWaitingOn lists the edges that wait on a dependency.
func (b *Broker) edgesWaitingOn(ctx context.Context, tx pgx.Tx, dependencyID string) ([]dependencyEdge, error) {
	rows, err := tx.Query(ctx, edgesForDependencyQuery, dependencyID)
	if err != nil {
		return nil, fmt.Errorf("postgres: read waiting edges: %w", err)
	}
	defer rows.Close()

	var edges []dependencyEdge

	for rows.Next() {
		var edge dependencyEdge
		if err = rows.Scan(&edge.dependent, &edge.policy); err != nil {
			return nil, fmt.Errorf("postgres: scan waiting edge: %w", err)
		}

		edges = append(edges, edge)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: read waiting edges: %w", err)
	}

	return edges, nil
}

// promoteDependent promotes a blocked dependent once it holds no more edges,
// returning its queue and whether a promotion happened. It first locks the
// dependent row so concurrent resolvers of its sibling dependencies serialize,
// then re-checks the remaining edges in a fresh statement that observes their
// committed deletes — closing the lost-wakeup race on a concurrent fan-in join.
func (b *Broker) promoteDependent(ctx context.Context, tx pgx.Tx, dependentID string, now time.Time) (string, bool, error) {
	var locked string

	err := tx.QueryRow(ctx, lockBlockedDependentQuery, dependentID).Scan(&locked)
	if errors.Is(err, pgx.ErrNoRows) {
		// Not blocked: already promoted by a concurrent resolver, or never blocked.
		return "", false, nil
	}

	if err != nil {
		return "", false, fmt.Errorf("postgres: lock dependent: %w", err)
	}

	var queue string

	err = tx.QueryRow(ctx, promoteDependentQuery, dependentID, now).Scan(&queue)
	if errors.Is(err, pgx.ErrNoRows) {
		// Edges still remain: another dependency has yet to resolve.
		return "", false, nil
	}

	if err != nil {
		return "", false, fmt.Errorf("postgres: promote dependent: %w", err)
	}

	return queue, true, nil
}

// nullableTime maps the zero time to a NULL database value.
func nullableTime(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}

	return &t
}

// scanCronEntries materializes cron rows in the cronColumns projection.
func scanCronEntries(rows pgx.Rows) ([]*broker.CronEntry, error) {
	var entries []*broker.CronEntry

	for rows.Next() {
		var (
			entry       broker.CronEntry
			optionBytes []byte
			nextRunAt   *time.Time
		)

		err := rows.Scan(&entry.ID, &entry.Spec, &entry.TaskType, &entry.Queue,
			&entry.Payload, &entry.ContentType, &optionBytes, &entry.Paused, &nextRunAt)
		if err != nil {
			return nil, fmt.Errorf("postgres: scan cron entry: %w", err)
		}

		if nextRunAt != nil {
			entry.NextRunAt = *nextRunAt
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

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: list cron entries: %w", err)
	}

	return entries, nil
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

// stampExecutionTimes overlays the authoritative lease and terminal instants
// onto an envelope read from storage. Nil columns leave the fields unset.
func stampExecutionTimes(envelope *conveyorv1.TaskEnvelope, startedAt, completedAt *time.Time) {
	if startedAt != nil {
		envelope.StartedAt = timestamppb.New(*startedAt)
	}

	if completedAt != nil {
		envelope.CompletedAt = timestamppb.New(*completedAt)
	}
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
