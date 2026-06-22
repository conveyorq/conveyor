// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

// Package memory provides the in-memory Broker used for development,
// tests, and the embedded dev mode. It implements the exact semantics of
// the durable brokers and passes the same conformance suite, but holds
// everything in process memory.
package memory

import (
	"context"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/conveyorq/conveyor/internal/broker"
	"github.com/conveyorq/conveyor/internal/clock"
	"github.com/conveyorq/conveyor/internal/events"
	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
)

// taskRow is one stored task. The envelope is immutable after enqueue;
// execution state lives in the row fields and is overlaid on reads.
type taskRow struct {
	// envelope is the task as committed at enqueue time.
	envelope *conveyorv1.TaskEnvelope
	// state is the current lifecycle state.
	state conveyorv1.TaskState
	// priority orders dispatch within a queue, higher first.
	priority int32
	// processAt is when the task becomes due.
	processAt time.Time
	// expiresAt is the pre-dispatch expiry: a still-waiting task past it is
	// archived rather than leased. Zero means the task never expires.
	expiresAt time.Time
	// retried counts completed retry attempts.
	retried int32
	// lastError is the message of the most recent failure.
	lastError string
	// leaseID identifies the active lease, empty when not active.
	leaseID string
	// leaseExpiresAt is when the active lease lapses.
	leaseExpiresAt time.Time
	// startedAt is when the most recent execution attempt was leased. It is
	// reset on each re-lease so it reflects the current attempt.
	startedAt time.Time
	// completedAt is when the task reached a terminal state.
	completedAt time.Time
	// retention keeps the completed row for inspection before purge.
	retention time.Duration
	// uniqueKey is the uniqueness claim, empty when unclaimed or lapsed.
	uniqueKey string
	// uniqueExpiresAt bounds the claim; zero means until completion.
	uniqueExpiresAt time.Time
	// maxRetry caps retries before the reaper archives the task.
	maxRetry int32
	// group is the aggregation group key, empty for ungrouped tasks.
	group string
	// enqueuedAt is when the task was committed; orders group members and
	// drives the group's oldest/newest firing thresholds.
	enqueuedAt time.Time
	// deps maps each not-yet-satisfied dependency task id to the policy applied
	// when that dependency fails terminally. A non-empty map holds the row in
	// the blocked state; it drains to empty as dependencies succeed (or fail
	// under a continue policy), at which point the row is promoted.
	deps map[string]conveyorv1.DependencyFailurePolicy
}

// Broker is the in-memory broker.Broker implementation.
type Broker struct {
	// mutex guards all fields below.
	mutex sync.Mutex
	// clock supplies the current time.
	clock clock.Clock
	// tasks maps task id to its row.
	tasks map[string]*taskRow
	// pausedQueues holds the persisted queue pause flags.
	pausedQueues map[string]bool
	// rateLimits holds per-queue dispatch-rate overrides by queue name.
	rateLimits map[string]broker.RateLimit
	// concurrencyLimits holds per-queue per-key concurrency caps by queue name.
	concurrencyLimits map[string]broker.ConcurrencyLimit
	// cronEntries maps entry id to the stored entry.
	cronEntries map[string]*broker.CronEntry
	// dependents is the reverse dependency index: each dependency task id maps
	// to the set of blocked task ids waiting on it. It makes resolving a
	// finished task's dependents proportional to its dependent count rather than
	// the total task count, which matters because resolution runs on every
	// successful completion.
	dependents map[string]map[string]struct{}
	// sink receives lifecycle events on each state transition; nil until wired
	// by the server. It is a pointer to an interface so SetEventSink is race-free
	// against concurrent transitions.
	sink atomic.Pointer[events.Sink]
}

// SetEventSink wires the lifecycle-event sink. It is set once at startup, before
// the broker serves traffic; a nil or unset sink makes every emission a no-op.
func (b *Broker) SetEventSink(sink events.Sink) {
	b.sink.Store(&sink)
}

// emit derives and delivers the lifecycle event for a transition to the
// configured sink, if any. It builds the event only when a sink is wired, so a
// broker with events disabled does no per-transition allocation. The sink is
// non-blocking, so this is safe to call while holding the broker mutex.
func (b *Broker) emit(oldState, newState conveyorv1.TaskState, id, queue, taskType, lastError string, attempt int32, occurredAt time.Time) {
	sink := b.sink.Load()
	if sink == nil {
		return
	}

	if event := events.Derive(oldState, newState, id, queue, taskType, lastError, attempt, occurredAt); event != nil {
		(*sink).Emit(event)
	}
}

// enforce interface compliance at compile time.
var _ broker.Broker = (*Broker)(nil)

// New returns an empty in-memory broker reading time from the given clock.
func New(timeSource clock.Clock) *Broker {
	return &Broker{
		clock:             timeSource,
		tasks:             make(map[string]*taskRow),
		pausedQueues:      make(map[string]bool),
		rateLimits:        make(map[string]broker.RateLimit),
		concurrencyLimits: make(map[string]broker.ConcurrencyLimit),
		cronEntries:       make(map[string]*broker.CronEntry),
		dependents:        make(map[string]map[string]struct{}),
	}
}

// addDependent records that dependentID waits on dependencyID. Callers must
// hold the mutex.
func (b *Broker) addDependent(dependencyID, dependentID string) {
	waiters := b.dependents[dependencyID]
	if waiters == nil {
		waiters = make(map[string]struct{})
		b.dependents[dependencyID] = waiters
	}

	waiters[dependentID] = struct{}{}
}

// removeDependent clears the record that dependentID waits on dependencyID,
// dropping the dependency's entry once no waiter remains. Callers must hold the
// mutex.
func (b *Broker) removeDependent(dependencyID, dependentID string) {
	waiters := b.dependents[dependencyID]
	if waiters == nil {
		return
	}

	delete(waiters, dependentID)

	if len(waiters) == 0 {
		delete(b.dependents, dependencyID)
	}
}

// Enqueue durably commits a task; see broker.Broker.
func (b *Broker) Enqueue(_ context.Context, task *conveyorv1.TaskEnvelope) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	if _, exists := b.tasks[task.GetId()]; exists {
		return nil
	}

	now := b.clock.Now()
	options := task.GetOptions()
	uniqueKey := options.GetUniqueKey()

	if uniqueKey != "" && b.uniqueKeyClaimed(uniqueKey, now) {
		return broker.ErrDuplicateTask
	}

	processAt := now
	if options.GetProcessAt() != nil {
		processAt = options.GetProcessAt().AsTime()
	}

	group := options.GetGroup()
	if group != "" && processAt.After(now) {
		return broker.ErrGroupedSchedule
	}

	enqueuedAt := now
	if task.GetEnqueuedAt() != nil {
		enqueuedAt = task.GetEnqueuedAt().AsTime()
	}

	state := conveyorv1.TaskState_TASK_STATE_PENDING

	switch {
	case group != "":
		state = conveyorv1.TaskState_TASK_STATE_AGGREGATING
	case processAt.After(now):
		state = conveyorv1.TaskState_TASK_STATE_SCHEDULED
	}

	deps, cancel := b.resolveInitialDeps(task.GetId(), options.GetDependsOn())

	switch {
	case cancel:
		state = conveyorv1.TaskState_TASK_STATE_CANCELED
	case len(deps) > 0:
		state = conveyorv1.TaskState_TASK_STATE_BLOCKED
	}

	var uniqueExpiresAt time.Time
	if uniqueKey != "" && options.GetUniqueTtl() != nil {
		uniqueExpiresAt = now.Add(options.GetUniqueTtl().AsDuration())
	}

	var expiresAt time.Time
	if options.GetExpiresAt() != nil {
		expiresAt = options.GetExpiresAt().AsTime()
	}

	row := &taskRow{
		envelope:        proto.Clone(task).(*conveyorv1.TaskEnvelope),
		state:           state,
		priority:        options.GetPriority(),
		processAt:       processAt,
		expiresAt:       expiresAt,
		retried:         task.GetRetried(),
		lastError:       task.GetLastError(),
		retention:       options.GetRetention().AsDuration(),
		uniqueKey:       uniqueKey,
		uniqueExpiresAt: uniqueExpiresAt,
		maxRetry:        options.GetMaxRetry(),
		group:           group,
		enqueuedAt:      enqueuedAt,
		deps:            deps,
	}

	if state == conveyorv1.TaskState_TASK_STATE_CANCELED {
		row.completedAt = now
	}

	for dependencyID := range deps {
		b.addDependent(dependencyID, task.GetId())
	}

	b.tasks[task.GetId()] = row

	b.emit(conveyorv1.TaskState_TASK_STATE_UNSPECIFIED, state,
		task.GetId(), task.GetQueue(), task.GetType(), row.lastError, row.retried, now)

	return nil
}

// resolveInitialDeps computes the unsatisfied dependencies of a task being
// enqueued. A dependency already completed is satisfied and dropped; one that
// already failed terminally is applied through its policy (continue drops it,
// block keeps it, cascade-cancel marks the new task for immediate cancellation);
// every other dependency — pending, running, or not yet enqueued — is retained
// as a block. Callers must hold the mutex.
func (b *Broker) resolveInitialDeps(taskID string, edges []*conveyorv1.TaskDependency) (map[string]conveyorv1.DependencyFailurePolicy, bool) {
	if len(edges) == 0 {
		return nil, false
	}

	deps := make(map[string]conveyorv1.DependencyFailurePolicy)

	for _, edge := range edges {
		depID := edge.GetTaskId()
		if depID == "" || depID == taskID {
			continue
		}

		policy := dependencyPolicy(edge.GetOnFailure())
		dep, exists := b.tasks[depID]

		switch {
		case !exists:
			deps[depID] = policy
		case dep.state == conveyorv1.TaskState_TASK_STATE_COMPLETED:
			// already satisfied
		case dep.state == conveyorv1.TaskState_TASK_STATE_ARCHIVED || dep.state == conveyorv1.TaskState_TASK_STATE_CANCELED:
			switch policy {
			case conveyorv1.DependencyFailurePolicy_DEPENDENCY_FAILURE_POLICY_CONTINUE:
				// failed dependency treated as satisfied
			case conveyorv1.DependencyFailurePolicy_DEPENDENCY_FAILURE_POLICY_CASCADE_CANCEL:
				return nil, true
			default:
				deps[depID] = policy
			}
		default:
			deps[depID] = policy
		}
	}

	if len(deps) == 0 {
		return nil, false
	}

	return deps, false
}

// dependencyPolicy resolves the unspecified policy to its block default.
func dependencyPolicy(policy conveyorv1.DependencyFailurePolicy) conveyorv1.DependencyFailurePolicy {
	if policy == conveyorv1.DependencyFailurePolicy_DEPENDENCY_FAILURE_POLICY_UNSPECIFIED {
		return conveyorv1.DependencyFailurePolicy_DEPENDENCY_FAILURE_POLICY_BLOCK
	}

	return policy
}

// uniqueKeyClaimed reports whether an incomplete task holds the key and the
// claim has not lapsed. Callers must hold the mutex.
func (b *Broker) uniqueKeyClaimed(key string, now time.Time) bool {
	for _, row := range b.tasks {
		if row.uniqueKey != key || !incomplete(row.state) {
			continue
		}

		if row.uniqueExpiresAt.IsZero() || row.uniqueExpiresAt.After(now) {
			return true
		}
	}

	return false
}

// incomplete reports whether the state still holds a uniqueness claim.
func incomplete(state conveyorv1.TaskState) bool {
	switch state {
	case conveyorv1.TaskState_TASK_STATE_SCHEDULED,
		conveyorv1.TaskState_TASK_STATE_PENDING,
		conveyorv1.TaskState_TASK_STATE_ACTIVE,
		conveyorv1.TaskState_TASK_STATE_RETRY,
		conveyorv1.TaskState_TASK_STATE_AGGREGATING,
		conveyorv1.TaskState_TASK_STATE_BLOCKED:
		return true
	default:
		return false
	}
}

// Lease atomically claims up to limit due tasks; see broker.Broker.
func (b *Broker) Lease(_ context.Context, queue string, limit int, ttl time.Duration, leaseID string) ([]*conveyorv1.TaskEnvelope, error) {
	if limit <= 0 {
		return nil, nil
	}

	b.mutex.Lock()
	defer b.mutex.Unlock()

	now := b.clock.Now()

	var due []*taskRow

	for _, row := range b.tasks {
		if row.envelope.GetQueue() == queue && dispatchable(row.state) && !row.processAt.After(now) && !expired(row, now) {
			due = append(due, row)
		}
	}

	slices.SortFunc(due, func(a, other *taskRow) int {
		if a.priority != other.priority {
			return int(other.priority - a.priority)
		}

		if !a.processAt.Equal(other.processAt) {
			return a.processAt.Compare(other.processAt)
		}

		return strings.Compare(a.envelope.GetId(), other.envelope.GetId())
	})

	if len(due) > limit {
		due = due[:limit]
	}

	leased := make([]*conveyorv1.TaskEnvelope, 0, len(due))

	for _, row := range due {
		oldState := row.state
		row.state = conveyorv1.TaskState_TASK_STATE_ACTIVE
		row.leaseID = leaseID
		row.leaseExpiresAt = now.Add(ttl)
		row.startedAt = now
		leased = append(leased, overlay(row))
		b.emit(oldState, row.state, row.envelope.GetId(), row.envelope.GetQueue(), row.envelope.GetType(), row.lastError, row.retried, now)
	}

	return leased, nil
}

// LeaseGroup claims a group's aggregating members as one batch; see broker.Broker.
func (b *Broker) LeaseGroup(_ context.Context, queue, group string, limit int, ttl time.Duration, leaseID string) ([]*conveyorv1.TaskEnvelope, error) {
	if limit <= 0 {
		return nil, nil
	}

	b.mutex.Lock()
	defer b.mutex.Unlock()

	now := b.clock.Now()

	var members []*taskRow

	for _, row := range b.tasks {
		if row.state == conveyorv1.TaskState_TASK_STATE_AGGREGATING && row.envelope.GetQueue() == queue && row.group == group {
			members = append(members, row)
		}
	}

	slices.SortFunc(members, func(a, other *taskRow) int {
		if !a.enqueuedAt.Equal(other.enqueuedAt) {
			return a.enqueuedAt.Compare(other.enqueuedAt)
		}

		return strings.Compare(a.envelope.GetId(), other.envelope.GetId())
	})

	if len(members) > limit {
		members = members[:limit]
	}

	leased := make([]*conveyorv1.TaskEnvelope, 0, len(members))

	for _, row := range members {
		oldState := row.state
		row.state = conveyorv1.TaskState_TASK_STATE_ACTIVE
		row.leaseID = leaseID
		row.leaseExpiresAt = now.Add(ttl)
		row.startedAt = now
		leased = append(leased, overlay(row))
		b.emit(oldState, row.state, row.envelope.GetId(), row.envelope.GetQueue(), row.envelope.GetType(), row.lastError, row.retried, now)
	}

	return leased, nil
}

// GroupStats summarizes the aggregating groups across all queues; see
// broker.Broker.
func (b *Broker) GroupStats(_ context.Context) ([]broker.GroupStat, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	type groupKey struct {
		queue string
		group string
	}

	stats := make(map[groupKey]*broker.GroupStat)

	for _, row := range b.tasks {
		if row.state != conveyorv1.TaskState_TASK_STATE_AGGREGATING {
			continue
		}

		key := groupKey{queue: row.envelope.GetQueue(), group: row.group}

		stat, ok := stats[key]
		if !ok {
			stat = &broker.GroupStat{
				Queue:  key.queue,
				Group:  key.group,
				Type:   row.envelope.GetType(),
				Oldest: row.enqueuedAt,
				Newest: row.enqueuedAt,
			}
			stats[key] = stat
		}

		stat.Count++

		if row.enqueuedAt.Before(stat.Oldest) {
			stat.Oldest = row.enqueuedAt
		}

		if row.enqueuedAt.After(stat.Newest) {
			stat.Newest = row.enqueuedAt
		}
	}

	result := make([]broker.GroupStat, 0, len(stats))
	for _, stat := range stats {
		result = append(result, *stat)
	}

	slices.SortFunc(result, func(a, other broker.GroupStat) int {
		if a.Queue != other.Queue {
			return strings.Compare(a.Queue, other.Queue)
		}

		return strings.Compare(a.Group, other.Group)
	})

	return result, nil
}

// dispatchable reports whether the state is eligible for leasing.
func dispatchable(state conveyorv1.TaskState) bool {
	return state == conveyorv1.TaskState_TASK_STATE_PENDING ||
		state == conveyorv1.TaskState_TASK_STATE_RETRY
}

// overlay clones the stored envelope and stamps the mutable execution
// fields onto it. Callers must hold the mutex.
func overlay(row *taskRow) *conveyorv1.TaskEnvelope {
	envelope := proto.Clone(row.envelope).(*conveyorv1.TaskEnvelope)
	envelope.Retried = row.retried
	envelope.LastError = row.lastError

	if !row.startedAt.IsZero() {
		envelope.StartedAt = timestamppb.New(row.startedAt)
	}

	if !row.completedAt.IsZero() {
		envelope.CompletedAt = timestamppb.New(row.completedAt)
	}

	return envelope
}

// activeRow returns the row only when it is active under leaseID. Callers
// must hold the mutex.
func (b *Broker) activeRow(taskID, leaseID string) (*taskRow, error) {
	row, exists := b.tasks[taskID]
	if !exists {
		return nil, broker.ErrLeaseLost
	}

	if row.state != conveyorv1.TaskState_TASK_STATE_ACTIVE || row.leaseID != leaseID {
		return nil, broker.ErrLeaseLost
	}

	return row, nil
}

// ExtendLease pushes the lease expiry forward; see broker.Broker.
func (b *Broker) ExtendLease(_ context.Context, taskID, leaseID string, ttl time.Duration) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	row, err := b.activeRow(taskID, leaseID)
	if err != nil {
		return err
	}

	row.leaseExpiresAt = b.clock.Now().Add(ttl)

	return nil
}

// Ack completes an active task; see broker.Broker. The worker-reported result
// is discarded: the in-memory broker, like the Broker interface, exposes no
// result-read path.
func (b *Broker) Ack(_ context.Context, taskID, leaseID string, _ []byte) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	row, err := b.activeRow(taskID, leaseID)
	if err != nil {
		return err
	}

	now := b.clock.Now()
	oldState := row.state
	row.state = conveyorv1.TaskState_TASK_STATE_COMPLETED
	row.completedAt = now
	row.leaseID = ""

	b.emit(oldState, row.state, taskID, row.envelope.GetQueue(), row.envelope.GetType(), row.lastError, row.retried, now)

	return nil
}

// Fail records a failed attempt and schedules the retry; see broker.Broker.
func (b *Broker) Fail(_ context.Context, taskID, leaseID, errMsg string, processAt time.Time) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	row, err := b.activeRow(taskID, leaseID)
	if err != nil {
		return err
	}

	oldState := row.state
	row.state = conveyorv1.TaskState_TASK_STATE_RETRY
	row.retried++
	row.lastError = errMsg
	row.processAt = processAt
	row.leaseID = ""

	b.emit(oldState, row.state, taskID, row.envelope.GetQueue(), row.envelope.GetType(), row.lastError, row.retried, b.clock.Now())

	return nil
}

// Release returns an active task to pending without a retry penalty; see
// broker.Broker.
func (b *Broker) Release(_ context.Context, taskID, leaseID string) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	row, err := b.activeRow(taskID, leaseID)
	if err != nil {
		return err
	}

	now := b.clock.Now()
	oldState := row.state
	row.state = conveyorv1.TaskState_TASK_STATE_PENDING
	row.processAt = now
	row.leaseID = ""

	b.emit(oldState, row.state, taskID, row.envelope.GetQueue(), row.envelope.GetType(), row.lastError, row.retried, now)

	return nil
}

// Archive dead-letters a task; see broker.Broker.
func (b *Broker) Archive(_ context.Context, taskID, leaseID, errMsg string) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	var row *taskRow

	if leaseID != "" {
		activeRow, err := b.activeRow(taskID, leaseID)
		if err != nil {
			return err
		}

		row = activeRow
	} else {
		storedRow, exists := b.tasks[taskID]
		if !exists {
			return broker.ErrTaskNotFound
		}

		if storedRow.state == conveyorv1.TaskState_TASK_STATE_COMPLETED {
			return broker.ErrInvalidState
		}

		row = storedRow
	}

	now := b.clock.Now()
	oldState := row.state
	row.state = conveyorv1.TaskState_TASK_STATE_ARCHIVED
	row.lastError = errMsg
	row.completedAt = now
	row.leaseID = ""

	b.emit(oldState, row.state, taskID, row.envelope.GetQueue(), row.envelope.GetType(), row.lastError, row.retried, now)

	return nil
}

// ReapExpiredLeases reclaims lapsed leases; see broker.Broker.
func (b *Broker) ReapExpiredLeases(_ context.Context, limit int) ([]string, error) {
	if limit <= 0 {
		return nil, nil
	}

	b.mutex.Lock()
	defer b.mutex.Unlock()

	now := b.clock.Now()
	queues := make(map[string]struct{})
	reaped := 0

	for _, row := range b.tasks {
		if reaped == limit {
			break
		}

		if row.state != conveyorv1.TaskState_TASK_STATE_ACTIVE || row.leaseExpiresAt.After(now) {
			continue
		}

		row.leaseID = ""
		row.lastError = broker.LeaseExpiredMessage
		oldState := row.state

		if row.retried >= row.maxRetry {
			row.state = conveyorv1.TaskState_TASK_STATE_ARCHIVED
			row.completedAt = now
		} else {
			row.state = conveyorv1.TaskState_TASK_STATE_RETRY
			row.retried++
			row.processAt = now
			queues[row.envelope.GetQueue()] = struct{}{}
		}

		b.emit(oldState, row.state, row.envelope.GetId(), row.envelope.GetQueue(), row.envelope.GetType(), row.lastError, row.retried, now)

		reaped++
	}

	return slices.Collect(maps.Keys(queues)), nil
}

// PromoteScheduled moves due scheduled tasks to pending; see broker.Broker.
func (b *Broker) PromoteScheduled(_ context.Context, limit int) ([]string, error) {
	if limit <= 0 {
		return nil, nil
	}

	b.mutex.Lock()
	defer b.mutex.Unlock()

	now := b.clock.Now()
	queues := make(map[string]struct{})
	promoted := 0

	for _, row := range b.tasks {
		if promoted == limit {
			break
		}

		if row.state != conveyorv1.TaskState_TASK_STATE_SCHEDULED || row.processAt.After(now) {
			continue
		}

		oldState := row.state
		row.state = conveyorv1.TaskState_TASK_STATE_PENDING
		queues[row.envelope.GetQueue()] = struct{}{}
		b.emit(oldState, row.state, row.envelope.GetId(), row.envelope.GetQueue(), row.envelope.GetType(), row.lastError, row.retried, now)
		promoted++
	}

	return slices.Collect(maps.Keys(queues)), nil
}

// ResolveDependents reconciles edges pointing at a terminal task; see
// broker.Broker.
func (b *Broker) ResolveDependents(_ context.Context, taskID string) ([]string, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	return b.resolveDependents(taskID, b.clock.Now()), nil
}

// resolveDependents drains the dependency edges that point at the given
// terminal task, applying each dependent's policy and promoting any dependent
// left unblocked. Cascade-cancel propagates through a worklist so a canceled
// dependent resolves its own dependents in turn. It returns the distinct queues
// whose tasks became newly eligible. Callers must hold the mutex.
func (b *Broker) resolveDependents(taskID string, now time.Time) []string {
	queues := make(map[string]struct{})
	seen := make(map[string]struct{})
	worklist := []string{taskID}

	for len(worklist) > 0 {
		finishedID := worklist[0]
		worklist = worklist[1:]

		if _, done := seen[finishedID]; done {
			continue
		}

		seen[finishedID] = struct{}{}

		finished, exists := b.tasks[finishedID]
		if !exists {
			continue
		}

		succeeded := finished.state == conveyorv1.TaskState_TASK_STATE_COMPLETED
		failed := finished.state == conveyorv1.TaskState_TASK_STATE_ARCHIVED || finished.state == conveyorv1.TaskState_TASK_STATE_CANCELED

		if !succeeded && !failed {
			continue
		}

		// Iterate a snapshot of the waiting set: the loop mutates the reverse
		// index as it drains satisfied edges.
		for _, dependentID := range slices.Collect(maps.Keys(b.dependents[finishedID])) {
			dependent, exists := b.tasks[dependentID]
			if !exists {
				b.removeDependent(finishedID, dependentID)

				continue
			}

			policy, waiting := dependent.deps[finishedID]
			if !waiting {
				b.removeDependent(finishedID, dependentID)

				continue
			}

			if failed && policy == conveyorv1.DependencyFailurePolicy_DEPENDENCY_FAILURE_POLICY_BLOCK {
				continue
			}

			if failed && policy == conveyorv1.DependencyFailurePolicy_DEPENDENCY_FAILURE_POLICY_CASCADE_CANCEL {
				oldState := dependent.state
				dependent.state = conveyorv1.TaskState_TASK_STATE_CANCELED
				dependent.completedAt = now
				dependent.lastError = broker.CascadeCanceledMessage

				for dependencyID := range dependent.deps {
					b.removeDependent(dependencyID, dependentID)
				}

				dependent.deps = nil
				b.emit(oldState, dependent.state, dependentID, dependent.envelope.GetQueue(), dependent.envelope.GetType(), dependent.lastError, dependent.retried, now)
				worklist = append(worklist, dependentID)

				continue
			}

			delete(dependent.deps, finishedID)
			b.removeDependent(finishedID, dependentID)

			if queue, promoted := b.promoteIfReady(dependent, now); promoted {
				queues[queue] = struct{}{}
			}
		}
	}

	return slices.Collect(maps.Keys(queues))
}

// promoteIfReady moves a blocked row with no remaining dependencies to the
// state it would have held without dependencies: aggregating when grouped,
// scheduled when its process_at is still in the future, otherwise pending. It
// returns the row's queue and true when a promotion happened. Callers must hold
// the mutex.
func (b *Broker) promoteIfReady(row *taskRow, now time.Time) (string, bool) {
	if row.state != conveyorv1.TaskState_TASK_STATE_BLOCKED || len(row.deps) > 0 {
		return "", false
	}

	oldState := row.state

	switch {
	case row.group != "":
		row.state = conveyorv1.TaskState_TASK_STATE_AGGREGATING
	case row.processAt.After(now):
		row.state = conveyorv1.TaskState_TASK_STATE_SCHEDULED
	default:
		row.state = conveyorv1.TaskState_TASK_STATE_PENDING
	}

	b.emit(oldState, row.state, row.envelope.GetId(), row.envelope.GetQueue(), row.envelope.GetType(), row.lastError, row.retried, now)

	return row.envelope.GetQueue(), true
}

// PromoteReadyDependents reconciles blocked tasks whose dependencies have since
// gone terminal; see broker.Broker.
func (b *Broker) PromoteReadyDependents(_ context.Context, limit int) ([]string, error) {
	if limit <= 0 {
		return nil, nil
	}

	b.mutex.Lock()
	defer b.mutex.Unlock()

	now := b.clock.Now()
	finished := make(map[string]struct{})

	for _, row := range b.tasks {
		if row.state != conveyorv1.TaskState_TASK_STATE_BLOCKED {
			continue
		}

		for depID := range row.deps {
			if dep, exists := b.tasks[depID]; exists && terminal(dep.state) {
				finished[depID] = struct{}{}
			}
		}
	}

	queues := make(map[string]struct{})
	resolved := 0

	for depID := range finished {
		if resolved == limit {
			break
		}

		for _, queue := range b.resolveDependents(depID, now) {
			queues[queue] = struct{}{}
		}

		resolved++
	}

	// Backstop: promote any task left blocked with no remaining dependencies,
	// mirroring the Postgres sweep that recovers a dependent stranded by an
	// interrupted resolution.
	for _, row := range b.tasks {
		if queue, promoted := b.promoteIfReady(row, now); promoted {
			queues[queue] = struct{}{}
		}
	}

	return slices.Collect(maps.Keys(queues)), nil
}

// terminal reports whether the state is a terminal outcome a dependent reacts
// to: a success or a failure.
func terminal(state conveyorv1.TaskState) bool {
	switch state {
	case conveyorv1.TaskState_TASK_STATE_COMPLETED,
		conveyorv1.TaskState_TASK_STATE_ARCHIVED,
		conveyorv1.TaskState_TASK_STATE_CANCELED:
		return true
	default:
		return false
	}
}

// hasDependents reports whether any stored task is still blocked on the given
// task id. Callers must hold the mutex.
func (b *Broker) hasDependents(taskID string) bool {
	return len(b.dependents[taskID]) > 0
}

// PurgeCompleted removes retention-expired completed tasks and lapsed
// unique-key claims; see broker.Broker.
func (b *Broker) PurgeCompleted(_ context.Context, limit int) (int, error) {
	if limit <= 0 {
		return 0, nil
	}

	b.mutex.Lock()
	defer b.mutex.Unlock()

	now := b.clock.Now()
	purged := 0

	for id, row := range b.tasks {
		if row.uniqueKey != "" && !row.uniqueExpiresAt.IsZero() && !row.uniqueExpiresAt.After(now) {
			row.uniqueKey = ""
		}

		if purged == limit {
			continue
		}

		if row.state == conveyorv1.TaskState_TASK_STATE_COMPLETED && !row.completedAt.Add(row.retention).After(now) && !b.hasDependents(id) {
			delete(b.tasks, id)
			purged++
		}
	}

	return purged, nil
}

// expired reports whether the row carries a pre-dispatch expiry that has
// passed by now. A zero expiry never expires.
func expired(row *taskRow, now time.Time) bool {
	return !row.expiresAt.IsZero() && !row.expiresAt.After(now)
}

// ArchiveExpired dead-letters still-waiting tasks past their expiry; see broker.Broker.
func (b *Broker) ArchiveExpired(_ context.Context, limit int) (int, error) {
	if limit <= 0 {
		return 0, nil
	}

	b.mutex.Lock()
	defer b.mutex.Unlock()

	now := b.clock.Now()
	archived := 0

	for _, row := range b.tasks {
		if archived == limit {
			break
		}

		if !dispatchable(row.state) && row.state != conveyorv1.TaskState_TASK_STATE_SCHEDULED && row.state != conveyorv1.TaskState_TASK_STATE_BLOCKED {
			continue
		}

		if !expired(row, now) {
			continue
		}

		oldState := row.state
		row.state = conveyorv1.TaskState_TASK_STATE_ARCHIVED
		row.lastError = broker.TaskExpiredMessage
		row.completedAt = now
		row.leaseID = ""
		b.emit(oldState, row.state, row.envelope.GetId(), row.envelope.GetQueue(), row.envelope.GetType(), row.lastError, row.retried, now)
		archived++
	}

	return archived, nil
}

// PendingCount counts due tasks per queue; see broker.Broker.
func (b *Broker) PendingCount(_ context.Context) (map[string]int64, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	now := b.clock.Now()
	counts := make(map[string]int64)

	for _, row := range b.tasks {
		if dispatchable(row.state) && !row.processAt.After(now) {
			counts[row.envelope.GetQueue()]++
		}
	}

	return counts, nil
}

// QueueStats aggregates task counts and pause flags per queue; see
// broker.Broker.
func (b *Broker) QueueStats(_ context.Context) ([]broker.QueueStat, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	stats := make(map[string]*broker.QueueStat)

	queueStat := func(queue string) *broker.QueueStat {
		stat, exists := stats[queue]

		if !exists {
			stat = &broker.QueueStat{Queue: queue, Paused: b.pausedQueues[queue]}
			stats[queue] = stat
		}

		return stat
	}

	for queue := range b.pausedQueues {
		queueStat(queue)
	}

	for _, row := range b.tasks {
		stat := queueStat(row.envelope.GetQueue())

		switch row.state {
		case conveyorv1.TaskState_TASK_STATE_SCHEDULED:
			stat.Scheduled++
		case conveyorv1.TaskState_TASK_STATE_PENDING:
			stat.Pending++
		case conveyorv1.TaskState_TASK_STATE_ACTIVE:
			stat.Active++
		case conveyorv1.TaskState_TASK_STATE_RETRY:
			stat.Retry++
		case conveyorv1.TaskState_TASK_STATE_COMPLETED:
			stat.Completed++
		case conveyorv1.TaskState_TASK_STATE_ARCHIVED:
			stat.Archived++
		case conveyorv1.TaskState_TASK_STATE_AGGREGATING:
			stat.Aggregating++
		case conveyorv1.TaskState_TASK_STATE_BLOCKED:
			stat.Blocked++
		}
	}

	ordered := make([]broker.QueueStat, 0, len(stats))
	for _, stat := range stats {
		ordered = append(ordered, *stat)
	}

	slices.SortFunc(ordered, func(a, other broker.QueueStat) int {
		return strings.Compare(a.Queue, other.Queue)
	})

	return ordered, nil
}

// SetQueuePaused persists the queue pause flag; see broker.Broker.
func (b *Broker) SetQueuePaused(_ context.Context, queue string, paused bool) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	b.pausedQueues[queue] = paused

	return nil
}

// QueuePaused reports the queue pause flag; see broker.Broker.
func (b *Broker) QueuePaused(_ context.Context, queue string) (bool, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	return b.pausedQueues[queue], nil
}

// SetQueueRateLimit persists a per-queue dispatch-rate override; see broker.Broker.
func (b *Broker) SetQueueRateLimit(_ context.Context, queue string, ratePerSec float64, burst int) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	b.rateLimits[queue] = broker.RateLimit{
		Queue:      queue,
		RatePerSec: ratePerSec,
		Burst:      burst,
		UpdatedAt:  b.clock.Now(),
	}

	return nil
}

// DeleteQueueRateLimit removes a queue's override; see broker.Broker.
func (b *Broker) DeleteQueueRateLimit(_ context.Context, queue string) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	delete(b.rateLimits, queue)

	return nil
}

// QueueRateLimit returns a queue's override and whether one is set; see broker.Broker.
func (b *Broker) QueueRateLimit(_ context.Context, queue string) (broker.RateLimit, bool, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	limit, ok := b.rateLimits[queue]

	return limit, ok, nil
}

// QueueRateLimits returns every override ordered by queue name; see broker.Broker.
func (b *Broker) QueueRateLimits(_ context.Context) ([]broker.RateLimit, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	limits := make([]broker.RateLimit, 0, len(b.rateLimits))
	for _, limit := range b.rateLimits {
		limits = append(limits, limit)
	}

	slices.SortFunc(limits, func(a, b broker.RateLimit) int {
		return strings.Compare(a.Queue, b.Queue)
	})

	return limits, nil
}

// SetQueueConcurrencyLimit persists a per-queue per-key concurrency cap; see broker.Broker.
func (b *Broker) SetQueueConcurrencyLimit(_ context.Context, queue string, maxActive int) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	b.concurrencyLimits[queue] = broker.ConcurrencyLimit{
		Queue:     queue,
		MaxActive: maxActive,
		UpdatedAt: b.clock.Now(),
	}

	return nil
}

// DeleteQueueConcurrencyLimit removes a queue's concurrency limit; see broker.Broker.
func (b *Broker) DeleteQueueConcurrencyLimit(_ context.Context, queue string) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	delete(b.concurrencyLimits, queue)

	return nil
}

// QueueConcurrencyLimit returns a queue's limit and whether one is set; see broker.Broker.
func (b *Broker) QueueConcurrencyLimit(_ context.Context, queue string) (broker.ConcurrencyLimit, bool, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	limit, ok := b.concurrencyLimits[queue]

	return limit, ok, nil
}

// QueueConcurrencyLimits returns every limit ordered by queue name; see broker.Broker.
func (b *Broker) QueueConcurrencyLimits(_ context.Context) ([]broker.ConcurrencyLimit, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	limits := make([]broker.ConcurrencyLimit, 0, len(b.concurrencyLimits))
	for _, limit := range b.concurrencyLimits {
		limits = append(limits, limit)
	}

	slices.SortFunc(limits, func(a, b broker.ConcurrencyLimit) int {
		return strings.Compare(a.Queue, b.Queue)
	})

	return limits, nil
}

// Info reports the in-memory engine's driver and row counts; see broker.Broker.
func (b *Broker) Info(_ context.Context) (broker.Info, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	return broker.Info{
		Driver: "memory",
		Metrics: map[string]string{
			"tasks":              strconv.Itoa(len(b.tasks)),
			"cron_entries":       strconv.Itoa(len(b.cronEntries)),
			"paused_queues":      strconv.Itoa(len(b.pausedQueues)),
			"rate_limits":        strconv.Itoa(len(b.rateLimits)),
			"concurrency_limits": strconv.Itoa(len(b.concurrencyLimits)),
		},
	}, nil
}

// GetTask returns one task and its state; see broker.Broker.
func (b *Broker) GetTask(_ context.Context, id string) (*conveyorv1.TaskEnvelope, conveyorv1.TaskState, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	row, exists := b.tasks[id]
	if !exists {
		return nil, conveyorv1.TaskState_TASK_STATE_UNSPECIFIED, broker.ErrTaskNotFound
	}

	return overlay(row), row.state, nil
}

// ListTasks returns tasks matching the query; see broker.Broker.
func (b *Broker) ListTasks(_ context.Context, query broker.TaskQuery) ([]broker.TaskRecord, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	limit := broker.EffectiveListLimit(query.Limit)

	var matched []*taskRow

	for _, row := range b.tasks {
		if query.Queue != "" && row.envelope.GetQueue() != query.Queue {
			continue
		}

		if query.State != conveyorv1.TaskState_TASK_STATE_UNSPECIFIED && row.state != query.State {
			continue
		}

		if query.AfterID != "" && row.envelope.GetId() >= query.AfterID {
			continue
		}

		matched = append(matched, row)
	}

	slices.SortFunc(matched, func(a, other *taskRow) int {
		return strings.Compare(other.envelope.GetId(), a.envelope.GetId())
	})

	if len(matched) > limit {
		matched = matched[:limit]
	}

	records := make([]broker.TaskRecord, 0, len(matched))
	for _, row := range matched {
		records = append(records, broker.TaskRecord{Envelope: overlay(row), State: row.state})
	}

	return records, nil
}

// CancelTask cancels a not-yet-running task; see broker.Broker.
func (b *Broker) CancelTask(_ context.Context, id string) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	row, exists := b.tasks[id]
	if !exists {
		return broker.ErrTaskNotFound
	}

	switch row.state {
	case conveyorv1.TaskState_TASK_STATE_SCHEDULED,
		conveyorv1.TaskState_TASK_STATE_PENDING,
		conveyorv1.TaskState_TASK_STATE_RETRY,
		conveyorv1.TaskState_TASK_STATE_AGGREGATING,
		conveyorv1.TaskState_TASK_STATE_BLOCKED:
		now := b.clock.Now()
		oldState := row.state
		row.state = conveyorv1.TaskState_TASK_STATE_CANCELED
		row.completedAt = now

		b.emit(oldState, row.state, id, row.envelope.GetQueue(), row.envelope.GetType(), row.lastError, row.retried, now)

		return nil
	default:
		return broker.ErrInvalidState
	}
}

// DeleteTask removes a non-active task; see broker.Broker.
func (b *Broker) DeleteTask(_ context.Context, id string) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	row, exists := b.tasks[id]
	if !exists {
		return broker.ErrTaskNotFound
	}

	if row.state == conveyorv1.TaskState_TASK_STATE_ACTIVE {
		return broker.ErrInvalidState
	}

	// Drop the deleted task's outgoing edges from the reverse index, mirroring
	// the Postgres DeleteTask, so no dependency keeps a stale waiter.
	for dependencyID := range row.deps {
		b.removeDependent(dependencyID, id)
	}

	delete(b.tasks, id)

	return nil
}

// RunTaskNow makes a task due immediately; see broker.Broker.
func (b *Broker) RunTaskNow(_ context.Context, id string) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	row, exists := b.tasks[id]
	if !exists {
		return broker.ErrTaskNotFound
	}

	switch row.state {
	case conveyorv1.TaskState_TASK_STATE_SCHEDULED,
		conveyorv1.TaskState_TASK_STATE_PENDING,
		conveyorv1.TaskState_TASK_STATE_RETRY,
		conveyorv1.TaskState_TASK_STATE_ARCHIVED:
		now := b.clock.Now()
		oldState := row.state
		row.state = conveyorv1.TaskState_TASK_STATE_PENDING
		row.processAt = now
		row.completedAt = time.Time{}

		// A pending task made due again is a no-op state-wise and carries no event.
		if oldState != row.state {
			b.emit(oldState, row.state, id, row.envelope.GetQueue(), row.envelope.GetType(), row.lastError, row.retried, now)
		}

		return nil
	default:
		return broker.ErrInvalidState
	}
}

// ArchiveTask dead-letters a waiting task; see broker.Broker.
func (b *Broker) ArchiveTask(_ context.Context, id string) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	row, exists := b.tasks[id]
	if !exists {
		return broker.ErrTaskNotFound
	}

	switch row.state {
	case conveyorv1.TaskState_TASK_STATE_SCHEDULED,
		conveyorv1.TaskState_TASK_STATE_PENDING,
		conveyorv1.TaskState_TASK_STATE_RETRY,
		conveyorv1.TaskState_TASK_STATE_BLOCKED:
		now := b.clock.Now()
		oldState := row.state
		row.state = conveyorv1.TaskState_TASK_STATE_ARCHIVED
		row.completedAt = now

		b.emit(oldState, row.state, id, row.envelope.GetQueue(), row.envelope.GetType(), row.lastError, row.retried, now)

		return nil
	default:
		return broker.ErrInvalidState
	}
}

// UpsertCronEntry creates or replaces a cron entry; see broker.Broker.
func (b *Broker) UpsertCronEntry(_ context.Context, entry *broker.CronEntry) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	b.cronEntries[entry.ID] = cloneCronEntry(entry)

	return nil
}

// ListCronEntries returns all cron entries ordered by id; see broker.Broker.
func (b *Broker) ListCronEntries(_ context.Context) ([]*broker.CronEntry, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	entries := make([]*broker.CronEntry, 0, len(b.cronEntries))
	for _, entry := range b.cronEntries {
		entries = append(entries, cloneCronEntry(entry))
	}

	slices.SortFunc(entries, func(a, other *broker.CronEntry) int {
		return strings.Compare(a.ID, other.ID)
	})

	return entries, nil
}

// ListDueCronEntries returns the non-paused entries due to fire; see
// broker.Broker.
func (b *Broker) ListDueCronEntries(_ context.Context, now time.Time) ([]*broker.CronEntry, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	var entries []*broker.CronEntry

	for _, entry := range b.cronEntries {
		if entry.Paused {
			continue
		}

		if entry.NextRunAt.IsZero() || !entry.NextRunAt.After(now) {
			entries = append(entries, cloneCronEntry(entry))
		}
	}

	slices.SortFunc(entries, func(a, other *broker.CronEntry) int {
		return strings.Compare(a.ID, other.ID)
	})

	return entries, nil
}

// SetCronPaused persists the entry pause flag; see broker.Broker.
func (b *Broker) SetCronPaused(_ context.Context, id string, paused bool) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	entry, exists := b.cronEntries[id]
	if !exists {
		return broker.ErrTaskNotFound
	}

	entry.Paused = paused

	return nil
}

// UpdateCronNextRun compare-and-sets one entry's next fire time; see
// broker.Broker.
func (b *Broker) UpdateCronNextRun(_ context.Context, id string, expected, next time.Time) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	entry, exists := b.cronEntries[id]
	if !exists || !entry.NextRunAt.Equal(expected) {
		return nil
	}

	entry.NextRunAt = next

	return nil
}

// DeleteCronEntry removes a cron entry; see broker.Broker.
func (b *Broker) DeleteCronEntry(_ context.Context, id string) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	delete(b.cronEntries, id)

	return nil
}

// Close releases resources; the in-memory broker holds none.
func (b *Broker) Close() error {
	return nil
}

// cloneCronEntry deep-copies an entry so callers cannot alias stored state.
func cloneCronEntry(entry *broker.CronEntry) *broker.CronEntry {
	clone := *entry
	clone.Payload = slices.Clone(entry.Payload)

	if entry.Options != nil {
		clone.Options = proto.Clone(entry.Options).(*conveyorv1.TaskOptions)
	}

	return &clone
}
