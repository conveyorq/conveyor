// Package memory provides the in-memory Broker used for development,
// tests, and the embedded dev mode. It implements the exact semantics of
// the durable brokers and passes the same conformance suite, but holds
// everything in process memory.
package memory

import (
	"context"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/conveyorq/conveyor/internal/broker"
	"github.com/conveyorq/conveyor/internal/clock"
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
	// retried counts completed retry attempts.
	retried int32
	// lastError is the message of the most recent failure.
	lastError string
	// leaseID identifies the active lease, empty when not active.
	leaseID string
	// leaseExpiresAt is when the active lease lapses.
	leaseExpiresAt time.Time
	// completedAt is when the task reached a terminal state.
	completedAt time.Time
	// result is the worker-reported result stored at Ack.
	result []byte
	// retention keeps the completed row for inspection before purge.
	retention time.Duration
	// uniqueKey is the uniqueness claim, empty when unclaimed or lapsed.
	uniqueKey string
	// uniqueExpiresAt bounds the claim; zero means until completion.
	uniqueExpiresAt time.Time
	// maxRetry caps retries before the reaper archives the task.
	maxRetry int32
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
	// cronEntries maps entry id to the stored entry.
	cronEntries map[string]*broker.CronEntry
}

// enforce interface compliance at compile time.
var _ broker.Broker = (*Broker)(nil)

// New returns an empty in-memory broker reading time from the given clock.
func New(timeSource clock.Clock) *Broker {
	return &Broker{
		clock:        timeSource,
		tasks:        make(map[string]*taskRow),
		pausedQueues: make(map[string]bool),
		cronEntries:  make(map[string]*broker.CronEntry),
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

	state := conveyorv1.TaskState_TASK_STATE_PENDING
	if processAt.After(now) {
		state = conveyorv1.TaskState_TASK_STATE_SCHEDULED
	}

	var uniqueExpiresAt time.Time
	if uniqueKey != "" && options.GetUniqueTtl() != nil {
		uniqueExpiresAt = now.Add(options.GetUniqueTtl().AsDuration())
	}

	b.tasks[task.GetId()] = &taskRow{
		envelope:        proto.Clone(task).(*conveyorv1.TaskEnvelope),
		state:           state,
		priority:        options.GetPriority(),
		processAt:       processAt,
		retried:         task.GetRetried(),
		lastError:       task.GetLastError(),
		retention:       options.GetRetention().AsDuration(),
		uniqueKey:       uniqueKey,
		uniqueExpiresAt: uniqueExpiresAt,
		maxRetry:        options.GetMaxRetry(),
	}

	return nil
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
		conveyorv1.TaskState_TASK_STATE_RETRY:
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
		if row.envelope.GetQueue() == queue && dispatchable(row.state) && !row.processAt.After(now) {
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
		row.state = conveyorv1.TaskState_TASK_STATE_ACTIVE
		row.leaseID = leaseID
		row.leaseExpiresAt = now.Add(ttl)
		leased = append(leased, overlay(row))
	}

	return leased, nil
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

// Ack completes an active task; see broker.Broker.
func (b *Broker) Ack(_ context.Context, taskID, leaseID string, result []byte) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	row, err := b.activeRow(taskID, leaseID)
	if err != nil {
		return err
	}

	row.state = conveyorv1.TaskState_TASK_STATE_COMPLETED
	row.completedAt = b.clock.Now()
	row.result = result
	row.leaseID = ""

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

	row.state = conveyorv1.TaskState_TASK_STATE_RETRY
	row.retried++
	row.lastError = errMsg
	row.processAt = processAt
	row.leaseID = ""

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

	row.state = conveyorv1.TaskState_TASK_STATE_PENDING
	row.processAt = b.clock.Now()
	row.leaseID = ""

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

	row.state = conveyorv1.TaskState_TASK_STATE_ARCHIVED
	row.lastError = errMsg
	row.completedAt = b.clock.Now()
	row.leaseID = ""

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

		if row.retried >= row.maxRetry {
			row.state = conveyorv1.TaskState_TASK_STATE_ARCHIVED
			row.completedAt = now
		} else {
			row.state = conveyorv1.TaskState_TASK_STATE_RETRY
			row.retried++
			row.processAt = now
			queues[row.envelope.GetQueue()] = struct{}{}
		}

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

		row.state = conveyorv1.TaskState_TASK_STATE_PENDING
		queues[row.envelope.GetQueue()] = struct{}{}
		promoted++
	}

	return slices.Collect(maps.Keys(queues)), nil
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

		if row.state == conveyorv1.TaskState_TASK_STATE_COMPLETED && !row.completedAt.Add(row.retention).After(now) {
			delete(b.tasks, id)
			purged++
		}
	}

	return purged, nil
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
		conveyorv1.TaskState_TASK_STATE_RETRY:
		row.state = conveyorv1.TaskState_TASK_STATE_CANCELED
		row.completedAt = b.clock.Now()

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
		conveyorv1.TaskState_TASK_STATE_RETRY:
		row.state = conveyorv1.TaskState_TASK_STATE_PENDING
		row.processAt = b.clock.Now()

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
