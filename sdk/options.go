package conveyor

import (
	"time"
)

// Option configures a Client or a Worker.
type Option func(*options)

// options collects the settings shared by NewClient and NewWorker; the
// constructors validate what applies to them.
type options struct {
	// token is the bearer token presented to the server.
	token string
	// queues maps queue name to dispatch weight (workers only).
	queues map[string]int
	// concurrency is the worker's total execution slots (workers only).
	concurrency int
}

// WithToken authenticates with the given bearer token.
func WithToken(token string) Option {
	return func(o *options) { o.token = token }
}

// WithQueues declares the queues a worker serves, mapping queue name to
// dispatch weight. Workers require at least one queue.
func WithQueues(queues map[string]int) Option {
	return func(o *options) { o.queues = queues }
}

// WithConcurrency sets the worker's total concurrent execution slots.
func WithConcurrency(n int) Option {
	return func(o *options) { o.concurrency = n }
}

// EnqueueOption configures one Enqueue call.
type EnqueueOption func(*enqueueOptions)

// enqueueOptions collects per-enqueue settings.
type enqueueOptions struct {
	// taskID is a client-assigned id for idempotent retries.
	taskID string
	// queue is the target queue; empty selects the server default.
	queue string
	// maxRetry is the retry budget; zero selects the server default.
	maxRetry int
	// priority orders dispatch within a queue; zero selects the default.
	priority int
	// processAt delays execution to an absolute time.
	processAt time.Time
	// processIn delays execution by a duration.
	processIn time.Duration
	// retention keeps the completed task visible before purge.
	retention time.Duration
	// uniqueKey enforces uniqueness among incomplete tasks.
	uniqueKey string
	// uniqueTTL bounds how long the uniqueness claim is held.
	uniqueTTL time.Duration
}

// TaskID assigns a client-chosen task id, making Enqueue retries
// idempotent: re-enqueueing an existing id is a no-op.
func TaskID(id string) EnqueueOption {
	return func(o *enqueueOptions) { o.taskID = id }
}

// Queue routes the task to the named queue instead of "default".
func Queue(name string) EnqueueOption {
	return func(o *enqueueOptions) { o.queue = name }
}

// MaxRetry sets how many times a failing task is retried before archiving.
func MaxRetry(n int) EnqueueOption {
	return func(o *enqueueOptions) { o.maxRetry = n }
}

// Priority orders dispatch within a queue, 1 (lowest) to 9 (highest);
// unset tasks run at the default priority 4.
func Priority(p int) EnqueueOption {
	return func(o *enqueueOptions) { o.priority = p }
}

// ProcessAt delays execution until the given time.
func ProcessAt(t time.Time) EnqueueOption {
	return func(o *enqueueOptions) { o.processAt = t }
}

// ProcessIn delays execution by the given duration.
func ProcessIn(d time.Duration) EnqueueOption {
	return func(o *enqueueOptions) { o.processIn = d }
}

// Retention keeps the completed task row visible for inspection for the
// given duration before it is purged.
func Retention(d time.Duration) EnqueueOption {
	return func(o *enqueueOptions) { o.retention = d }
}

// Unique rejects the enqueue with ErrDuplicateTask while an incomplete
// task with the same uniqueness key exists, for at most ttl. The key is
// derived from the task type and payload; combine with UniqueKey to choose
// it explicitly.
func Unique(ttl time.Duration) EnqueueOption {
	return func(o *enqueueOptions) { o.uniqueTTL = ttl }
}

// UniqueKey sets the explicit uniqueness key compared by Unique, e.g.
// "user:42:welcome", instead of the derived type-and-payload key.
func UniqueKey(key string) EnqueueOption {
	return func(o *enqueueOptions) { o.uniqueKey = key }
}
