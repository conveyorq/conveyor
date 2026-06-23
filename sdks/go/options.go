// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package conveyor

import (
	"time"

	"github.com/conveyorq/conveyor/encryption"
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
	// minServerVersion is the minimum server version the worker requires
	// (workers only); empty imposes no requirement.
	minServerVersion string
	// enqueueMiddleware decorates Client.Enqueue, outermost first (clients only).
	enqueueMiddleware []EnqueueMiddlewareFunc
	// encryptor encrypts task payloads end to end; nil leaves them in clear.
	encryptor encryption.Encryptor
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

// WithMinServerVersion requires the connected server to be at least the given
// semver version (e.g. "v1.2.0"). The server refuses the session if it is
// older. A non-semver value is ignored by the server's check; the empty
// default imposes no requirement.
func WithMinServerVersion(version string) Option {
	return func(o *options) { o.minServerVersion = version }
}

// WithEnqueueMiddleware appends middleware applied to every Client.Enqueue
// call, outermost first. It is the client-side counterpart of Mux.Use, letting
// callers inject metadata, enforce policy, or record metrics on the enqueue
// path. Passing a nil middleware panics.
func WithEnqueueMiddleware(middleware ...EnqueueMiddlewareFunc) Option {
	return func(o *options) {
		for _, wrap := range middleware {
			if wrap == nil {
				panic("conveyor: WithEnqueueMiddleware with nil middleware")
			}

			o.enqueueMiddleware = append(o.enqueueMiddleware, wrap)
		}
	}
}

// WithEncryption encrypts task payloads end to end with enc: a client seals
// each payload before it is enqueued, and a worker opens it on dispatch, so the
// server only ever stores and relays ciphertext — it holds no keys. Set the
// same Encryptor on every client and worker that share a queue. Use
// encryption.NewAESGCM for the built-in AES-256-GCM scheme, or supply your own
// Encryptor backed by a KMS, HSM, or custom codec.
//
// A nil enc is ignored, leaving encryption off, so callers can pass an
// optionally-configured Encryptor without a nil check.
//
// A worker decrypts only payloads that were sealed by an encrypting client, so
// encrypted and plaintext tasks may coexist on one queue; a worker that
// receives an encrypted task without an Encryptor fails the task rather than
// processing ciphertext.
func WithEncryption(enc encryption.Encryptor) Option {
	return func(o *options) { o.encryptor = enc }
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
	// timeout bounds a single execution attempt; zero leaves it unbounded.
	timeout time.Duration
	// deadline is the absolute time after which the task must not run.
	deadline time.Time
	// processAt delays execution to an absolute time.
	processAt time.Time
	// processIn delays execution by a duration.
	processIn time.Duration
	// expiresAt is the absolute time after which an undispatched task is
	// archived instead of run.
	expiresAt time.Time
	// expiresIn is the duration after enqueue after which an undispatched task
	// is archived instead of run.
	expiresIn time.Duration
	// retention keeps the completed task visible before purge.
	retention time.Duration
	// uniqueKey enforces uniqueness among incomplete tasks.
	uniqueKey string
	// uniqueTTL bounds how long the uniqueness claim is held.
	uniqueTTL time.Duration
	// group makes the task an aggregation-group member.
	group string
	// dependsOn lists the tasks this task waits for before it becomes eligible.
	dependsOn []Dependency
	// concurrencyKey caps how many tasks sharing it run at once on its queue.
	concurrencyKey string
	// retryStrategy overrides the server's default backoff growth; zero
	// (RetryDefault) keeps it.
	retryStrategy RetryStrategy
	// retryBase overrides the first-retry delay ceiling; zero keeps the default.
	retryBase time.Duration
	// retryMax overrides the overall retry delay cap; zero keeps the default.
	retryMax time.Duration
}

// RetryStrategy selects how a task's retry backoff delay grows with the attempt.
type RetryStrategy int

const (
	// RetryDefault uses the server's default backoff strategy.
	RetryDefault RetryStrategy = iota
	// RetryExponential doubles the delay ceiling each retry.
	RetryExponential
	// RetryLinear grows the delay ceiling linearly with the attempt.
	RetryLinear
	// RetryFixed holds the delay ceiling constant across retries.
	RetryFixed
)

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

// Timeout bounds a single execution attempt: the handler's context is
// canceled after the duration. The effective deadline is the earliest of the
// timeout, any Deadline, and the lease expiry.
func Timeout(d time.Duration) EnqueueOption {
	return func(o *enqueueOptions) { o.timeout = d }
}

// Deadline sets an absolute time after which the task must not run; the
// handler's context is canceled at that time if execution is still in flight.
func Deadline(t time.Time) EnqueueOption {
	return func(o *enqueueOptions) { o.deadline = t }
}

// ProcessAt delays execution until the given time.
func ProcessAt(t time.Time) EnqueueOption {
	return func(o *enqueueOptions) { o.processAt = t }
}

// ProcessIn delays execution by the given duration.
func ProcessIn(d time.Duration) EnqueueOption {
	return func(o *enqueueOptions) { o.processIn = d }
}

// ExpiresAt drops the task if it has not been dispatched by the given time:
// a task still waiting then is archived instead of run. Use it for work that
// loses its value once a moment passes (a one-time code, a timely
// notification). It is distinct from Deadline, which cancels a task already
// running, and Retention, which purges a task already completed. Mutually
// exclusive with ExpiresIn.
func ExpiresAt(t time.Time) EnqueueOption {
	return func(o *enqueueOptions) { o.expiresAt = t }
}

// ExpiresIn drops the task if it has not been dispatched within the given
// duration of enqueue; it is the relative form of ExpiresAt, resolved against
// the server clock. Mutually exclusive with ExpiresAt.
func ExpiresIn(d time.Duration) EnqueueOption {
	return func(o *enqueueOptions) { o.expiresIn = d }
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

// ConcurrencyKey caps how many tasks sharing the key run at once on the task's
// queue: the queue dispatches at most its configured concurrency limit of tasks
// with this key simultaneously, holding the rest pending until an active one
// finishes (e.g. "customer:42" to bound in-flight work per customer). It has no
// effect unless the queue has a concurrency limit set. Mutually exclusive with
// Group.
func ConcurrencyKey(key string) EnqueueOption {
	return func(o *enqueueOptions) { o.concurrencyKey = key }
}

// RetryPolicy overrides the server's default retry backoff for this task: the
// growth strategy plus the first-retry delay (base) and the overall cap (max).
// A zero base or max keeps the server default for that field, and RetryDefault
// keeps the server's strategy, so you can override only the part you need, e.g.
// RetryPolicy(RetryFixed, time.Minute, time.Minute) for a steady one-minute
// retry against a rate-limited downstream.
func RetryPolicy(strategy RetryStrategy, base, maxDelay time.Duration) EnqueueOption {
	return func(o *enqueueOptions) {
		o.retryStrategy = strategy
		o.retryBase = base
		o.retryMax = maxDelay
	}
}
