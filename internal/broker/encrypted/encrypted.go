// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

// Package encrypted wraps a Broker so task payloads and results are encrypted at
// rest: the storage engine holds only ciphertext, while callers above the
// broker keep working with plaintext. It is the server-side, zero-code
// placement of Conveyor's encryption seam — the alternative to encrypting
// end to end in the SDK. A deployment uses one or the other, never both, so a
// payload is encrypted exactly once.
//
// The decorator implements every Broker method by hand rather than embedding
// the interface. This is deliberate and a security property: a payload- or
// result-bearing method added to Broker later will not compile here until its
// encryption is decided, so a new method can never silently bypass encryption.
// Methods that carry payload or result bytes seal or open them; every other
// method delegates to the wrapped broker unchanged. The decorator never mutates
// a caller's argument or a value the wrapped broker returns: when a payload
// changes it does so on a clone, so an aliased stored object is never corrupted.
package encrypted

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/conveyorq/conveyor/encryption"
	"github.com/conveyorq/conveyor/internal/broker"
	"github.com/conveyorq/conveyor/internal/events"
	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
)

// encryptedBroker satisfies broker.Broker. Because every method is implemented
// explicitly, this assertion fails to compile if Broker grows a method,
// forcing a decision about whether the new method handles payload or result.
var _ broker.Broker = (*encryptedBroker)(nil)

// encryptedBroker encrypts payloads and results on the way into the wrapped
// broker and decrypts them on the way out.
type encryptedBroker struct {
	// inner is the wrapped storage engine.
	inner broker.Broker
	// encryptor seals and opens payloads and results.
	encryptor encryption.Encryptor
}

// New wraps inner so payloads and results are encrypted at rest with encryptor.
// The returned broker is a drop-in replacement: callers see plaintext, the
// wrapped broker sees only ciphertext.
func New(inner broker.Broker, encryptor encryption.Encryptor) broker.Broker {
	return &encryptedBroker{inner: inner, encryptor: encryptor}
}

// Enqueue encrypts the task payload, then commits the task. An empty payload
// is committed as-is, so a task with no payload stays one.
func (e *encryptedBroker) Enqueue(ctx context.Context, task *conveyorv1.TaskEnvelope) error {
	if len(task.GetPayload()) == 0 {
		return e.inner.Enqueue(ctx, task)
	}

	sealed, err := e.encryptor.Encrypt(ctx, task.GetPayload())
	if err != nil {
		return fmt.Errorf("broker/encrypted: encrypting payload: %w", err)
	}

	clone := proto.Clone(task).(*conveyorv1.TaskEnvelope)
	clone.Payload = sealed

	return e.inner.Enqueue(ctx, clone)
}

// Lease decrypts the payload of every leased task.
func (e *encryptedBroker) Lease(ctx context.Context, queue string, limit int, ttl time.Duration, leaseID string) ([]*conveyorv1.TaskEnvelope, error) {
	tasks, err := e.inner.Lease(ctx, queue, limit, ttl, leaseID)
	if err != nil {
		return nil, err
	}

	return e.openEnvelopes(ctx, tasks)
}

// LeaseGroup decrypts the payload of every leased group member.
func (e *encryptedBroker) LeaseGroup(ctx context.Context, queue, group string, limit int, ttl time.Duration, leaseID string) ([]*conveyorv1.TaskEnvelope, error) {
	tasks, err := e.inner.LeaseGroup(ctx, queue, group, limit, ttl, leaseID)
	if err != nil {
		return nil, err
	}

	return e.openEnvelopes(ctx, tasks)
}

// Ack encrypts the result before retaining it. An empty result is stored as-is.
//
// There is no symmetric decrypt: the Broker interface returns no read path for
// a stored result, so encrypting on write is sufficient. If a result-read path
// is ever added to Broker, it must decrypt here — the compile-time assertion
// above forces that method through this package first.
func (e *encryptedBroker) Ack(ctx context.Context, taskID, leaseID string, result []byte) error {
	if len(result) == 0 {
		return e.inner.Ack(ctx, taskID, leaseID, result)
	}

	sealed, err := e.encryptor.Encrypt(ctx, result)
	if err != nil {
		return fmt.Errorf("broker/encrypted: encrypting result: %w", err)
	}

	return e.inner.Ack(ctx, taskID, leaseID, sealed)
}

// GetTask decrypts the returned task's payload.
func (e *encryptedBroker) GetTask(ctx context.Context, id string) (*conveyorv1.TaskEnvelope, conveyorv1.TaskState, error) {
	task, state, err := e.inner.GetTask(ctx, id)
	if err != nil {
		return task, state, err
	}

	opened, err := e.openEnvelope(ctx, task)
	if err != nil {
		return nil, state, err
	}

	return opened, state, nil
}

// ListTasks decrypts the payload of every returned record.
func (e *encryptedBroker) ListTasks(ctx context.Context, query broker.TaskQuery) ([]broker.TaskRecord, error) {
	records, err := e.inner.ListTasks(ctx, query)
	if err != nil {
		return nil, err
	}

	for i := range records {
		opened, err := e.openEnvelope(ctx, records[i].Envelope)
		if err != nil {
			return nil, err
		}

		records[i].Envelope = opened
	}

	return records, nil
}

// UpsertCronEntry encrypts the entry's payload before persisting it, so cron
// payloads are protected at rest like task payloads.
func (e *encryptedBroker) UpsertCronEntry(ctx context.Context, entry *broker.CronEntry) error {
	if len(entry.Payload) == 0 {
		return e.inner.UpsertCronEntry(ctx, entry)
	}

	sealed, err := e.encryptor.Encrypt(ctx, entry.Payload)
	if err != nil {
		return fmt.Errorf("broker/encrypted: encrypting cron payload: %w", err)
	}

	clone := *entry
	clone.Payload = sealed

	return e.inner.UpsertCronEntry(ctx, &clone)
}

// ListCronEntries decrypts the payload of every returned entry.
func (e *encryptedBroker) ListCronEntries(ctx context.Context) ([]*broker.CronEntry, error) {
	entries, err := e.inner.ListCronEntries(ctx)
	if err != nil {
		return nil, err
	}

	return e.openCronEntries(ctx, entries)
}

// ListDueCronEntries decrypts the payload of every returned entry.
func (e *encryptedBroker) ListDueCronEntries(ctx context.Context, now time.Time) ([]*broker.CronEntry, error) {
	entries, err := e.inner.ListDueCronEntries(ctx, now)
	if err != nil {
		return nil, err
	}

	return e.openCronEntries(ctx, entries)
}

// The methods below carry neither payload nor result bytes; they delegate to
// the wrapped broker unchanged. They are written out explicitly, rather than
// promoted from an embedded interface, so a new Broker method cannot be added
// without a deliberate choice here (see the package and assertion comments).

// GroupStats delegates to the wrapped broker.
func (e *encryptedBroker) GroupStats(ctx context.Context) ([]broker.GroupStat, error) {
	return e.inner.GroupStats(ctx)
}

// ExtendLease delegates to the wrapped broker.
func (e *encryptedBroker) ExtendLease(ctx context.Context, taskID, leaseID string, ttl time.Duration) error {
	return e.inner.ExtendLease(ctx, taskID, leaseID, ttl)
}

// Fail delegates to the wrapped broker.
func (e *encryptedBroker) Fail(ctx context.Context, taskID, leaseID, errMsg string, processAt time.Time) error {
	return e.inner.Fail(ctx, taskID, leaseID, errMsg, processAt)
}

// Release delegates to the wrapped broker.
func (e *encryptedBroker) Release(ctx context.Context, taskID, leaseID string) error {
	return e.inner.Release(ctx, taskID, leaseID)
}

// Archive delegates to the wrapped broker.
func (e *encryptedBroker) Archive(ctx context.Context, taskID, leaseID, errMsg string) error {
	return e.inner.Archive(ctx, taskID, leaseID, errMsg)
}

// ReapExpiredLeases delegates to the wrapped broker.
func (e *encryptedBroker) ReapExpiredLeases(ctx context.Context, limit int) ([]string, error) {
	return e.inner.ReapExpiredLeases(ctx, limit)
}

// PromoteScheduled delegates to the wrapped broker.
func (e *encryptedBroker) PromoteScheduled(ctx context.Context, limit int) ([]string, error) {
	return e.inner.PromoteScheduled(ctx, limit)
}

// ResolveDependents delegates to the wrapped broker.
func (e *encryptedBroker) ResolveDependents(ctx context.Context, taskID string) ([]string, error) {
	return e.inner.ResolveDependents(ctx, taskID)
}

// PromoteReadyDependents delegates to the wrapped broker.
func (e *encryptedBroker) PromoteReadyDependents(ctx context.Context, limit int) ([]string, error) {
	return e.inner.PromoteReadyDependents(ctx, limit)
}

// PurgeCompleted delegates to the wrapped broker.
func (e *encryptedBroker) PurgeCompleted(ctx context.Context, limit int) (int, error) {
	return e.inner.PurgeCompleted(ctx, limit)
}

// ArchiveExpired delegates to the wrapped broker.
func (e *encryptedBroker) ArchiveExpired(ctx context.Context, limit int) (int, error) {
	return e.inner.ArchiveExpired(ctx, limit)
}

// PendingCount delegates to the wrapped broker.
func (e *encryptedBroker) PendingCount(ctx context.Context) (map[string]int64, error) {
	return e.inner.PendingCount(ctx)
}

// QueueStats delegates to the wrapped broker.
func (e *encryptedBroker) QueueStats(ctx context.Context) ([]broker.QueueStat, error) {
	return e.inner.QueueStats(ctx)
}

// SetQueuePaused delegates to the wrapped broker.
func (e *encryptedBroker) SetQueuePaused(ctx context.Context, queue string, paused bool) error {
	return e.inner.SetQueuePaused(ctx, queue, paused)
}

// QueuePaused delegates to the wrapped broker.
func (e *encryptedBroker) QueuePaused(ctx context.Context, queue string) (bool, error) {
	return e.inner.QueuePaused(ctx, queue)
}

// SetQueueRateLimit delegates to the wrapped broker.
func (e *encryptedBroker) SetQueueRateLimit(ctx context.Context, queue string, ratePerSec float64, burst int) error {
	return e.inner.SetQueueRateLimit(ctx, queue, ratePerSec, burst)
}

// DeleteQueueRateLimit delegates to the wrapped broker.
func (e *encryptedBroker) DeleteQueueRateLimit(ctx context.Context, queue string) error {
	return e.inner.DeleteQueueRateLimit(ctx, queue)
}

// QueueRateLimit delegates to the wrapped broker.
func (e *encryptedBroker) QueueRateLimit(ctx context.Context, queue string) (broker.RateLimit, bool, error) {
	return e.inner.QueueRateLimit(ctx, queue)
}

// QueueRateLimits delegates to the wrapped broker.
func (e *encryptedBroker) QueueRateLimits(ctx context.Context) ([]broker.RateLimit, error) {
	return e.inner.QueueRateLimits(ctx)
}

// SetQueueConcurrencyLimit delegates to the wrapped broker.
func (e *encryptedBroker) SetQueueConcurrencyLimit(ctx context.Context, queue string, maxActive int) error {
	return e.inner.SetQueueConcurrencyLimit(ctx, queue, maxActive)
}

// DeleteQueueConcurrencyLimit delegates to the wrapped broker.
func (e *encryptedBroker) DeleteQueueConcurrencyLimit(ctx context.Context, queue string) error {
	return e.inner.DeleteQueueConcurrencyLimit(ctx, queue)
}

// QueueConcurrencyLimit delegates to the wrapped broker.
func (e *encryptedBroker) QueueConcurrencyLimit(ctx context.Context, queue string) (broker.ConcurrencyLimit, bool, error) {
	return e.inner.QueueConcurrencyLimit(ctx, queue)
}

// QueueConcurrencyLimits delegates to the wrapped broker.
func (e *encryptedBroker) QueueConcurrencyLimits(ctx context.Context) ([]broker.ConcurrencyLimit, error) {
	return e.inner.QueueConcurrencyLimits(ctx)
}

// Info delegates to the wrapped broker.
func (e *encryptedBroker) Info(ctx context.Context) (broker.Info, error) {
	return e.inner.Info(ctx)
}

// CancelTask delegates to the wrapped broker.
func (e *encryptedBroker) CancelTask(ctx context.Context, id string) error {
	return e.inner.CancelTask(ctx, id)
}

// DeleteTask delegates to the wrapped broker.
func (e *encryptedBroker) DeleteTask(ctx context.Context, id string) error {
	return e.inner.DeleteTask(ctx, id)
}

// RunTaskNow delegates to the wrapped broker.
func (e *encryptedBroker) RunTaskNow(ctx context.Context, id string) error {
	return e.inner.RunTaskNow(ctx, id)
}

// ArchiveTask delegates to the wrapped broker.
func (e *encryptedBroker) ArchiveTask(ctx context.Context, id string) error {
	return e.inner.ArchiveTask(ctx, id)
}

// SetCronPaused delegates to the wrapped broker.
func (e *encryptedBroker) SetCronPaused(ctx context.Context, id string, paused bool) error {
	return e.inner.SetCronPaused(ctx, id, paused)
}

// UpdateCronNextRun delegates to the wrapped broker.
func (e *encryptedBroker) UpdateCronNextRun(ctx context.Context, id string, expected, next time.Time) error {
	return e.inner.UpdateCronNextRun(ctx, id, expected, next)
}

// DeleteCronEntry delegates to the wrapped broker.
func (e *encryptedBroker) DeleteCronEntry(ctx context.Context, id string) error {
	return e.inner.DeleteCronEntry(ctx, id)
}

// SetEventSink delegates to the wrapped broker. Lifecycle events carry no
// payload or result bytes, so there is nothing to encrypt: the wrapped broker
// emits the same events whether or not it is wrapped.
func (e *encryptedBroker) SetEventSink(sink events.Sink) {
	e.inner.SetEventSink(sink)
}

// Close delegates to the wrapped broker.
func (e *encryptedBroker) Close() error {
	return e.inner.Close()
}

// openEnvelopes decrypts the payload of each task in the slice, replacing
// entries with decrypted clones.
func (e *encryptedBroker) openEnvelopes(ctx context.Context, tasks []*conveyorv1.TaskEnvelope) ([]*conveyorv1.TaskEnvelope, error) {
	for i, task := range tasks {
		opened, err := e.openEnvelope(ctx, task)
		if err != nil {
			return nil, err
		}

		tasks[i] = opened
	}

	return tasks, nil
}

// openEnvelope returns a clone of task with its payload decrypted, leaving the
// original untouched. A nil or empty-payload task is returned unchanged.
func (e *encryptedBroker) openEnvelope(ctx context.Context, task *conveyorv1.TaskEnvelope) (*conveyorv1.TaskEnvelope, error) {
	if task == nil || len(task.GetPayload()) == 0 {
		return task, nil
	}

	plaintext, err := e.encryptor.Decrypt(ctx, task.GetPayload())
	if err != nil {
		return nil, fmt.Errorf("broker/encrypted: decrypting payload of task %q: %w", task.GetId(), err)
	}

	clone := proto.Clone(task).(*conveyorv1.TaskEnvelope)
	clone.Payload = plaintext

	return clone, nil
}

// openCronEntries decrypts the payload of each entry in the slice, replacing
// entries with decrypted clones.
func (e *encryptedBroker) openCronEntries(ctx context.Context, entries []*broker.CronEntry) ([]*broker.CronEntry, error) {
	for i, entry := range entries {
		if entry == nil || len(entry.Payload) == 0 {
			continue
		}

		plaintext, err := e.encryptor.Decrypt(ctx, entry.Payload)
		if err != nil {
			return nil, fmt.Errorf("broker/encrypted: decrypting payload of cron entry %q: %w", entry.ID, err)
		}

		clone := *entry
		clone.Payload = plaintext
		entries[i] = &clone
	}

	return entries, nil
}
