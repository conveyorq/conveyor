// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package encrypted

import (
	"bytes"
	"context"
	"errors"
	"iter"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/conveyorq/conveyor/encryption"
	"github.com/conveyorq/conveyor/internal/broker"
	"github.com/conveyorq/conveyor/internal/broker/brokertest"
	"github.com/conveyorq/conveyor/internal/broker/memory"
	"github.com/conveyorq/conveyor/internal/clock"
	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
)

// testEncryptor builds an AES-256-GCM encryptor for tests.
func testEncryptor(t *testing.T) encryption.Encryptor {
	t.Helper()

	key := encryption.Key{ID: "test", Secret: bytes.Repeat([]byte{0x2a}, 32)}

	encryptor, err := encryption.NewAESGCM("test", key)
	if err != nil {
		t.Fatalf("NewAESGCM: %v", err)
	}

	return encryptor
}

// TestConformance runs the full broker conformance suite against an in-memory
// broker wrapped in the encrypted decorator, proving encryption is transparent:
// every contract the bare broker honors still holds end to end.
func TestConformance(t *testing.T) {
	brokertest.Run(t, func(t *testing.T, timeSource clock.Clock) broker.Broker {
		inner := memory.New(timeSource)
		t.Cleanup(func() { _ = inner.Close() })

		return New(inner, testEncryptor(t))
	})
}

// TestMaxPayloadBytesDelegates confirms the decorator reports the wrapped
// broker's payload cap rather than one of its own.
func TestMaxPayloadBytesDelegates(t *testing.T) {
	const payloadCap = 4096

	inner := memory.NewWithMaxPayload(clock.System(), payloadCap)
	t.Cleanup(func() { _ = inner.Close() })

	require.Equal(t, payloadCap, New(inner, testEncryptor(t)).MaxPayloadBytes())
}

// errEncryptor is an Encryptor that always fails, used to prove the decorator
// propagates encryption and decryption errors rather than swallowing them.
type errEncryptor struct {
	// fail is the error returned by both Encrypt and Decrypt.
	fail error
}

func (e errEncryptor) Encrypt(context.Context, []byte) ([]byte, error) { return nil, e.fail }

func (e errEncryptor) Decrypt(context.Context, []byte) ([]byte, error) { return nil, e.fail }

// TestEncryptErrorPropagates confirms an Encrypt failure aborts Enqueue and
// surfaces to the caller, so a task is never silently stored in plaintext.
func TestEncryptErrorPropagates(t *testing.T) {
	inner := memory.New(clock.System())
	t.Cleanup(func() { _ = inner.Close() })

	sentinel := errors.New("kms unreachable")
	decorated := New(inner, errEncryptor{fail: sentinel})

	task := &conveyorv1.TaskEnvelope{
		Id:      "01J000000000000000000FAIL0",
		Queue:   "default",
		Type:    "test:task",
		Payload: []byte("never stored"),
	}

	err := decorated.Enqueue(context.Background(), task)
	if !errors.Is(err, sentinel) {
		t.Fatalf("Enqueue error = %v, want %v", err, sentinel)
	}

	if _, _, err := inner.GetTask(context.Background(), task.GetId()); !errors.Is(err, broker.ErrTaskNotFound) {
		t.Fatalf("task was stored despite encryption failure: %v", err)
	}
}

// TestEncryptErrorPropagatesBatch confirms an Encrypt failure aborts EnqueueBatch
// before any task is committed, so the atomic path never stores plaintext.
func TestEncryptErrorPropagatesBatch(t *testing.T) {
	inner := memory.New(clock.System())
	t.Cleanup(func() { _ = inner.Close() })

	sentinel := errors.New("kms unreachable")
	decorated := New(inner, errEncryptor{fail: sentinel})

	tasks := []*conveyorv1.TaskEnvelope{
		{Id: "01J000000000000000000BATCH", Queue: "default", Type: "test:task", Payload: []byte("never stored")},
	}

	err := decorated.EnqueueBatch(context.Background(), tasks)
	if !errors.Is(err, sentinel) {
		t.Fatalf("EnqueueBatch error = %v, want %v", err, sentinel)
	}

	if _, _, err := inner.GetTask(context.Background(), tasks[0].GetId()); !errors.Is(err, broker.ErrTaskNotFound) {
		t.Fatalf("task was stored despite encryption failure: %v", err)
	}
}

// TestDecryptErrorPropagates confirms a Decrypt failure surfaces from a read
// path rather than returning corrupt bytes. A non-empty payload is stored
// through the bare broker, then read back through a failing decryptor.
func TestDecryptErrorPropagates(t *testing.T) {
	inner := memory.New(clock.System())
	t.Cleanup(func() { _ = inner.Close() })

	ctx := context.Background()
	task := &conveyorv1.TaskEnvelope{
		Id:      "01J000000000000000000FAIL1",
		Queue:   "default",
		Type:    "test:task",
		Payload: []byte("stored bytes"),
	}

	if err := inner.Enqueue(ctx, task); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	sentinel := errors.New("bad key")
	decorated := New(inner, errEncryptor{fail: sentinel})

	if _, _, err := decorated.GetTask(ctx, task.GetId()); !errors.Is(err, sentinel) {
		t.Fatalf("GetTask error = %v, want %v", err, sentinel)
	}
}

// TestPayloadStoredEncrypted confirms the wrapped broker holds ciphertext, not
// plaintext, while the decorator returns the original plaintext.
func TestPayloadStoredEncrypted(t *testing.T) {
	inner := memory.New(clock.System())
	t.Cleanup(func() { _ = inner.Close() })

	decorated := New(inner, testEncryptor(t))

	ctx := context.Background()
	plaintext := []byte("sensitive task payload")
	task := &conveyorv1.TaskEnvelope{
		Id:      "01J000000000000000000TASK0",
		Queue:   "default",
		Type:    "test:task",
		Payload: plaintext,
	}

	if err := decorated.Enqueue(ctx, task); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// Read straight from the wrapped broker: it must hold ciphertext.
	stored, _, err := inner.GetTask(ctx, task.GetId())
	if err != nil {
		t.Fatalf("inner GetTask: %v", err)
	}

	if bytes.Equal(stored.GetPayload(), plaintext) {
		t.Fatal("wrapped broker stored plaintext, want ciphertext")
	}

	if bytes.Contains(stored.GetPayload(), plaintext) {
		t.Fatal("stored payload contains plaintext")
	}

	// Read through the decorator: it must return the original plaintext.
	got, _, err := decorated.GetTask(ctx, task.GetId())
	if err != nil {
		t.Fatalf("decorated GetTask: %v", err)
	}

	if !bytes.Equal(got.GetPayload(), plaintext) {
		t.Fatalf("decorated payload = %q, want %q", got.GetPayload(), plaintext)
	}
}

// TestEnqueueDoesNotMutateCaller guards the decorator against encrypting in
// place: the caller's envelope must still hold plaintext after Enqueue.
func TestEnqueueDoesNotMutateCaller(t *testing.T) {
	inner := memory.New(clock.System())
	t.Cleanup(func() { _ = inner.Close() })

	decorated := New(inner, testEncryptor(t))

	plaintext := []byte("must remain plaintext for the caller")
	task := &conveyorv1.TaskEnvelope{
		Id:      "01J000000000000000000TASK1",
		Queue:   "default",
		Type:    "test:task",
		Payload: plaintext,
	}

	if err := decorated.Enqueue(context.Background(), task); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if !bytes.Equal(task.GetPayload(), plaintext) {
		t.Fatal("Enqueue mutated the caller's payload")
	}
}

// TestEmptyPayloadPassesThrough confirms a task with no payload is stored with
// no payload, not a ciphertext frame.
func TestEmptyPayloadPassesThrough(t *testing.T) {
	inner := memory.New(clock.System())
	t.Cleanup(func() { _ = inner.Close() })

	decorated := New(inner, testEncryptor(t))

	ctx := context.Background()
	task := &conveyorv1.TaskEnvelope{
		Id:    "01J000000000000000000TASK2",
		Queue: "default",
		Type:  "test:task",
	}

	if err := decorated.Enqueue(ctx, task); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	stored, _, err := inner.GetTask(ctx, task.GetId())
	if err != nil {
		t.Fatalf("inner GetTask: %v", err)
	}

	if len(stored.GetPayload()) != 0 {
		t.Fatalf("empty payload stored as %d bytes, want 0", len(stored.GetPayload()))
	}
}

// TestCronPayloadStoredEncrypted confirms cron entry payloads are encrypted at
// rest and decrypted transparently on read.
func TestCronPayloadStoredEncrypted(t *testing.T) {
	inner := memory.New(clock.System())
	t.Cleanup(func() { _ = inner.Close() })

	decorated := New(inner, testEncryptor(t))

	ctx := context.Background()
	plaintext := []byte("cron payload")
	entry := &broker.CronEntry{
		ID:       "nightly",
		Spec:     "0 0 * * * *",
		TaskType: "report:nightly",
		Queue:    "default",
		Payload:  plaintext,
	}

	if err := decorated.UpsertCronEntry(ctx, entry); err != nil {
		t.Fatalf("UpsertCronEntry: %v", err)
	}

	storedEntries, err := inner.ListCronEntries(ctx)
	if err != nil {
		t.Fatalf("inner ListCronEntries: %v", err)
	}

	if len(storedEntries) != 1 {
		t.Fatalf("stored entries = %d, want 1", len(storedEntries))
	}

	if bytes.Equal(storedEntries[0].Payload, plaintext) {
		t.Fatal("wrapped broker stored cron payload as plaintext")
	}

	decoratedEntries, err := decorated.ListCronEntries(ctx)
	if err != nil {
		t.Fatalf("decorated ListCronEntries: %v", err)
	}

	if !bytes.Equal(decoratedEntries[0].Payload, plaintext) {
		t.Fatalf("decorated cron payload = %q, want %q", decoratedEntries[0].Payload, plaintext)
	}
}

// chunks yields the given chunks in order.
func chunks(data ...[]byte) iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {
		for _, chunk := range data {
			if !yield(chunk, nil) {
				return
			}
		}
	}
}

// collect drains a payload sequence, returning its chunks and first error.
func collect(sequence iter.Seq2[[]byte, error]) ([][]byte, error) {
	var collected [][]byte

	for data, err := range sequence {
		if err != nil {
			return collected, err
		}

		collected = append(collected, data)
	}

	return collected, nil
}

// rewritingBroker hands back the wrapped broker's payload chunks after
// rewrite, standing in for a store whose rows were reordered, dropped, or
// unreadable.
type rewritingBroker struct {
	broker.Broker
	// rewrite transforms the stored chunks, or fails the read.
	rewrite func(stored [][]byte) ([][]byte, error)
}

// ReadPayload yields the wrapped broker's chunks after rewrite.
func (r rewritingBroker) ReadPayload(ctx context.Context, taskID string, from, count int) iter.Seq2[[]byte, error] {
	stored, err := collect(r.Broker.ReadPayload(ctx, taskID, from, count))
	if err == nil {
		stored, err = r.rewrite(stored)
	}

	return func(yield func([]byte, error) bool) {
		if err != nil {
			yield(nil, err)

			return
		}

		for _, data := range stored {
			if !yield(data, nil) {
				return
			}
		}
	}
}

// readThrough enqueues a two-chunk payload through a decorator over a store
// that rewrites its chunks on read, then reads it back through the decorator.
func readThrough(t *testing.T, rewrite func(stored [][]byte) ([][]byte, error)) error {
	t.Helper()

	ctx := context.Background()
	inner := memory.New(clock.System())
	t.Cleanup(func() { _ = inner.Close() })

	decorated := New(rewritingBroker{Broker: inner, rewrite: rewrite}, testEncryptor(t))
	require.NoError(t, decorated.EnqueueChunked(ctx, chunkedTask("task-1"), chunks([]byte("first"), []byte("second"))))

	_, err := collect(decorated.ReadPayload(ctx, "task-1", 0, 2))

	return err
}

// chunkedTask builds a task ready for EnqueueChunked.
func chunkedTask(id string) *conveyorv1.TaskEnvelope {
	return &conveyorv1.TaskEnvelope{Id: id, Queue: "default", Type: "test:task"}
}

func TestChunkedPayloadStoredEncrypted(t *testing.T) {
	ctx := context.Background()
	inner := memory.New(clock.System())
	t.Cleanup(func() { _ = inner.Close() })

	decorated := New(inner, testEncryptor(t))
	require.NoError(t, decorated.EnqueueChunked(ctx, chunkedTask("task-1"), chunks([]byte("secret-one"), []byte("secret-two"))))

	stored, err := collect(inner.ReadPayload(ctx, "task-1", 0, 2))
	require.NoError(t, err)
	require.Len(t, stored, 2)

	for _, chunk := range stored {
		require.NotContains(t, string(chunk), "secret")
	}

	opened, err := collect(decorated.ReadPayload(ctx, "task-1", 0, 2))
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("secret-one"), []byte("secret-two")}, opened)

	second, err := collect(decorated.ReadPayload(ctx, "task-1", 1, 1))
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("secret-two")}, second, "a range starting mid-payload opens in place")
}

func TestEmptyChunkedPayloadStoredAsOneSealedChunk(t *testing.T) {
	ctx := context.Background()
	inner := memory.New(clock.System())
	t.Cleanup(func() { _ = inner.Close() })

	decorated := New(inner, testEncryptor(t))
	require.NoError(t, decorated.EnqueueChunked(ctx, chunkedTask("task-1"), chunks()))

	stored, err := collect(inner.ReadPayload(ctx, "task-1", 0, 1))
	require.NoError(t, err)
	require.Len(t, stored, 1)

	opened, err := collect(decorated.ReadPayload(ctx, "task-1", 0, 1))
	require.NoError(t, err)
	require.Equal(t, [][]byte{{}}, opened)
}

func TestChunkedPayloadReorderDetected(t *testing.T) {
	err := readThrough(t, func(stored [][]byte) ([][]byte, error) {
		slices.Reverse(stored)

		return stored, nil
	})
	require.ErrorIs(t, err, encryption.ErrChunkOrder)
}

func TestChunkedPayloadTruncationDetected(t *testing.T) {
	err := readThrough(t, func(stored [][]byte) ([][]byte, error) {
		return stored[:1], nil
	})
	require.ErrorIs(t, err, encryption.ErrChunkTruncated)
}

func TestChunkedPayloadStoreReadErrorPropagates(t *testing.T) {
	failure := errors.New("store down")
	err := readThrough(t, func([][]byte) ([][]byte, error) {
		return nil, failure
	})
	require.ErrorIs(t, err, failure)
}

func TestChunkedEncryptErrorCommitsNothing(t *testing.T) {
	ctx := context.Background()
	inner := memory.New(clock.System())
	t.Cleanup(func() { _ = inner.Close() })

	failure := errors.New("kms down")
	decorated := New(inner, errEncryptor{fail: failure})

	err := decorated.EnqueueChunked(ctx, chunkedTask("task-1"), chunks([]byte("first"), []byte("second")))
	require.ErrorIs(t, err, failure)

	_, _, err = inner.GetTask(ctx, "task-1")
	require.ErrorIs(t, err, broker.ErrTaskNotFound)
}

func TestChunkedSourceErrorPropagates(t *testing.T) {
	ctx := context.Background()
	inner := memory.New(clock.System())
	t.Cleanup(func() { _ = inner.Close() })

	failure := errors.New("upload broken")
	broken := func(yield func([]byte, error) bool) {
		yield(nil, failure)
	}

	err := New(inner, testEncryptor(t)).EnqueueChunked(ctx, chunkedTask("task-1"), broken)
	require.ErrorIs(t, err, failure)
}

func TestChunkedDecryptErrorPropagates(t *testing.T) {
	ctx := context.Background()
	inner := memory.New(clock.System())
	t.Cleanup(func() { _ = inner.Close() })

	require.NoError(t, New(inner, testEncryptor(t)).EnqueueChunked(ctx, chunkedTask("task-1"), chunks([]byte("data"))))

	failure := errors.New("kms down")
	_, err := collect(New(inner, errEncryptor{fail: failure}).ReadPayload(ctx, "task-1", 0, 1))
	require.ErrorIs(t, err, failure)
}
