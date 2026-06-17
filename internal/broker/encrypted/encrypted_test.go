// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package encrypted

import (
	"bytes"
	"context"
	"errors"
	"testing"

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
