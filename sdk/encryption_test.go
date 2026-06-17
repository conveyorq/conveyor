// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package conveyor

import (
	"bytes"
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/conveyorq/conveyor/encryption"
)

// testAESGCM builds an AES-256-GCM encryptor whose secret is filled with one
// byte, so distinct fills make distinct keys.
func testAESGCM(t *testing.T, keyID string, fill byte) encryption.Encryptor {
	t.Helper()

	encryptor, err := encryption.NewAESGCM(keyID, encryption.Key{
		ID:     keyID,
		Secret: bytes.Repeat([]byte{fill}, 32),
	})
	require.NoError(t, err)

	return encryptor
}

func TestWithEncryptionSetsEncryptor(t *testing.T) {
	encryptor := testAESGCM(t, "k1", 0x01)

	settings := &options{}
	WithEncryption(encryptor)(settings)

	require.Same(t, encryptor, settings.encryptor)
}

func TestWithEncryptionNilIsIgnored(t *testing.T) {
	settings := &options{}

	require.NotPanics(t, func() { WithEncryption(nil)(settings) })
	require.Nil(t, settings.encryptor)
}

func TestWithEncryptionMarkerCopiesAndMarks(t *testing.T) {
	original := map[string]string{"trace": "abc"}

	marked := withEncryptionMarker(original)

	require.Equal(t, encryptionMarkerValue, marked[encryptionMarkerKey])
	require.Equal(t, "abc", marked["trace"])

	// The caller's map is untouched, so the Task can be enqueued again.
	require.NotContains(t, original, encryptionMarkerKey)
}

func TestWithEncryptionMarkerHandlesNilMetadata(t *testing.T) {
	marked := withEncryptionMarker(nil)

	require.Equal(t, map[string]string{encryptionMarkerKey: encryptionMarkerValue}, marked)
}

// secretPayload is the round-trip test's task payload.
type secretPayload struct {
	Secret string `json:"secret"`
}

// runWorker starts a worker against baseURL and stops it when the test ends.
func runWorker(t *testing.T, baseURL string, mux *Mux, opts ...Option) {
	t.Helper()

	settings := append([]Option{WithQueues(map[string]int{"default": 1}), WithConcurrency(4)}, opts...)

	worker, err := NewWorker(baseURL, settings...)
	require.NoError(t, err)

	runCtx, stop := context.WithCancel(context.Background())
	done := make(chan error, 1)

	go func() { done <- worker.Run(runCtx, mux) }()

	t.Cleanup(func() {
		stop()

		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("worker did not stop on context cancellation")
		}
	})
}

// TestEncryptionRoundTrip proves the end-to-end path: an encrypting client and
// an encrypting worker sharing a key see the original plaintext at the handler,
// while the server only ever relays what the client sealed.
func TestEncryptionRoundTrip(t *testing.T) {
	baseURL := startTestServer(t, nil)
	encryptor := testAESGCM(t, "k1", 0x01)

	client, err := NewClient(baseURL, WithEncryption(encryptor))
	require.NoError(t, err)

	var (
		mutex   sync.Mutex
		decoded secretPayload
	)

	mux := NewMux()
	mux.HandleFunc("secret:task", func(_ context.Context, task *Task) error {
		var payload secretPayload
		if err := task.Bind(&payload); err != nil {
			return SkipRetry(err)
		}

		mutex.Lock()
		decoded = payload
		mutex.Unlock()

		return nil
	})

	runWorker(t, baseURL, mux, WithEncryption(encryptor))

	info, err := client.Enqueue(context.Background(),
		NewTask("secret:task", JSON(secretPayload{Secret: "launch-code"})), Retention(time.Hour))
	require.NoError(t, err)

	awaitTaskState(t, client, info.ID, TaskStateCompleted)

	mutex.Lock()
	require.Equal(t, "launch-code", decoded.Secret)
	mutex.Unlock()
}

// TestEncryptionWrongKeyCannotDecrypt proves the payload is genuine ciphertext,
// not plaintext: a worker holding a different secret for the same key id cannot
// open it, the handler never runs, and the task dead-letters. If the server had
// stored plaintext, the wrong key would be irrelevant.
func TestEncryptionWrongKeyCannotDecrypt(t *testing.T) {
	baseURL := startTestServer(t, nil)

	client, err := NewClient(baseURL, WithEncryption(testAESGCM(t, "k1", 0x01)))
	require.NoError(t, err)

	var invocations atomic.Int64

	mux := NewMux()
	mux.HandleFunc("secret:task", func(context.Context, *Task) error {
		invocations.Add(1)

		return nil
	})

	runWorker(t, baseURL, mux, WithEncryption(testAESGCM(t, "k1", 0x02)))

	info, err := client.Enqueue(context.Background(),
		NewTask("secret:task", JSON(secretPayload{Secret: "launch-code"})),
		MaxRetry(1), Retention(time.Hour))
	require.NoError(t, err)

	awaitTaskState(t, client, info.ID, TaskStateArchived)

	require.Zero(t, invocations.Load(), "the handler must never run on an undecryptable task")
}

// TestEncryptionCoexistsWithPlaintext proves the marker gating: a plaintext
// task from a non-encrypting client is processed unchanged by an encrypting
// worker, so encrypted and plaintext tasks share a queue.
func TestEncryptionCoexistsWithPlaintext(t *testing.T) {
	baseURL := startTestServer(t, nil)

	client, err := NewClient(baseURL)
	require.NoError(t, err)

	var (
		mutex   sync.Mutex
		decoded secretPayload
	)

	mux := NewMux()
	mux.HandleFunc("secret:task", func(_ context.Context, task *Task) error {
		var payload secretPayload
		if err := task.Bind(&payload); err != nil {
			return SkipRetry(err)
		}

		mutex.Lock()
		decoded = payload
		mutex.Unlock()

		return nil
	})

	runWorker(t, baseURL, mux, WithEncryption(testAESGCM(t, "k1", 0x01)))

	info, err := client.Enqueue(context.Background(),
		NewTask("secret:task", JSON(secretPayload{Secret: "plain"})), Retention(time.Hour))
	require.NoError(t, err)

	awaitTaskState(t, client, info.ID, TaskStateCompleted)

	mutex.Lock()
	require.Equal(t, "plain", decoded.Secret)
	mutex.Unlock()
}
