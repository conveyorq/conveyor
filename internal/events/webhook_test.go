// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package events

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"

	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
)

// discardLogger is a logger that drops all output, for tests.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestWebhookDeliversEvent(t *testing.T) {
	received := make(chan *conveyorv1.TaskEvent, 1)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))

		decoded := &conveyorv1.TaskEvent{}
		require.NoError(t, protojson.Unmarshal(body, decoded))
		received <- decoded

		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	bus := NewEventBus(8, nil)
	hook := NewWebhook(WebhookConfig{URL: server.URL}, discardLogger())
	hook.Start(bus)
	defer hook.Stop()

	bus.Emit(&conveyorv1.TaskEvent{Id: "task-1", Queue: "emails", EventType: conveyorv1.TaskEventType_TASK_EVENT_TYPE_COMPLETED})

	select {
	case got := <-received:
		assert.Equal(t, "task-1", got.GetId())
		assert.Equal(t, conveyorv1.TaskEventType_TASK_EVENT_TYPE_COMPLETED, got.GetEventType())
	case <-time.After(2 * time.Second):
		t.Fatal("webhook did not deliver the event")
	}
}

func TestWebhookSendsSecretHeader(t *testing.T) {
	auth := make(chan string, 1)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth <- r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	bus := NewEventBus(8, nil)
	hook := NewWebhook(WebhookConfig{URL: server.URL, Secret: "s3cret"}, discardLogger())
	hook.Start(bus)
	defer hook.Stop()

	bus.Emit(&conveyorv1.TaskEvent{Id: "task-1"})

	select {
	case got := <-auth:
		assert.Equal(t, "Bearer s3cret", got)
	case <-time.After(2 * time.Second):
		t.Fatal("webhook did not deliver the event")
	}
}

func TestWebhookRetriesThenSucceeds(t *testing.T) {
	var attempts atomic.Int64

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if attempts.Add(1) == 1 {
			w.WriteHeader(http.StatusInternalServerError)

			return
		}

		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	bus := NewEventBus(8, nil)
	hook := NewWebhook(WebhookConfig{URL: server.URL, MaxRetries: 3}, discardLogger())
	hook.Start(bus)
	defer hook.Stop()

	bus.Emit(&conveyorv1.TaskEvent{Id: "task-1"})

	require.Eventually(t, func() bool { return attempts.Load() == 2 }, 3*time.Second, 10*time.Millisecond)
}

func TestWebhookGivesUpAfterMaxRetries(t *testing.T) {
	var attempts atomic.Int64

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	bus := NewEventBus(8, nil)
	hook := NewWebhook(WebhookConfig{URL: server.URL, MaxRetries: 1}, discardLogger())
	hook.Start(bus)
	defer hook.Stop()

	bus.Emit(&conveyorv1.TaskEvent{Id: "task-1"})

	// One initial attempt plus one retry, then it gives up.
	require.Eventually(t, func() bool { return attempts.Load() == 2 }, 3*time.Second, 10*time.Millisecond)
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, int64(2), attempts.Load())
}

func TestWebhookDoesNotRetryClientError(t *testing.T) {
	var attempts atomic.Int64

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer server.Close()

	bus := NewEventBus(8, nil)
	hook := NewWebhook(WebhookConfig{URL: server.URL, MaxRetries: 5}, discardLogger())
	hook.Start(bus)
	defer hook.Stop()

	bus.Emit(&conveyorv1.TaskEvent{Id: "task-1"})

	require.Eventually(t, func() bool { return attempts.Load() == 1 }, 2*time.Second, 10*time.Millisecond)
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, int64(1), attempts.Load(), "a 4xx must not be retried")
}

func TestWebhookFilterAppliesToDelivery(t *testing.T) {
	delivered := make(chan string, 4)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		decoded := &conveyorv1.TaskEvent{}
		_ = protojson.Unmarshal(body, decoded)
		delivered <- decoded.GetQueue()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	bus := NewEventBus(8, nil)
	hook := NewWebhook(WebhookConfig{URL: server.URL, Filter: NewFilter([]string{"emails"}, nil)}, discardLogger())
	hook.Start(bus)
	defer hook.Stop()

	bus.Emit(&conveyorv1.TaskEvent{Id: "a", Queue: "payments"})
	bus.Emit(&conveyorv1.TaskEvent{Id: "b", Queue: "emails"})

	select {
	case got := <-delivered:
		assert.Equal(t, "emails", got)
	case <-time.After(2 * time.Second):
		t.Fatal("webhook did not deliver the filtered event")
	}

	select {
	case extra := <-delivered:
		t.Fatalf("expected only the emails event, also got %q", extra)
	case <-time.After(200 * time.Millisecond):
	}
}
