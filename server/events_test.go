// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"

	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
	"github.com/conveyorq/conveyor/internal/proto/conveyor/v1/conveyorv1connect"
	"github.com/conveyorq/conveyor/internal/wire"
)

// startEventsNode boots a dev-mode node, applying mutate to its config before
// start, and returns its base URL.
func startEventsNode(t *testing.T, mutate func(*Config)) string {
	t.Helper()

	ports := freePorts(t, 3)

	config := DevConfig()
	config.API.Listen = "127.0.0.1:0"
	config.Metrics.Listen = "127.0.0.1:0"
	config.Cluster.RemotingPort = ports[0]
	config.Cluster.DiscoveryPort = ports[1]
	config.Cluster.PeersPort = ports[2]

	if mutate != nil {
		mutate(config)
	}

	node, err := New(config, NewLogger(LogConfig{Level: LogLevelError, Format: LogFormatText}))
	require.NoError(t, err)
	require.NoError(t, node.Start(context.Background()))

	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = node.Stop(stopCtx)
	})

	return "http://" + node.Addr()
}

func TestWatchEventsStreamsTransitions(t *testing.T) {
	baseURL := startEventsNode(t, nil)

	admin := conveyorv1connect.NewAdminServiceClient(wire.NewH2CClient(), baseURL)
	tasks := conveyorv1connect.NewTaskServiceClient(wire.NewH2CClient(), baseURL)

	// Produce events continuously and concurrently: a server-streaming call
	// blocks until the first frame, so the producer must run independently of
	// opening the stream. This also rides past the relay's async topic
	// subscription at startup.
	stop := make(chan struct{})
	defer close(stop)

	go func() {
		for {
			select {
			case <-stop:
				return
			default:
			}

			_, _ = tasks.Enqueue(context.Background(), connect.NewRequest(&conveyorv1.EnqueueRequest{
				Queue: "default", Type: "demo",
			}))

			time.Sleep(100 * time.Millisecond)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	stream, err := admin.WatchEvents(ctx, connect.NewRequest(&conveyorv1.WatchEventsRequest{
		EventTypes: []conveyorv1.TaskEventType{conveyorv1.TaskEventType_TASK_EVENT_TYPE_ENQUEUED},
	}))
	require.NoError(t, err)

	require.True(t, stream.Receive(), "expected an event, stream error: %v", stream.Err())

	event := stream.Msg()
	assert.Equal(t, conveyorv1.TaskEventType_TASK_EVENT_TYPE_ENQUEUED, event.GetEventType())
	assert.Equal(t, "default", event.GetQueue())
	assert.Equal(t, "demo", event.GetType())
	assert.NotEmpty(t, event.GetId())

	require.NoError(t, stream.Close())
}

func TestWatchEventsDisabledReturnsUnavailable(t *testing.T) {
	baseURL := startEventsNode(t, func(config *Config) {
		config.Events.Enabled = false
	})

	admin := conveyorv1connect.NewAdminServiceClient(wire.NewH2CClient(), baseURL)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := admin.WatchEvents(ctx, connect.NewRequest(&conveyorv1.WatchEventsRequest{}))
	require.NoError(t, err)

	// The handler rejects with Unavailable; it surfaces on the first Receive.
	assert.False(t, stream.Receive())
	require.Error(t, stream.Err())
	assert.Equal(t, connect.CodeUnavailable, connect.CodeOf(stream.Err()))
}

func TestWebhookDeliversLifecycleEvents(t *testing.T) {
	delivered := make(chan *conveyorv1.TaskEvent, 8)

	hookServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		event := &conveyorv1.TaskEvent{}

		if protojson.Unmarshal(body, event) == nil {
			select {
			case delivered <- event:
			default:
			}
		}

		w.WriteHeader(http.StatusOK)
	}))
	defer hookServer.Close()

	baseURL := startEventsNode(t, func(config *Config) {
		config.Events.Webhook.URL = hookServer.URL
		config.Events.Webhook.EventTypes = []string{"TASK_EVENT_TYPE_ENQUEUED"}
	})

	tasks := conveyorv1connect.NewTaskServiceClient(wire.NewH2CClient(), baseURL)

	stop := make(chan struct{})
	defer close(stop)

	go func() {
		for {
			select {
			case <-stop:
				return
			default:
			}

			_, _ = tasks.Enqueue(context.Background(), connect.NewRequest(&conveyorv1.EnqueueRequest{
				Queue: "default", Type: "demo",
			}))

			time.Sleep(100 * time.Millisecond)
		}
	}()

	select {
	case event := <-delivered:
		assert.Equal(t, conveyorv1.TaskEventType_TASK_EVENT_TYPE_ENQUEUED, event.GetEventType())
		assert.Equal(t, "default", event.GetQueue())
	case <-time.After(15 * time.Second):
		t.Fatal("webhook did not receive any event")
	}
}
