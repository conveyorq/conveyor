// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package actors

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/conveyorq/conveyor/internal/broker/memory"
	"github.com/conveyorq/conveyor/internal/clock"
	"github.com/conveyorq/conveyor/internal/events"
	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
)

// TestEngineStreamsLifecycleEvents proves the full node-local path: a broker
// transition is published to the cluster topic, the relay republishes it into
// the node-local bus, and a WatchEvents-style subscriber receives it.
func TestEngineStreamsLifecycleEvents(t *testing.T) {
	settings := testSettings
	settings.EventsEnabled = true

	taskLog := memory.New(clock.System())
	engine := newNode(taskLog, settings, freePorts(t, 3), nil)
	require.NoError(t, engine.Start(context.Background()))

	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = engine.Stop(stopCtx)
	})

	// Wire the broker to publish through the engine's topic sink, as the server does.
	taskLog.SetEventSink(engine.EventSink())

	channel, cancel := engine.EventBus().Subscribe(events.NewFilter(nil, []conveyorv1.TaskEventType{
		conveyorv1.TaskEventType_TASK_EVENT_TYPE_ENQUEUED,
	}))
	defer cancel()

	// The relay subscribes to the topic asynchronously on start; enqueue distinct
	// tasks until one event lands, then assert its shape.
	stop := make(chan struct{})
	defer close(stop)

	go func() {
		for index := 0; ; index++ {
			select {
			case <-stop:
				return
			default:
			}

			_ = engine.Enqueue(context.Background(), &conveyorv1.TaskEnvelope{
				Id: fmt.Sprintf("evt-%d", index), Queue: "default", Type: "demo",
				Options: &conveyorv1.TaskOptions{MaxRetry: 1},
			})

			time.Sleep(50 * time.Millisecond)
		}
	}()

	select {
	case got := <-channel:
		assert.Equal(t, conveyorv1.TaskEventType_TASK_EVENT_TYPE_ENQUEUED, got.GetEventType())
		assert.Equal(t, "default", got.GetQueue())
		assert.Equal(t, "demo", got.GetType())
	case <-time.After(10 * time.Second):
		t.Fatal("no lifecycle event received via the topic relay")
	}
}

// TestEngineEventSinkNilWhenDisabled verifies events stay off by default.
func TestEngineEventSinkNilWhenDisabled(t *testing.T) {
	taskLog := memory.New(clock.System())
	engine := startEngine(t, taskLog)

	assert.Nil(t, engine.EventSink(), "event sink must be nil when events are disabled")
	assert.NotNil(t, engine.EventBus(), "event bus is always present")
}
