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
	goakt "github.com/tochemey/goakt/v4/actor"
	goaktlog "github.com/tochemey/goakt/v4/log"

	"github.com/conveyorq/conveyor/internal/broker/memory"
	"github.com/conveyorq/conveyor/internal/clock"
	"github.com/conveyorq/conveyor/internal/events"
	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
)

func TestEventRelayPreStartRequiresRuntimeExtension(t *testing.T) {
	ctx := context.Background()

	system, err := goakt.NewActorSystem("bare-relay-system", goakt.WithLogger(goaktlog.DiscardLogger))
	require.NoError(t, err)
	require.NoError(t, system.Start(ctx))

	t.Cleanup(func() { _ = system.Stop(ctx) })

	_, err = system.Spawn(ctx, "relay-no-runtime", &eventRelay{})
	require.ErrorContains(t, err, "is not registered")
}

func TestEventRelayIgnoresUnknownMessage(t *testing.T) {
	requireUnhandled(t, spawnIsolated(t, "extra-relay", &eventRelay{}), new(conveyorv1.ReapTick))
}

func TestTopicSinkEmitIgnoresNilEvent(t *testing.T) {
	require.NotPanics(t, func() { (&topicSink{}).Emit(nil) })
}

func TestTopicSinkEmitDropsWithoutTopicActor(t *testing.T) {
	ctx := context.Background()

	system, err := goakt.NewActorSystem("sink-no-topic", goakt.WithLogger(goaktlog.DiscardLogger))
	require.NoError(t, err)
	require.NoError(t, system.Start(ctx))

	t.Cleanup(func() { _ = system.Stop(ctx) })

	sink := &topicSink{system: system, newID: func() string { return "id" }, logger: quietLogger()}

	require.NotPanics(t, func() { sink.Emit(&conveyorv1.TaskEvent{Id: "e"}) })
}

func TestTopicSinkEmitDropsOnTellFailure(t *testing.T) {
	ctx := context.Background()

	settings := testSettings
	settings.EventsEnabled = true

	engine := newNode(memory.New(clock.System()), settings, freePorts(t, 3), nil)
	require.NoError(t, engine.Start(ctx))

	topicActor := engine.System().TopicActor()
	require.NotNil(t, topicActor)

	sink := &topicSink{system: engine.System(), newID: func() string { return "id" }, logger: quietLogger()}

	// Stopping the engine stops the topic actor; the system still reports it, so
	// the publish Tell reaches a non-running actor and fails.
	stopCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	require.NoError(t, engine.Stop(stopCtx))
	require.False(t, topicActor.IsRunning())

	require.NotPanics(t, func() { sink.Emit(&conveyorv1.TaskEvent{Id: "e"}) })
}

func TestEventRelayPostStartWithoutTopicActor(t *testing.T) {
	ctx := context.Background()

	runtime := NewRuntime(memory.New(clock.System()), clock.System(), testSettings, quietLogger())

	system, err := goakt.NewActorSystem("relay-no-topic",
		goakt.WithLogger(goaktlog.DiscardLogger), goakt.WithExtensions(runtime))
	require.NoError(t, err)
	require.NoError(t, system.Start(ctx))

	t.Cleanup(func() { _ = system.Stop(ctx) })

	pid, err := system.Spawn(ctx, "relay", &eventRelay{})
	require.NoError(t, err)
	require.True(t, pid.IsRunning())

	// PostStart is delivered asynchronously; give it a turn to run the
	// no-topic-actor branch before the system is torn down.
	time.Sleep(200 * time.Millisecond)
}

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
