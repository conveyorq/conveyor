// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package memory

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/conveyorq/conveyor/internal/clock"
	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
)

// capturingSink records every emitted event for assertions.
type capturingSink struct {
	mutex  sync.Mutex
	events []*conveyorv1.TaskEvent
}

func (s *capturingSink) Emit(event *conveyorv1.TaskEvent) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.events = append(s.events, event)
}

func (s *capturingSink) snapshot() []*conveyorv1.TaskEvent {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return append([]*conveyorv1.TaskEvent(nil), s.events...)
}

// types returns the event types in emission order.
func (s *capturingSink) types() []conveyorv1.TaskEventType {
	out := []conveyorv1.TaskEventType{}
	for _, event := range s.snapshot() {
		out = append(out, event.GetEventType())
	}

	return out
}

func newEventBroker(t *testing.T) (*Broker, *capturingSink, *clock.Fake) {
	t.Helper()

	fake := clock.NewFake(time.Unix(1_700_000_000, 0).UTC())
	broker := New(fake)
	sink := &capturingSink{}
	broker.SetEventSink(sink)

	t.Cleanup(func() { _ = broker.Close() })

	return broker, sink, fake
}

func envelope(id, queue string) *conveyorv1.TaskEnvelope {
	return &conveyorv1.TaskEnvelope{Id: id, Queue: queue, Type: "demo", Options: &conveyorv1.TaskOptions{MaxRetry: 3}}
}

func TestEmitEnqueuePending(t *testing.T) {
	broker, sink, _ := newEventBroker(t)

	require.NoError(t, broker.Enqueue(context.Background(), envelope("t1", "default")))

	events := sink.snapshot()
	require.Len(t, events, 1)
	assert.Equal(t, conveyorv1.TaskEventType_TASK_EVENT_TYPE_ENQUEUED, events[0].GetEventType())
	assert.Equal(t, conveyorv1.TaskState_TASK_STATE_PENDING, events[0].GetState())
	assert.Equal(t, "t1", events[0].GetId())
	assert.Equal(t, "default", events[0].GetQueue())
	assert.Equal(t, "demo", events[0].GetType())
}

func TestEmitEnqueueScheduled(t *testing.T) {
	broker, sink, fake := newEventBroker(t)

	task := envelope("t1", "default")
	task.Options.ProcessAt = timestamppb.New(fake.Now().Add(time.Hour))

	require.NoError(t, broker.Enqueue(context.Background(), task))

	require.Len(t, sink.snapshot(), 1)
	assert.Equal(t, conveyorv1.TaskEventType_TASK_EVENT_TYPE_SCHEDULED, sink.snapshot()[0].GetEventType())
}

func TestEmitLeaseCompleteSequence(t *testing.T) {
	broker, sink, _ := newEventBroker(t)
	ctx := context.Background()

	require.NoError(t, broker.Enqueue(ctx, envelope("t1", "default")))

	leased, err := broker.Lease(ctx, "default", 10, time.Minute, "lease-1")
	require.NoError(t, err)
	require.Len(t, leased, 1)

	require.NoError(t, broker.Ack(ctx, "t1", "lease-1", nil))

	assert.Equal(t, []conveyorv1.TaskEventType{
		conveyorv1.TaskEventType_TASK_EVENT_TYPE_ENQUEUED,
		conveyorv1.TaskEventType_TASK_EVENT_TYPE_LEASED,
		conveyorv1.TaskEventType_TASK_EVENT_TYPE_COMPLETED,
	}, sink.types())
}

func TestEmitFailRetried(t *testing.T) {
	broker, sink, fake := newEventBroker(t)
	ctx := context.Background()

	require.NoError(t, broker.Enqueue(ctx, envelope("t1", "default")))
	_, err := broker.Lease(ctx, "default", 10, time.Minute, "lease-1")
	require.NoError(t, err)
	require.NoError(t, broker.Fail(ctx, "t1", "lease-1", "boom", fake.Now().Add(time.Minute)))

	last := sink.snapshot()[len(sink.snapshot())-1]
	assert.Equal(t, conveyorv1.TaskEventType_TASK_EVENT_TYPE_RETRIED, last.GetEventType())
	assert.Equal(t, "boom", last.GetLastError())
	assert.Equal(t, int32(1), last.GetAttempt())
}

func TestEmitCancel(t *testing.T) {
	broker, sink, _ := newEventBroker(t)
	ctx := context.Background()

	require.NoError(t, broker.Enqueue(ctx, envelope("t1", "default")))
	require.NoError(t, broker.CancelTask(ctx, "t1"))

	last := sink.snapshot()[len(sink.snapshot())-1]
	assert.Equal(t, conveyorv1.TaskEventType_TASK_EVENT_TYPE_CANCELED, last.GetEventType())
}

func TestEmitReapArchivesExhausted(t *testing.T) {
	broker, sink, fake := newEventBroker(t)
	ctx := context.Background()

	task := envelope("t1", "default")
	task.Options.MaxRetry = 0
	task.Retried = 0

	require.NoError(t, broker.Enqueue(ctx, task))
	_, err := broker.Lease(ctx, "default", 10, time.Minute, "lease-1")
	require.NoError(t, err)

	fake.Advance(2 * time.Minute)
	_, err = broker.ReapExpiredLeases(ctx, 10)
	require.NoError(t, err)

	last := sink.snapshot()[len(sink.snapshot())-1]
	assert.Equal(t, conveyorv1.TaskEventType_TASK_EVENT_TYPE_ARCHIVED, last.GetEventType())
}

func TestNoSinkIsNoop(t *testing.T) {
	broker := New(clock.NewFake(time.Unix(1, 0)))
	require.NoError(t, broker.Enqueue(context.Background(), envelope("t1", "default")))
}

func TestEmitWithoutSinkDoesNotAllocate(t *testing.T) {
	// With events disabled (no sink) a transition must build no event, so the
	// emit path is allocation-free — the broker pays nothing for events it does
	// not deliver.
	broker := New(clock.NewFake(time.Unix(1, 0)))
	now := time.Unix(1, 0)

	allocs := testing.AllocsPerRun(1000, func() {
		broker.emit(conveyorv1.TaskState_TASK_STATE_ACTIVE, conveyorv1.TaskState_TASK_STATE_COMPLETED,
			"t1", "default", "demo", "", 0, now)
	})

	assert.Zero(t, allocs, "emit must not allocate when no sink is configured")
}
