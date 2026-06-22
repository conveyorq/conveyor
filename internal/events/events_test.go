// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package events

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
)

// event builds a minimal TaskEvent for bus tests.
func event(queue string, eventType conveyorv1.TaskEventType) *conveyorv1.TaskEvent {
	return &conveyorv1.TaskEvent{Id: "t", Queue: queue, EventType: eventType}
}

func TestEventBusDeliversToMatchingSubscribers(t *testing.T) {
	bus := NewEventBus(8, nil)

	first, cancelFirst := bus.Subscribe(Filter{})
	defer cancelFirst()

	second, cancelSecond := bus.Subscribe(Filter{})
	defer cancelSecond()

	sent := event("default", conveyorv1.TaskEventType_TASK_EVENT_TYPE_ENQUEUED)
	bus.Emit(sent)

	assert.Same(t, sent, <-first)
	assert.Same(t, sent, <-second)
}

func TestEventBusEmitNilIsNoop(t *testing.T) {
	bus := NewEventBus(8, nil)

	channel, cancel := bus.Subscribe(Filter{})
	defer cancel()

	bus.Emit(nil)

	select {
	case got := <-channel:
		t.Fatalf("expected no delivery for nil event, got %v", got)
	default:
	}
}

func TestEventBusFilterByQueue(t *testing.T) {
	bus := NewEventBus(8, nil)

	channel, cancel := bus.Subscribe(NewFilter([]string{"emails"}, nil))
	defer cancel()

	bus.Emit(event("payments", conveyorv1.TaskEventType_TASK_EVENT_TYPE_ENQUEUED))
	bus.Emit(event("emails", conveyorv1.TaskEventType_TASK_EVENT_TYPE_ENQUEUED))

	got := <-channel
	assert.Equal(t, "emails", got.GetQueue())

	select {
	case extra := <-channel:
		t.Fatalf("expected only the emails event, also got %v", extra.GetQueue())
	default:
	}
}

func TestEventBusFilterByEventType(t *testing.T) {
	bus := NewEventBus(8, nil)

	channel, cancel := bus.Subscribe(NewFilter(nil, []conveyorv1.TaskEventType{conveyorv1.TaskEventType_TASK_EVENT_TYPE_ARCHIVED}))
	defer cancel()

	bus.Emit(event("default", conveyorv1.TaskEventType_TASK_EVENT_TYPE_COMPLETED))
	bus.Emit(event("default", conveyorv1.TaskEventType_TASK_EVENT_TYPE_ARCHIVED))

	got := <-channel
	assert.Equal(t, conveyorv1.TaskEventType_TASK_EVENT_TYPE_ARCHIVED, got.GetEventType())

	select {
	case extra := <-channel:
		t.Fatalf("expected only the archived event, also got %v", extra.GetEventType())
	default:
	}
}

func TestNewFilterIgnoresUnspecifiedEventType(t *testing.T) {
	// A filter naming only UNSPECIFIED must not silence the stream.
	bus := NewEventBus(8, nil)

	channel, cancel := bus.Subscribe(NewFilter(nil, []conveyorv1.TaskEventType{conveyorv1.TaskEventType_TASK_EVENT_TYPE_UNSPECIFIED}))
	defer cancel()

	bus.Emit(event("default", conveyorv1.TaskEventType_TASK_EVENT_TYPE_ENQUEUED))

	assert.Equal(t, conveyorv1.TaskEventType_TASK_EVENT_TYPE_ENQUEUED, (<-channel).GetEventType())
}

func TestEventBusDropsWhenBufferFull(t *testing.T) {
	var drops atomic.Int64

	bus := NewEventBus(1, func() { drops.Add(1) })

	channel, cancel := bus.Subscribe(Filter{})
	defer cancel()

	// First fills the single-slot buffer; the next two have nowhere to go.
	bus.Emit(event("default", conveyorv1.TaskEventType_TASK_EVENT_TYPE_ENQUEUED))
	bus.Emit(event("default", conveyorv1.TaskEventType_TASK_EVENT_TYPE_ENQUEUED))
	bus.Emit(event("default", conveyorv1.TaskEventType_TASK_EVENT_TYPE_ENQUEUED))

	assert.Equal(t, int64(2), drops.Load())

	got := <-channel
	require.NotNil(t, got)
}

func TestEventBusUnsubscribeClosesChannel(t *testing.T) {
	bus := NewEventBus(8, nil)

	channel, cancel := bus.Subscribe(Filter{})
	cancel()

	_, open := <-channel
	assert.False(t, open, "channel should be closed after cancel")

	// A second cancel is safe, and emitting after cancel does not panic.
	cancel()
	bus.Emit(event("default", conveyorv1.TaskEventType_TASK_EVENT_TYPE_ENQUEUED))
}
