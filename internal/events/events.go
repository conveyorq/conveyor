// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

// Package events is the in-process task lifecycle event fan-out: a bounded,
// non-blocking bus that brokers emit transitions into and that the
// WatchEvents stream and the webhook sink read from. Delivery is best-effort
// and non-durable by design — a watcher too slow to keep up has events
// dropped rather than stalling the producer, so emitting an event never backs
// up task processing.
package events

import (
	"sync"

	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
)

// DefaultBufferSize is the per-subscriber channel depth applied when a
// non-positive size is requested. It bounds how many events a slow watcher may
// fall behind before its further events are dropped.
const DefaultBufferSize = 1024

// Sink receives task lifecycle events. Implementations must be non-blocking:
// Emit is called from broker transition paths and must never stall them.
type Sink interface {
	// Emit delivers one event. A nil event is a no-op.
	Emit(event *conveyorv1.TaskEvent)
}

// Filter narrows an event subscription by queue and event type. Within each
// dimension the listed values are alternatives (any match passes); an empty
// dimension places no constraint. The zero Filter matches every event.
type Filter struct {
	// queues is the set of accepted queues, or nil for every queue.
	queues map[string]struct{}
	// eventTypes is the set of accepted transitions, or nil for every type.
	eventTypes map[conveyorv1.TaskEventType]struct{}
}

// NewFilter builds a Filter from the requested queues and event types. Empty
// (or nil) slices leave that dimension unconstrained; an UNSPECIFIED event type
// is ignored so it never silences the stream.
func NewFilter(queues []string, eventTypes []conveyorv1.TaskEventType) Filter {
	filter := Filter{}

	if len(queues) > 0 {
		filter.queues = make(map[string]struct{}, len(queues))

		for _, queue := range queues {
			filter.queues[queue] = struct{}{}
		}
	}

	if len(eventTypes) > 0 {
		accepted := make(map[conveyorv1.TaskEventType]struct{}, len(eventTypes))

		for _, eventType := range eventTypes {
			if eventType != conveyorv1.TaskEventType_TASK_EVENT_TYPE_UNSPECIFIED {
				accepted[eventType] = struct{}{}
			}
		}

		if len(accepted) > 0 {
			filter.eventTypes = accepted
		}
	}

	return filter
}

// matches reports whether the event passes both dimensions of the filter.
func (f Filter) matches(event *conveyorv1.TaskEvent) bool {
	if f.queues != nil {
		if _, ok := f.queues[event.GetQueue()]; !ok {
			return false
		}
	}

	if f.eventTypes != nil {
		if _, ok := f.eventTypes[event.GetEventType()]; !ok {
			return false
		}
	}

	return true
}

// subscription is one live watcher: its filter and its delivery channel.
type subscription struct {
	filter  Filter
	channel chan *conveyorv1.TaskEvent
}

// EventBus fans one stream of task lifecycle events out to many subscribers
// without ever blocking the emitter. Each subscriber has a bounded buffer;
// when it is full the subscriber's further events are dropped (and counted),
// so one stalled watcher cannot slow the dispatch path.
type EventBus struct {
	// bufferSize is the per-subscriber channel depth.
	bufferSize int
	// onDrop, when set, is invoked once per dropped event for metrics.
	onDrop func()

	// mutex guards subscribers and nextID.
	mutex sync.Mutex
	// subscribers maps a subscription id to its record.
	subscribers map[int]*subscription
	// nextID is the monotonic subscription id source.
	nextID int
}

// enforce interface compliance at compile time.
var _ Sink = (*EventBus)(nil)

// NewEventBus builds an event bus with the given per-subscriber buffer size
// (DefaultBufferSize when non-positive). onDrop, when non-nil, is called once
// for every event dropped because a subscriber's buffer was full.
func NewEventBus(bufferSize int, onDrop func()) *EventBus {
	if bufferSize <= 0 {
		bufferSize = DefaultBufferSize
	}

	return &EventBus{
		bufferSize:  bufferSize,
		onDrop:      onDrop,
		subscribers: make(map[int]*subscription),
	}
}

// Subscribe registers a watcher for events matching filter and returns its
// delivery channel and a cancel function. The channel is closed by cancel;
// callers must invoke cancel exactly when they stop reading (it is safe to
// call more than once). Events that arrive while the channel's buffer is full
// are dropped for this subscriber only.
func (b *EventBus) Subscribe(filter Filter) (<-chan *conveyorv1.TaskEvent, func()) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	id := b.nextID
	b.nextID++

	sub := &subscription{filter: filter, channel: make(chan *conveyorv1.TaskEvent, b.bufferSize)}
	b.subscribers[id] = sub

	var once sync.Once

	cancel := func() {
		once.Do(func() {
			b.mutex.Lock()
			defer b.mutex.Unlock()

			if _, ok := b.subscribers[id]; ok {
				delete(b.subscribers, id)
				close(sub.channel)
			}
		})
	}

	return sub.channel, cancel
}

// Emit delivers an event to every matching subscriber with a non-blocking
// send, dropping (and counting) it for any subscriber whose buffer is full. A
// nil event is a no-op. Emit never blocks, so it is safe on the broker's
// transition path. Cancel and Emit are mutually exclusive, so a send never
// races a channel close.
func (b *EventBus) Emit(event *conveyorv1.TaskEvent) {
	if event == nil {
		return
	}

	b.mutex.Lock()
	defer b.mutex.Unlock()

	for _, sub := range b.subscribers {
		if !sub.filter.matches(event) {
			continue
		}

		select {
		case sub.channel <- event:
		default:
			if b.onDrop != nil {
				b.onDrop()
			}
		}
	}
}
