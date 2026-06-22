// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package postgres

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

// captureSink records every emitted event for assertions.
type captureSink struct {
	mutex  sync.Mutex
	events []*conveyorv1.TaskEvent
}

func (s *captureSink) Emit(event *conveyorv1.TaskEvent) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.events = append(s.events, event)
}

func (s *captureSink) types() []conveyorv1.TaskEventType {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	out := []conveyorv1.TaskEventType{}
	for _, event := range s.events {
		out = append(out, event.GetEventType())
	}

	return out
}

func (s *captureSink) last() *conveyorv1.TaskEvent {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.events[len(s.events)-1]
}

func newEventBroker(t *testing.T) (*Broker, *captureSink) {
	t.Helper()

	broker, err := New(context.Background(), testDSN(), clock.NewFake(time.Unix(1_700_000_000, 0).UTC()))
	require.NoError(t, err)

	truncateAll(t)

	sink := &captureSink{}
	broker.SetEventSink(sink)

	t.Cleanup(func() { _ = broker.Close() })

	return broker, sink
}

// newEventBrokerWithClock builds an event-capturing broker on a caller-controlled
// fake clock, for tests that advance time.
func newEventBrokerWithClock(t *testing.T, fake *clock.Fake) (*Broker, *captureSink) {
	t.Helper()

	broker, err := New(context.Background(), testDSN(), fake)
	require.NoError(t, err)

	truncateAll(t)

	sink := &captureSink{}
	broker.SetEventSink(sink)

	t.Cleanup(func() { _ = broker.Close() })

	return broker, sink
}

func eventEnvelope(id string) *conveyorv1.TaskEnvelope {
	return &conveyorv1.TaskEnvelope{Id: id, Queue: "default", Type: "demo", Options: &conveyorv1.TaskOptions{MaxRetry: 3}}
}

func TestPostgresEmitsLeaseCompleteSequence(t *testing.T) {
	broker, sink := newEventBroker(t)
	ctx := context.Background()

	require.NoError(t, broker.Enqueue(ctx, eventEnvelope("t1")))

	leased, err := broker.Lease(ctx, "default", 10, time.Minute, "lease-1")
	require.NoError(t, err)
	require.Len(t, leased, 1)

	require.NoError(t, broker.Ack(ctx, "t1", "lease-1", nil))

	assert.Equal(t, []conveyorv1.TaskEventType{
		conveyorv1.TaskEventType_TASK_EVENT_TYPE_ENQUEUED,
		conveyorv1.TaskEventType_TASK_EVENT_TYPE_LEASED,
		conveyorv1.TaskEventType_TASK_EVENT_TYPE_COMPLETED,
	}, sink.types())

	last := sink.last()
	assert.Equal(t, "t1", last.GetId())
	assert.Equal(t, "default", last.GetQueue())
	assert.Equal(t, "demo", last.GetType())
}

func TestPostgresEmitsFailRetried(t *testing.T) {
	broker, sink := newEventBroker(t)
	ctx := context.Background()

	require.NoError(t, broker.Enqueue(ctx, eventEnvelope("t1")))
	_, err := broker.Lease(ctx, "default", 10, time.Minute, "lease-1")
	require.NoError(t, err)
	require.NoError(t, broker.Fail(ctx, "t1", "lease-1", "boom", time.Unix(1_700_000_100, 0)))

	last := sink.last()
	assert.Equal(t, conveyorv1.TaskEventType_TASK_EVENT_TYPE_RETRIED, last.GetEventType())
	assert.Equal(t, "boom", last.GetLastError())
	assert.Equal(t, int32(1), last.GetAttempt())
}

func TestPostgresEmitsCancel(t *testing.T) {
	broker, sink := newEventBroker(t)
	ctx := context.Background()

	require.NoError(t, broker.Enqueue(ctx, eventEnvelope("t1")))
	require.NoError(t, broker.CancelTask(ctx, "t1"))

	assert.Equal(t, conveyorv1.TaskEventType_TASK_EVENT_TYPE_CANCELED, sink.last().GetEventType())
}

func TestPostgresEmitsReapArchive(t *testing.T) {
	fake := clock.NewFake(time.Unix(1_700_000_000, 0).UTC())
	broker, sink := newEventBrokerWithClock(t, fake)

	ctx := context.Background()
	task := eventEnvelope("t1")
	task.Options.MaxRetry = 0

	require.NoError(t, broker.Enqueue(ctx, task))
	_, err := broker.Lease(ctx, "default", 10, time.Minute, "lease-1")
	require.NoError(t, err)

	// Advance past the lease TTL so the reaper reclaims it; retries are exhausted
	// (max_retry 0), so the task is archived rather than retried.
	fake.Advance(2 * time.Minute)
	_, err = broker.ReapExpiredLeases(ctx, 10)
	require.NoError(t, err)

	assert.Equal(t, conveyorv1.TaskEventType_TASK_EVENT_TYPE_ARCHIVED, sink.last().GetEventType())
}

func TestPostgresEmitsScheduledThenPromote(t *testing.T) {
	fake := clock.NewFake(time.Unix(1_700_000_000, 0).UTC())
	broker, sink := newEventBrokerWithClock(t, fake)

	ctx := context.Background()
	task := eventEnvelope("t1")
	task.Options.ProcessAt = timestamppb.New(fake.Now().Add(time.Hour))

	require.NoError(t, broker.Enqueue(ctx, task))
	assert.Equal(t, conveyorv1.TaskEventType_TASK_EVENT_TYPE_SCHEDULED, sink.last().GetEventType())

	fake.Advance(2 * time.Hour)
	_, err := broker.PromoteScheduled(ctx, 10)
	require.NoError(t, err)

	assert.Equal(t, conveyorv1.TaskEventType_TASK_EVENT_TYPE_ENQUEUED, sink.last().GetEventType())
	assert.Equal(t, conveyorv1.TaskState_TASK_STATE_PENDING, sink.last().GetState())
}
