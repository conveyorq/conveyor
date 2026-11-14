// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package actors

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	goakt "github.com/tochemey/goakt/v4/actor"

	"github.com/conveyorq/conveyor/internal/broker/memory"
	"github.com/conveyorq/conveyor/internal/clock"
	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
)

// frameKinds lists the kinds of the frames a recorder captured, in order.
func frameKinds(r *frameRecorder) []string {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	kinds := make([]string, 0, len(r.frames))

	for _, frame := range r.frames {
		switch {
		case frame.GetDispatch() != nil:
			kinds = append(kinds, "dispatch:"+frame.GetDispatch().GetTask().GetId())

		case frame.GetCancel() != nil:
			kinds = append(kinds, "cancel:"+frame.GetCancel().GetTaskId())
		}
	}

	return kinds
}

func TestGatewayOutboxKeepsFrameOrder(t *testing.T) {
	ctx := context.Background()
	taskLog := memory.New(clock.System())
	pauseQueue(t, taskLog, payloadQueue)
	engine := startEngine(t, taskLog)
	recorder := newFrameRecorder()

	handle, err := engine.SpawnGateway(ctx, GatewaySession{
		SessionID:   "session-outbox",
		Queues:      []string{payloadQueue},
		Concurrency: 4,
	}, recorder)
	require.NoError(t, err)

	t.Cleanup(func() { _ = handle.Stop(ctx) })

	require.NoError(t, handle.Tell(ctx, leaseOne(t, engine, payloadQueue, "task-1", "lease-1")))
	require.NoError(t, handle.Tell(ctx, &conveyorv1.CancelActive{TaskId: "task-1"}))

	require.Eventually(t, func() bool { return len(frameKinds(recorder)) == 2 }, 5*time.Second, 10*time.Millisecond)
	require.Equal(t, []string{"dispatch:task-1", "cancel:task-1"}, frameKinds(recorder),
		"a cancel decided after a dispatch reaches the worker after it")
}

func TestGatewayOutboxDropsFramesOfDeliveriesNoLongerInFlight(t *testing.T) {
	gateway, _ := faultGateway(t)
	recorder := newFrameRecorder()
	gateway.sender = recorder
	gateway.inflight = make(map[string]*inflightTask)
	gateway.outbox = []outboundFrame{{
		frame:    &conveyorv1.ServerMessage{Frame: &conveyorv1.ServerMessage_Dispatch{Dispatch: &conveyorv1.Dispatch{}}},
		delivery: &outboundDelivery{leaseID: "lease-1", taskIDs: []string{"task-gone"}},
	}}

	// Every queued frame is dropped, so no flush starts and no receive context
	// is needed.
	gateway.flushOutbox(nil)

	require.Empty(t, gateway.outbox)
	require.False(t, gateway.flushing)
	require.Empty(t, recorder.dispatched)
}

func TestGatewayLogsAnUndeliverableFrameWithoutADelivery(t *testing.T) {
	gateway, _ := faultGateway(t)
	gateway.inflight = make(map[string]*inflightTask)

	// A Cancel needs no follow-up: the failure is only logged.
	gateway.frameUndeliverable(nil, cancelFrame("task-1"), errors.New("stream closed"))
}

func TestGatewayIgnoresAFlushReportFromBeforeARestart(t *testing.T) {
	gateway, _ := faultGateway(t)
	gateway.inflight = make(map[string]*inflightTask)
	gateway.nextFlushID = 2
	gateway.flushing = true

	// The stale report is dropped before anything is applied or flushed, so
	// no receive context is needed.
	gateway.outboxFlushed(nil, framesFlushed{flushID: 1})

	require.True(t, gateway.flushing, "the current flush is still in flight")
}

func TestGatewayClearsOutboxStateOnStart(t *testing.T) {
	ctx := context.Background()
	system := startBareSystem(t, memory.New(clock.System()))
	recorder := newFrameRecorder()

	// As a restart that lost a flush's completion would leave it.
	gateway := newGateway(GatewaySession{SessionID: "session-restarted", Queues: []string{payloadQueue}, Concurrency: 1}, recorder)
	gateway.flushing = true
	gateway.outbox = []outboundFrame{cancelFrame("task-stale")}

	pid, err := system.Spawn(ctx, gatewayNamePrefix+"restarted", gateway)
	require.NoError(t, err)

	task := newTask("task-1", payloadQueue, "test:inline", 4)
	require.NoError(t, goakt.Tell(ctx, pid, &conveyorv1.ExecuteTask{Task: task, LeaseId: "lease-1"}))

	require.Eventually(t, func() bool { return len(frameKinds(recorder)) == 1 }, 5*time.Second, 10*time.Millisecond)
	require.Equal(t, []string{"dispatch:task-1"}, frameKinds(recorder), "a frame queued before the restart is never sent")
}
