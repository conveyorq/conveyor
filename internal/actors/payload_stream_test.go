// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package actors

import (
	"context"
	"errors"
	"iter"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/conveyorq/conveyor/internal/broker"
	"github.com/conveyorq/conveyor/internal/broker/memory"
	"github.com/conveyorq/conveyor/internal/clock"
	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
)

// Fixed names shared by the payload delivery tests.
const (
	// payloadQueue is the paused queue tasks are leased from by hand.
	payloadQueue = "payload"
	// payloadTaskType is the type of every chunked test task.
	payloadTaskType = "test:payload"
	// payloadLeaseID is the lease every hand-leased delivery runs under.
	payloadLeaseID = "payload-lease"
	// payloadGroup is the aggregation group of the batch tests.
	payloadGroup = "G"
)

// errPayloadRead is the armed ReadPayload failure.
var errPayloadRead = errors.New("chunk store unreachable")

// payloadHarness is one engine and one gateway session over a fault broker,
// with the queue paused so deliveries are driven by hand.
type payloadHarness struct {
	// engine is the running engine node.
	engine *Engine
	// faults is the broker the engine reads from, with injectable failures.
	faults *faultBroker
	// recorder captures every frame sent to the session.
	recorder *frameRecorder
	// handle addresses the session's gateway.
	handle *GatewayHandle
}

// newPayloadHarness starts an engine and spawns one gateway whose session
// streams payloads or not, and handles batches of payloadTaskType.
func newPayloadHarness(t *testing.T, streaming bool) *payloadHarness {
	t.Helper()

	ctx := context.Background()
	inner := memory.New(clock.System())
	faults := newFaultBroker(inner)
	pauseQueue(t, faults, payloadQueue)

	engine := startEngine(t, faults)
	recorder := newFrameRecorder()

	handle, err := engine.SpawnGateway(ctx, GatewaySession{
		SessionID:        "session-payload",
		Queues:           []string{payloadQueue},
		Concurrency:      4,
		BatchTypes:       []string{payloadTaskType},
		PayloadStreaming: streaming,
	}, recorder)
	require.NoError(t, err)

	t.Cleanup(func() { _ = handle.Stop(ctx) })

	return &payloadHarness{engine: engine, faults: faults, recorder: recorder, handle: handle}
}

// chunksOf yields each string as one chunk.
func chunksOf(chunks ...string) iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {
		for _, chunk := range chunks {
			if !yield([]byte(chunk), nil) {
				return
			}
		}
	}
}

// dispatchChunked commits a chunked task, leases it by hand, and hands it to
// the gateway.
func (h *payloadHarness) dispatchChunked(t *testing.T, id string, chunks ...string) {
	t.Helper()

	ctx := context.Background()
	task := newTask(id, payloadQueue, payloadTaskType, 4)
	task.Payload = nil
	require.NoError(t, h.faults.EnqueueChunked(ctx, task, chunksOf(chunks...)))

	leased, err := h.faults.Lease(ctx, payloadQueue, 1, 30*time.Second, payloadLeaseID)
	require.NoError(t, err)
	require.Len(t, leased, 1)

	require.NoError(t, h.handle.Tell(ctx, &conveyorv1.ExecuteTask{
		Task:           leased[0],
		LeaseId:        payloadLeaseID,
		LeaseExpiresAt: timestamppb.New(h.engine.runtime.Clock().Now().Add(30 * time.Second)),
	}))
}

// dispatchChunkedBatch commits a group of chunked tasks, leases the group by
// hand, and hands it to the gateway as one batch.
func (h *payloadHarness) dispatchChunkedBatch(t *testing.T, chunksByID map[string][]string) {
	t.Helper()

	ctx := context.Background()

	for id, chunks := range chunksByID {
		task := groupedTask(id, payloadQueue, payloadTaskType, payloadGroup)
		task.Payload = nil
		require.NoError(t, h.faults.EnqueueChunked(ctx, task, chunksOf(chunks...)))
	}

	batch, err := h.faults.LeaseGroup(ctx, payloadQueue, payloadGroup, len(chunksByID), 30*time.Second, payloadLeaseID)
	require.NoError(t, err)
	require.Len(t, batch, len(chunksByID))

	require.NoError(t, h.handle.Tell(ctx, &conveyorv1.ExecuteBatch{
		Tasks:          batch,
		LeaseId:        payloadLeaseID,
		LeaseExpiresAt: timestamppb.New(h.engine.runtime.Clock().Now().Add(30 * time.Second)),
		Group:          payloadGroup,
	}))
}

// requireStreamedPayload waits for a task's closing chunk frame and returns the
// data frames before it joined.
func (h *payloadHarness) requireStreamedPayload(t *testing.T, id string) string {
	t.Helper()

	require.Eventually(t, func() bool {
		chunks := h.recorder.payloadChunks(id)

		return len(chunks) > 0 && chunks[len(chunks)-1].GetLast()
	}, 5*time.Second, 10*time.Millisecond, "the payload must be streamed and closed")

	var payload strings.Builder

	for _, chunk := range h.recorder.payloadChunks(id) {
		require.Empty(t, chunk.GetError())
		payload.Write(chunk.GetData())
	}

	return payload.String()
}

// requireRetriedForRead waits for a task to land in retry with the payload
// read failure recorded.
func (h *payloadHarness) requireRetriedForRead(t *testing.T, id string) {
	t.Helper()

	requireTaskState(t, h.engine, id, conveyorv1.TaskState_TASK_STATE_RETRY)

	stored, _, err := h.faults.GetTask(context.Background(), id)
	require.NoError(t, err)
	require.Contains(t, stored.GetLastError(), payloadReadFailurePrefix)
}

func TestGatewayStreamsChunkedPayloadAfterDispatch(t *testing.T) {
	harness := newPayloadHarness(t, true)
	harness.dispatchChunked(t, "task-1", "one", "two")

	dispatch := <-harness.recorder.dispatched
	require.True(t, dispatch.GetTask().GetPayloadChunked())
	require.Empty(t, dispatch.GetTask().GetPayload())
	require.Equal(t, "onetwo", harness.requireStreamedPayload(t, "task-1"))
}

func TestGatewayReassemblesChunkedPayloadForNonStreamingSession(t *testing.T) {
	harness := newPayloadHarness(t, false)
	harness.dispatchChunked(t, "task-1", "one", "two")

	dispatch := <-harness.recorder.dispatched
	require.False(t, dispatch.GetTask().GetPayloadChunked())
	require.Equal(t, "onetwo", string(dispatch.GetTask().GetPayload()))
	require.Empty(t, harness.recorder.payloadChunks("task-1"))
}

func TestGatewayStreamReadFailureRetriesTask(t *testing.T) {
	harness := newPayloadHarness(t, true)
	harness.faults.fault(methodReadPayload, errPayloadRead)
	harness.dispatchChunked(t, "task-1", "one")

	require.Eventually(t, func() bool {
		chunks := harness.recorder.payloadChunks("task-1")

		return len(chunks) == 1 && chunks[0].GetLast() && chunks[0].GetError() != ""
	}, 5*time.Second, 10*time.Millisecond, "a read failure must end the payload with an error frame")

	harness.requireRetriedForRead(t, "task-1")
}

func TestGatewayReassemblyReadFailureRetriesTask(t *testing.T) {
	harness := newPayloadHarness(t, false)
	harness.faults.fault(methodReadPayload, errPayloadRead)
	harness.dispatchChunked(t, "task-1", "one")

	harness.requireRetriedForRead(t, "task-1")
	require.Empty(t, harness.recorder.dispatched, "an unreadable payload must not be dispatched")
}

func TestGatewayChunkSendFailureLeavesTaskToDrain(t *testing.T) {
	harness := newPayloadHarness(t, true)
	harness.recorder.mutex.Lock()
	harness.recorder.chunkFailure = errors.New("stream closed")
	harness.recorder.mutex.Unlock()

	harness.dispatchChunked(t, "task-1", "one", "two")

	<-harness.recorder.dispatched
	require.Never(t, func() bool {
		return len(harness.recorder.payloadChunks("task-1")) > 0
	}, 200*time.Millisecond, 20*time.Millisecond, "no chunk reaches a broken stream")

	requireTaskState(t, harness.engine, "task-1", conveyorv1.TaskState_TASK_STATE_ACTIVE)
}

func TestGatewayStreamsChunkedBatchPayloads(t *testing.T) {
	harness := newPayloadHarness(t, true)
	harness.dispatchChunkedBatch(t, map[string][]string{"b-1": {"a", "b"}, "b-2": {"c"}})

	require.Eventually(t, func() bool {
		return batchDispatchFor(harness.recorder, payloadGroup) != nil
	}, 5*time.Second, 10*time.Millisecond, "the batch is dispatched first")

	require.Equal(t, "ab", harness.requireStreamedPayload(t, "b-1"))
	require.Equal(t, "c", harness.requireStreamedPayload(t, "b-2"))
}

func TestGatewayReassemblesChunkedBatchForNonStreamingSession(t *testing.T) {
	harness := newPayloadHarness(t, false)
	harness.dispatchChunkedBatch(t, map[string][]string{"b-1": {"a", "b"}, "b-2": {"c"}})

	require.Eventually(t, func() bool {
		return batchDispatchFor(harness.recorder, payloadGroup) != nil
	}, 5*time.Second, 10*time.Millisecond, "the reassembled batch is dispatched")

	payloads := make(map[string]string)
	for _, task := range batchDispatchFor(harness.recorder, payloadGroup).GetTasks() {
		require.False(t, task.GetPayloadChunked())
		payloads[task.GetId()] = string(task.GetPayload())
	}

	require.Equal(t, map[string]string{"b-1": "ab", "b-2": "c"}, payloads)
}

func TestGatewayBatchReadFailureRetriesEveryMember(t *testing.T) {
	harness := newPayloadHarness(t, false)
	harness.faults.fault(methodReadPayload, errPayloadRead)
	harness.dispatchChunkedBatch(t, map[string][]string{"b-1": {"a"}, "b-2": {"b"}})

	harness.requireRetriedForRead(t, "b-1")
	harness.requireRetriedForRead(t, "b-2")
}

func TestGatewayAssembledDispatchSendFailureReleasesTask(t *testing.T) {
	harness := newPayloadHarness(t, false)
	harness.recorder.fail(errors.New("stream closed"))
	harness.dispatchChunked(t, "task-1", "one")

	requireTaskState(t, harness.engine, "task-1", conveyorv1.TaskState_TASK_STATE_PENDING)
}

// chunkStrings returns n one-letter chunks.
func chunkStrings(n int) []string {
	chunks := make([]string, n)
	for index := range chunks {
		chunks[index] = "x"
	}

	return chunks
}

// dataChunks counts the data-bearing chunk frames sent for a task.
func (h *payloadHarness) dataChunks(taskID string) int {
	count := 0

	for _, chunk := range h.recorder.payloadChunks(taskID) {
		if !chunk.GetLast() {
			count++
		}
	}

	return count
}

func TestGatewayPausesAtTheChunkWindowUntilAcked(t *testing.T) {
	const extraChunks = 4

	harness := newPayloadHarness(t, true)
	harness.dispatchChunked(t, "task-1", chunkStrings(PayloadChunkWindow+extraChunks)...)

	require.Eventually(t, func() bool { return harness.dataChunks("task-1") == PayloadChunkWindow },
		5*time.Second, 10*time.Millisecond, "the gateway sends one window of chunks")
	require.Never(t, func() bool { return harness.dataChunks("task-1") > PayloadChunkWindow },
		200*time.Millisecond, 20*time.Millisecond, "and no more until the worker acks")

	ctx := context.Background()
	require.NoError(t, harness.handle.Tell(ctx, &conveyorv1.PayloadAck{TaskId: "task-unknown", Chunks: 1}), "an ack for an unknown task is ignored")
	require.NoError(t, harness.handle.Tell(ctx, &conveyorv1.PayloadAck{TaskId: "task-1", Chunks: extraChunks}))

	require.Equal(t, strings.Repeat("x", PayloadChunkWindow+extraChunks), harness.requireStreamedPayload(t, "task-1"))
}

func TestGatewayStopsStreamingOnceTheTaskResolves(t *testing.T) {
	harness := newPayloadHarness(t, true)
	harness.dispatchChunked(t, "task-1", chunkStrings(PayloadChunkWindow+1)...)

	require.Eventually(t, func() bool { return harness.dataChunks("task-1") == PayloadChunkWindow },
		5*time.Second, 10*time.Millisecond, "the first window is sent")

	ctx := context.Background()
	require.NoError(t, harness.handle.Tell(ctx, &conveyorv1.Result{TaskId: "task-1", Outcome: conveyorv1.TaskOutcome_TASK_OUTCOME_SUCCESS}))
	requireTaskState(t, harness.engine, "task-1", conveyorv1.TaskState_TASK_STATE_COMPLETED)

	require.NoError(t, harness.handle.Tell(ctx, &conveyorv1.PayloadAck{TaskId: "task-1", Chunks: PayloadChunkWindow}))
	require.Never(t, func() bool { return harness.dataChunks("task-1") > PayloadChunkWindow },
		200*time.Millisecond, 20*time.Millisecond, "a resolved task's payload is not streamed further")
}

func TestGatewayClosesAnEmptyChunkedPayload(t *testing.T) {
	harness := newPayloadHarness(t, true)
	harness.dispatchChunked(t, "task-1")

	require.Empty(t, harness.requireStreamedPayload(t, "task-1"))
	require.Zero(t, harness.dataChunks("task-1"))
}

func TestGatewayIgnoresARunFromAnEarlierStreamOfTheSameTask(t *testing.T) {
	const (
		earlierStream = 1
		currentStream = 2
		nextChunk     = 3
	)

	gateway, _ := faultGateway(t)
	current := &payloadStream{id: currentStream, next: nextChunk, total: 10, credit: PayloadChunkWindow, sending: true}
	gateway.payloadStreams = map[string]*payloadStream{"task-1": current}

	// The stale run is dropped before anything would be pumped, so no receive
	// context is needed.
	gateway.payloadSent(nil, chunksSent{taskID: "task-1", streamID: earlierStream, sent: PayloadChunkWindow})

	require.Equal(t, nextChunk, current.next, "a run of an ended stream must not advance the current one")
	require.Equal(t, PayloadChunkWindow, current.credit)
	require.True(t, current.sending)
}

func TestGatewayCapsAckedCreditAtTheWindow(t *testing.T) {
	gateway, _ := faultGateway(t)
	stream := &payloadStream{id: 1, total: 100, credit: 2, sending: true}
	gateway.payloadStreams = map[string]*payloadStream{"task-1": stream}

	// A run is in flight, so the ack only adds credit and pumps nothing: no
	// receive context is needed.
	gateway.payloadAck(nil, &conveyorv1.PayloadAck{TaskId: "task-1", Chunks: 100})

	require.Equal(t, PayloadChunkWindow, stream.credit, "an over-generous ack cannot outrun the worker's buffer")
}

func TestReadChunkedPayloadRenewsEveryWindowOfChunks(t *testing.T) {
	const chunkCount = 2*PayloadChunkWindow + 1

	ctx := context.Background()
	taskLog := memory.New(clock.System())
	task := newTask("task-1", payloadQueue, payloadTaskType, 4)
	task.Payload = nil
	require.NoError(t, taskLog.EnqueueChunked(ctx, task, chunksOf(chunkStrings(chunkCount)...)))

	stored, _, err := taskLog.GetTask(ctx, "task-1")
	require.NoError(t, err)

	renewals := 0
	payload, err := readChunkedPayload(ctx, taskLog, stored, func() error {
		renewals++

		return nil
	})
	require.NoError(t, err)
	require.Len(t, payload, chunkCount)
	require.Equal(t, 2, renewals, "the lease is renewed once per full window of chunks read")

	_, err = readChunkedPayload(ctx, taskLog, stored, func() error { return broker.ErrLeaseLost })
	require.ErrorIs(t, err, broker.ErrLeaseLost, "a lost lease ends the read")
}

func TestGatewayReassemblyStopsWhenTheLeaseIsLost(t *testing.T) {
	harness := newPayloadHarness(t, false)
	harness.faults.fault(methodExtendLease, broker.ErrLeaseLost)
	harness.dispatchChunked(t, "task-1", chunkStrings(PayloadChunkWindow+1)...)

	harness.requireRetriedForRead(t, "task-1")
	require.Empty(t, harness.recorder.dispatched, "a task whose lease was lost mid-reassembly is not dispatched")
}

func TestHasChunkedPayload(t *testing.T) {
	inline := &conveyorv1.TaskEnvelope{Payload: []byte("x")}
	chunked := &conveyorv1.TaskEnvelope{PayloadChunked: true}

	require.False(t, hasChunkedPayload())
	require.False(t, hasChunkedPayload(inline))
	require.True(t, hasChunkedPayload(inline, chunked))
}
