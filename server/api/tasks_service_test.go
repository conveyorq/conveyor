// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"context"
	"errors"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/conveyorq/conveyor/internal/broker"
	"github.com/conveyorq/conveyor/internal/broker/memory"
	"github.com/conveyorq/conveyor/internal/clock"
	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
	"github.com/conveyorq/conveyor/internal/proto/conveyor/v1/conveyorv1connect"
)

// newTestTaskService builds a TaskService over a fresh engine.
func newTestTaskService(t *testing.T) *TaskService {
	t.Helper()

	engine, taskLog := startTestEngine(t)

	return NewTaskService(engine, taskLog, clock.System(), testDefaultMaxRetry)
}

// TestMaxRequestBytesAdmitsFullBatch locks the wire read limit that the server
// applies via connect.WithReadMaxBytes: it must be large enough to admit the
// largest legitimate request — a full EnqueueBatch of maxBatchTasks tasks each
// at the inline payload limit — with room for per-task field overhead, so
// hardening the server against unbounded input never rejects a valid maximum
// batch. The ~1 GiB boundary itself is not exercised end to end, since sending
// that much in a test would exhaust the runner.
func TestMaxRequestBytesAdmitsFullBatch(t *testing.T) {
	fullBatchPayload := maxBatchTasks * broker.InlinePayloadBytes

	require.Greater(t, MaxRequestBytes, fullBatchPayload,
		"the read limit must admit a full batch of inline-limit tasks plus field overhead")
	require.Equal(t, maxBatchTasks*(broker.InlinePayloadBytes+perTaskOverheadBytes), MaxRequestBytes)
	require.Greater(t, MaxRequestBytes, maxPayloadChunkBytes, "the read limit must admit one upload chunk")
}

// TestMaxMessageBytesAdmitsOneTask locks the single-message read limit to one
// task at the inline payload limit plus field overhead.
func TestMaxMessageBytesAdmitsOneTask(t *testing.T) {
	require.Equal(t, broker.InlinePayloadBytes+perTaskOverheadBytes, MaxMessageBytes)
}

// TestEnqueueEnforcesBrokerPayloadCap checks that the enqueue path reads the
// payload cap from the broker: a payload at the cap is accepted and one byte
// over is rejected as invalid_argument.
func TestEnqueueEnforcesBrokerPayloadCap(t *testing.T) {
	const smallPayloadCap = 16

	engine, taskLog := startTestEngineWithBroker(t, memory.NewWithMaxPayload(clock.System(), smallPayloadCap))
	service := NewTaskService(engine, taskLog, clock.System(), testDefaultMaxRetry)
	ctx := context.Background()

	_, err := service.Enqueue(ctx, connect.NewRequest(&conveyorv1.EnqueueRequest{
		Type:    "test:at-cap",
		Payload: make([]byte, smallPayloadCap),
	}))
	require.NoError(t, err)

	_, err = service.Enqueue(ctx, connect.NewRequest(&conveyorv1.EnqueueRequest{
		Type:    "test:over-cap",
		Payload: make([]byte, smallPayloadCap+1),
	}))
	require.Error(t, err)
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
}

func TestEnqueueValidation(t *testing.T) {
	service := newTestTaskService(t)
	ctx := context.Background()

	cases := map[string]*conveyorv1.EnqueueRequest{
		"missing type":   {Queue: "default"},
		"bad queue name": {Type: "test:ok", Queue: "no spaces allowed"},
		"both delays": {
			Type:      "test:ok",
			ProcessAt: timestamppb.New(clock.System().Now().Add(time.Hour)),
			ProcessIn: durationpb.New(time.Hour),
		},
		"negative max_retry": {Type: "test:ok", MaxRetry: -1},
		"negative priority":  {Type: "test:ok", Priority: -1},
		"priority too high":  {Type: "test:ok", Priority: 10},
		"oversized payload":  {Type: "test:ok", Payload: make([]byte, broker.DefaultMaxPayloadBytes+1)},
	}

	for name, request := range cases {
		_, err := service.Enqueue(ctx, connect.NewRequest(request))
		require.Error(t, err, "case %s", name)
		require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err), "case %s", name)
	}
}

func TestEnqueueAppliesDefaults(t *testing.T) {
	service := newTestTaskService(t)

	response, err := service.Enqueue(context.Background(), connect.NewRequest(&conveyorv1.EnqueueRequest{
		Type: "test:defaults",
	}))
	require.NoError(t, err)

	task := response.Msg.GetTask()
	require.NotEmpty(t, task.GetId())
	require.Equal(t, defaultQueueName, task.GetQueue())
	require.EqualValues(t, defaultPriority, task.GetPriority())
	require.Equal(t, testDefaultMaxRetry, task.GetMaxRetry())
	require.Equal(t, conveyorv1.TaskState_TASK_STATE_PENDING, task.GetState())
	require.True(t, task.GetEnqueuedAt().IsValid())
}

func TestEnqueueScheduledStates(t *testing.T) {
	service := newTestTaskService(t)
	ctx := context.Background()

	delayed, err := service.Enqueue(ctx, connect.NewRequest(&conveyorv1.EnqueueRequest{
		Type:      "test:later",
		ProcessIn: durationpb.New(time.Hour),
	}))
	require.NoError(t, err)
	require.Equal(t, conveyorv1.TaskState_TASK_STATE_SCHEDULED, delayed.Msg.GetTask().GetState())
	require.True(t, delayed.Msg.GetTask().GetProcessAt().IsValid())

	absolute, err := service.Enqueue(ctx, connect.NewRequest(&conveyorv1.EnqueueRequest{
		Type:      "test:later-absolute",
		ProcessAt: timestamppb.New(clock.System().Now().Add(time.Hour)),
	}))
	require.NoError(t, err)
	require.Equal(t, conveyorv1.TaskState_TASK_STATE_SCHEDULED, absolute.Msg.GetTask().GetState())
}

func TestEnqueueExpiry(t *testing.T) {
	service := newTestTaskService(t)
	ctx := context.Background()

	relative, err := service.Enqueue(ctx, connect.NewRequest(&conveyorv1.EnqueueRequest{
		Type:      "test:expires-relative",
		ExpiresIn: durationpb.New(time.Hour),
	}))
	require.NoError(t, err)
	require.Equal(t, conveyorv1.TaskState_TASK_STATE_PENDING, relative.Msg.GetTask().GetState())

	_, err = service.Enqueue(ctx, connect.NewRequest(&conveyorv1.EnqueueRequest{
		Type:      "test:expires-absolute",
		ExpiresAt: timestamppb.New(clock.System().Now().Add(time.Hour)),
	}))
	require.NoError(t, err)

	_, err = service.Enqueue(ctx, connect.NewRequest(&conveyorv1.EnqueueRequest{
		Type:      "test:expires-both",
		ExpiresIn: durationpb.New(time.Hour),
		ExpiresAt: timestamppb.New(clock.System().Now().Add(time.Hour)),
	}))
	require.ErrorContains(t, err, "expires_at and expires_in are mutually exclusive")
}

func TestEnqueueWithDependencies(t *testing.T) {
	service := newTestTaskService(t)
	ctx := context.Background()

	dependency, err := service.Enqueue(ctx, connect.NewRequest(&conveyorv1.EnqueueRequest{
		TaskId: "dep-1",
		Type:   "test:dependency",
	}))
	require.NoError(t, err)
	require.Equal(t, conveyorv1.TaskState_TASK_STATE_PENDING, dependency.Msg.GetTask().GetState())

	dependent, err := service.Enqueue(ctx, connect.NewRequest(&conveyorv1.EnqueueRequest{
		Type:      "test:dependent",
		DependsOn: []*conveyorv1.TaskDependency{{TaskId: "dep-1"}},
	}))
	require.NoError(t, err)
	require.Equal(t, conveyorv1.TaskState_TASK_STATE_BLOCKED, dependent.Msg.GetTask().GetState())
}

func TestEnqueueConcurrencyKeyMapping(t *testing.T) {
	service := newTestTaskService(t)

	envelope, err := service.envelopeFromRequest(&conveyorv1.EnqueueRequest{
		Type:           "test:work",
		ConcurrencyKey: "tenant-42",
	})
	require.NoError(t, err)
	require.Equal(t, "tenant-42", envelope.GetOptions().GetConcurrencyKey())
}

func TestEnqueueRejectsGroupWithConcurrencyKey(t *testing.T) {
	service := newTestTaskService(t)

	_, err := service.Enqueue(context.Background(), connect.NewRequest(&conveyorv1.EnqueueRequest{
		Type:           "test:work",
		Group:          "g",
		ConcurrencyKey: "k",
	}))
	require.ErrorContains(t, err, "group and concurrency_key are mutually exclusive")
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
}

func TestEnqueueDependencyValidation(t *testing.T) {
	service := newTestTaskService(t)
	ctx := context.Background()

	_, err := service.Enqueue(ctx, connect.NewRequest(&conveyorv1.EnqueueRequest{
		Type:      "test:empty-dep",
		DependsOn: []*conveyorv1.TaskDependency{{TaskId: ""}},
	}))
	require.ErrorContains(t, err, "must name a task id")
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

	tooMany := make([]*conveyorv1.TaskDependency, maxDependencies+1)
	for index := range tooMany {
		tooMany[index] = &conveyorv1.TaskDependency{TaskId: "dep"}
	}

	_, err = service.Enqueue(ctx, connect.NewRequest(&conveyorv1.EnqueueRequest{
		Type:      "test:too-many-deps",
		DependsOn: tooMany,
	}))
	require.ErrorContains(t, err, "at most")
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
}

func TestEnqueueDuplicateUniqueKey(t *testing.T) {
	service := newTestTaskService(t)
	ctx := context.Background()

	request := &conveyorv1.EnqueueRequest{
		Type:      "test:unique",
		UniqueKey: "user:42:welcome",
		UniqueTtl: durationpb.New(time.Hour),
	}

	_, err := service.Enqueue(ctx, connect.NewRequest(request))
	require.NoError(t, err)

	_, err = service.Enqueue(ctx, connect.NewRequest(request))
	require.Error(t, err)
	require.Equal(t, connect.CodeAlreadyExists, connect.CodeOf(err))
}

func TestEnqueueBatch(t *testing.T) {
	service := newTestTaskService(t)
	ctx := context.Background()

	_, err := service.EnqueueBatch(ctx, connect.NewRequest(&conveyorv1.EnqueueBatchRequest{}))
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

	oversized := &conveyorv1.EnqueueBatchRequest{Tasks: make([]*conveyorv1.EnqueueRequest, maxBatchTasks+1)}
	_, err = service.EnqueueBatch(ctx, connect.NewRequest(oversized))
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	require.ErrorContains(t, err, "split it into smaller batches")

	response, err := service.EnqueueBatch(ctx, connect.NewRequest(&conveyorv1.EnqueueBatchRequest{
		Tasks: []*conveyorv1.EnqueueRequest{
			{Type: "test:ok"},
			{Queue: "default"}, // missing type: fails alone
			{Type: "test:ok-2"},
		},
	}))
	require.NoError(t, err)

	results := response.Msg.GetResults()
	require.Len(t, results, 3)
	require.NotNil(t, results[0].GetTask())
	require.Empty(t, results[0].GetError())
	require.Nil(t, results[1].GetTask())
	require.Contains(t, results[1].GetError(), "task type is required")
	require.NotNil(t, results[2].GetTask())
}

func TestEnqueueTx(t *testing.T) {
	service := newTestTaskService(t)
	ctx := context.Background()

	_, err := service.EnqueueTx(ctx, connect.NewRequest(&conveyorv1.EnqueueTxRequest{}))
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

	oversized := &conveyorv1.EnqueueTxRequest{Tasks: make([]*conveyorv1.EnqueueRequest, maxBatchTasks+1)}
	_, err = service.EnqueueTx(ctx, connect.NewRequest(oversized))
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	require.ErrorContains(t, err, "split it into smaller batches")

	// A validation failure on any task rejects the whole request and commits
	// nothing.
	_, err = service.EnqueueTx(ctx, connect.NewRequest(&conveyorv1.EnqueueTxRequest{
		Tasks: []*conveyorv1.EnqueueRequest{
			{TaskId: "tx-bad-1", Type: "test:ok"},
			{TaskId: "tx-bad-2"}, // missing type
		},
	}))
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	require.ErrorContains(t, err, "task 1")

	_, _, getErr := service.taskLog.GetTask(ctx, "tx-bad-1")
	require.ErrorIs(t, getErr, broker.ErrTaskNotFound)

	// All-or-nothing success across queues.
	response, err := service.EnqueueTx(ctx, connect.NewRequest(&conveyorv1.EnqueueTxRequest{
		Tasks: []*conveyorv1.EnqueueRequest{
			{TaskId: "tx-001", Type: "test:a", Queue: "billing"},
			{TaskId: "tx-002", Type: "test:b", Queue: "mail"},
		},
	}))
	require.NoError(t, err)
	require.Len(t, response.Msg.GetTasks(), 2)
	require.Equal(t, "tx-001", response.Msg.GetTasks()[0].GetId())
	require.Equal(t, "tx-002", response.Msg.GetTasks()[1].GetId())

	_, _, getErr = service.taskLog.GetTask(ctx, "tx-001")
	require.NoError(t, getErr)

	// A duplicate unique key rolls the whole batch back.
	_, err = service.EnqueueTx(ctx, connect.NewRequest(&conveyorv1.EnqueueTxRequest{
		Tasks: []*conveyorv1.EnqueueRequest{
			{TaskId: "tx-003", Type: "test:c", UniqueKey: "shared", UniqueTtl: durationpb.New(time.Hour)},
			{TaskId: "tx-004", Type: "test:c", UniqueKey: "shared", UniqueTtl: durationpb.New(time.Hour)},
		},
	}))
	require.Equal(t, connect.CodeAlreadyExists, connect.CodeOf(err))

	_, _, getErr = service.taskLog.GetTask(ctx, "tx-003")
	require.ErrorIs(t, getErr, broker.ErrTaskNotFound)
}

func TestGetTask(t *testing.T) {
	service := newTestTaskService(t)
	ctx := context.Background()

	_, err := service.GetTask(ctx, connect.NewRequest(&conveyorv1.GetTaskRequest{}))
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

	_, err = service.GetTask(ctx, connect.NewRequest(&conveyorv1.GetTaskRequest{Id: "01MISSING"}))
	require.Equal(t, connect.CodeNotFound, connect.CodeOf(err))

	enqueued, err := service.Enqueue(ctx, connect.NewRequest(&conveyorv1.EnqueueRequest{
		Type:     "test:get",
		Queue:    "critical",
		Priority: 7,
	}))
	require.NoError(t, err)

	fetched, err := service.GetTask(ctx, connect.NewRequest(&conveyorv1.GetTaskRequest{
		Id: enqueued.Msg.GetTask().GetId(),
	}))
	require.NoError(t, err)

	task := fetched.Msg.GetTask()
	require.Equal(t, "critical", task.GetQueue())
	require.Equal(t, "test:get", task.GetType())
	require.EqualValues(t, 7, task.GetPriority())
}

// streamTestPayloadCap is the payload cap of the streaming test server: above
// the inline limit, so chunked uploads are admitted, but small enough to test
// the cap cheaply.
const streamTestPayloadCap = 4 << 20

// newStreamTestClient serves the API over a memory broker capped at
// streamTestPayloadCap and returns a task client and the broker.
func newStreamTestClient(t *testing.T) (conveyorv1connect.TaskServiceClient, broker.Broker) {
	t.Helper()

	engine, taskLog := startTestEngineWithBroker(t, memory.NewWithMaxPayload(clock.System(), streamTestPayloadCap))
	baseURL := startAPIServer(t, engine, taskLog, nil)

	return conveyorv1connect.NewTaskServiceClient(h2cHTTPClient(), baseURL, connect.WithGRPC()), taskLog
}

// streamEnqueue uploads the given messages on one EnqueueStream call.
func streamEnqueue(t *testing.T, client conveyorv1connect.TaskServiceClient, messages ...*conveyorv1.EnqueueStreamRequest) (*connect.Response[conveyorv1.EnqueueResponse], error) {
	t.Helper()

	stream := client.EnqueueStream(context.Background())

	for _, message := range messages {
		if err := stream.Send(message); err != nil {
			break
		}
	}

	return stream.CloseAndReceive()
}

// headerMessage wraps a task header as an EnqueueStream message.
func headerMessage(header *conveyorv1.EnqueueRequest) *conveyorv1.EnqueueStreamRequest {
	return &conveyorv1.EnqueueStreamRequest{Frame: &conveyorv1.EnqueueStreamRequest_Header{Header: header}}
}

// chunkMessage wraps a payload chunk as an EnqueueStream message.
func chunkMessage(chunk []byte) *conveyorv1.EnqueueStreamRequest {
	return &conveyorv1.EnqueueStreamRequest{Frame: &conveyorv1.EnqueueStreamRequest_Chunk{Chunk: chunk}}
}

// commitMessage wraps an upload's closing commit as an EnqueueStream message.
func commitMessage(size int) *conveyorv1.EnqueueStreamRequest {
	return &conveyorv1.EnqueueStreamRequest{Frame: &conveyorv1.EnqueueStreamRequest_Commit{Commit: &conveyorv1.PayloadCommit{Size: uint64(size)}}}
}

// storedChunks reads a task's stored payload chunks as strings.
func storedChunks(t *testing.T, taskLog broker.Broker, taskID string) []string {
	t.Helper()

	envelope, _, err := taskLog.GetTask(context.Background(), taskID)
	require.NoError(t, err)

	var chunks []string

	for data, err := range taskLog.ReadPayload(context.Background(), taskID, 0, int(envelope.GetPayloadChunks())) {
		require.NoError(t, err)

		chunks = append(chunks, string(data))
	}

	return chunks
}

func TestEnqueueRejectsPayloadAboveInlineLimit(t *testing.T) {
	engine, taskLog := startTestEngineWithBroker(t, memory.NewWithMaxPayload(clock.System(), streamTestPayloadCap))
	service := NewTaskService(engine, taskLog, clock.System(), testDefaultMaxRetry)

	_, err := service.Enqueue(context.Background(), connect.NewRequest(&conveyorv1.EnqueueRequest{
		Type:    "test:large",
		Payload: make([]byte, broker.InlinePayloadBytes+1),
	}))
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	require.ErrorContains(t, err, "EnqueueStream")
}

func TestEnqueueStreamCommitsChunkedTask(t *testing.T) {
	client, taskLog := newStreamTestClient(t)

	response, err := streamEnqueue(t, client,
		headerMessage(&conveyorv1.EnqueueRequest{TaskId: "task-stream", Type: "test:large"}),
		chunkMessage([]byte("alpha")),
		chunkMessage([]byte("beta")),
		commitMessage(len("alphabeta")),
	)
	require.NoError(t, err)

	info := response.Msg.GetTask()
	require.Equal(t, "task-stream", info.GetId())
	require.True(t, info.GetPayloadChunked())
	require.EqualValues(t, len("alphabeta"), info.GetPayloadSize())
	require.Empty(t, info.GetPayload())
	require.Equal(t, []string{"alpha", "beta"}, storedChunks(t, taskLog, "task-stream"))

	fetched, err := client.GetTask(context.Background(), connect.NewRequest(&conveyorv1.GetTaskRequest{Id: "task-stream"}))
	require.NoError(t, err)
	require.True(t, fetched.Msg.GetTask().GetPayloadChunked())
	require.EqualValues(t, len("alphabeta"), fetched.Msg.GetTask().GetPayloadSize())
}

func TestEnqueueStreamIdempotentOnID(t *testing.T) {
	client, taskLog := newStreamTestClient(t)
	header := headerMessage(&conveyorv1.EnqueueRequest{TaskId: "task-stream", Type: "test:large"})

	_, err := streamEnqueue(t, client, header, chunkMessage([]byte("first")), commitMessage(len("first")))
	require.NoError(t, err)

	retried, err := streamEnqueue(t, client, header, chunkMessage([]byte("second")), commitMessage(len("second")))
	require.NoError(t, err)
	require.EqualValues(t, len("first"), retried.Msg.GetTask().GetPayloadSize(), "an idempotent retry answers with the stored task")
	require.True(t, retried.Msg.GetTask().GetPayloadChunked())
	require.Equal(t, []string{"first"}, storedChunks(t, taskLog, "task-stream"))
}

func TestEnqueueStreamDuplicateUniqueKey(t *testing.T) {
	client, _ := newStreamTestClient(t)
	header := func(id string) *conveyorv1.EnqueueStreamRequest {
		return headerMessage(&conveyorv1.EnqueueRequest{TaskId: id, Type: "test:large", UniqueKey: "report"})
	}

	_, err := streamEnqueue(t, client, header("task-1"), chunkMessage([]byte("data")), commitMessage(len("data")))
	require.NoError(t, err)

	_, err = streamEnqueue(t, client, header("task-2"), chunkMessage([]byte("data")), commitMessage(len("data")))
	require.Equal(t, connect.CodeAlreadyExists, connect.CodeOf(err))
}

func TestEnqueueStreamValidation(t *testing.T) {
	client, taskLog := newStreamTestClient(t)
	header := headerMessage(&conveyorv1.EnqueueRequest{TaskId: "task-invalid", Type: "test:large"})
	fullChunk := make([]byte, maxPayloadChunkBytes)

	cases := map[string][]*conveyorv1.EnqueueStreamRequest{
		"no messages":           nil,
		"chunk before header":   {chunkMessage([]byte("data"))},
		"header with payload":   {headerMessage(&conveyorv1.EnqueueRequest{TaskId: "task-invalid", Type: "test:large", Payload: []byte("inline")})},
		"invalid header":        {headerMessage(&conveyorv1.EnqueueRequest{TaskId: "task-invalid"})},
		"second header":         {header, chunkMessage([]byte("data")), header},
		"oversized chunk":       {header, chunkMessage(make([]byte, maxPayloadChunkBytes+1))},
		"payload above the cap": {header, chunkMessage(fullChunk), chunkMessage(fullChunk), chunkMessage([]byte("x"))},
		"missing commit":        {header, chunkMessage([]byte("data"))},
		"commit size mismatch":  {header, chunkMessage([]byte("data")), commitMessage(len("data") - 1)},
		"message after commit":  {header, chunkMessage([]byte("data")), commitMessage(len("data")), chunkMessage([]byte("more"))},
	}

	for name, messages := range cases {
		_, err := streamEnqueue(t, client, messages...)
		require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err), "case %s: %v", name, err)

		_, _, getErr := taskLog.GetTask(context.Background(), "task-invalid")
		require.ErrorIs(t, getErr, broker.ErrTaskNotFound, "case %s committed a task", name)
	}
}

func TestEnqueueStreamAcceptsPayloadAndChunkAtTheirLimits(t *testing.T) {
	client, taskLog := newStreamTestClient(t)
	fullChunk := make([]byte, maxPayloadChunkBytes)

	response, err := streamEnqueue(t, client,
		headerMessage(&conveyorv1.EnqueueRequest{TaskId: "task-at-cap", Type: "test:large"}),
		chunkMessage(fullChunk),
		chunkMessage(fullChunk),
		commitMessage(streamTestPayloadCap),
	)
	require.NoError(t, err, "a payload of exactly the cap in chunks of exactly the chunk limit is admitted")
	require.EqualValues(t, streamTestPayloadCap, response.Msg.GetTask().GetPayloadSize())
	require.Len(t, storedChunks(t, taskLog, "task-at-cap"), 2)
}

func TestCheckInlinePayloadBoundaries(t *testing.T) {
	const payloadCap = 2 * broker.InlinePayloadBytes

	require.NoError(t, checkInlinePayload(make([]byte, broker.InlinePayloadBytes), payloadCap))
	require.ErrorContains(t, checkInlinePayload(make([]byte, payloadCap), payloadCap), "EnqueueStream",
		"a payload of exactly the cap is streamable")
	require.ErrorContains(t, checkInlinePayload(make([]byte, payloadCap+1), payloadCap), "store large blobs elsewhere")
	require.ErrorContains(t, checkInlinePayload(make([]byte, broker.InlinePayloadBytes), broker.InlinePayloadBytes-1), "store large blobs elsewhere",
		"a cap below the inline limit bounds inline payloads")
}

// getTaskFailingBroker commits normally but cannot read a task back.
type getTaskFailingBroker struct {
	broker.Broker
}

// GetTask always fails.
func (getTaskFailingBroker) GetTask(context.Context, string) (*conveyorv1.TaskEnvelope, conveyorv1.TaskState, error) {
	return nil, conveyorv1.TaskState_TASK_STATE_UNSPECIFIED, errors.New("store down")
}

func TestEnqueueStreamReportsAFailedReadBack(t *testing.T) {
	failing := getTaskFailingBroker{Broker: memory.NewWithMaxPayload(clock.System(), streamTestPayloadCap)}
	engine, taskLog := startTestEngineWithBroker(t, failing)
	baseURL := startAPIServer(t, engine, taskLog, nil)
	client := conveyorv1connect.NewTaskServiceClient(h2cHTTPClient(), baseURL, connect.WithGRPC())

	_, err := streamEnqueue(t, client,
		headerMessage(&conveyorv1.EnqueueRequest{TaskId: "task-1", Type: "test:large"}),
		chunkMessage([]byte("data")),
		commitMessage(len("data")),
	)
	require.Equal(t, connect.CodeInternal, connect.CodeOf(err), "the commit stands; the client's retry of the id is a no-op")
}
