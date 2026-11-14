// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"context"
	"errors"
	"fmt"
	"iter"

	"connectrpc.com/connect"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/conveyorq/conveyor/internal/actors"
	"github.com/conveyorq/conveyor/internal/broker"
	"github.com/conveyorq/conveyor/internal/clock"
	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
	"github.com/conveyorq/conveyor/internal/proto/conveyor/v1/conveyorv1connect"
)

// defaultQueueName receives tasks that do not name a queue.
const defaultQueueName = "default"

// defaultPriority applies when a request leaves priority at zero; the
// lowest selectable priority is therefore 1.
const defaultPriority = 4

// maxPriority is the highest accepted task priority.
const maxPriority = 9

// maxDependencies caps how many tasks one task may depend on, bounding the
// dependency-edge fan-in committed per enqueue and the work one resolution does.
const maxDependencies = 1000

// maxBatchTasks caps the number of items in one EnqueueBatch request.
const maxBatchTasks = 1000

// perTaskOverheadBytes bounds one task's non-payload fields (id, queue, type,
// metadata, options) beyond the payload cap when sizing the request read limit.
const perTaskOverheadBytes = 64 << 10

// maxPayloadChunkBytes caps one chunk of an EnqueueStream upload. A producer
// sends 1 MiB chunks; the headroom admits end-to-end encryption overhead.
const maxPayloadChunkBytes = 2 << 20

// MaxRequestBytes bounds one decoded API request, and one frame of a stream, so
// the server rejects an oversized message before decoding it rather than after.
// It is the largest legitimate request: a full EnqueueBatch of maxBatchTasks
// tasks, each at the inline payload limit plus field overhead. A payload above
// the inline limit arrives through EnqueueStream in chunks far below this
// bound. The payload limits are still enforced per task after decode.
const MaxRequestBytes = maxBatchTasks * (broker.InlinePayloadBytes + perTaskOverheadBytes)

// MaxMessageBytes bounds one decoded message that carries a single task rather
// than a batch of them: the inline payload limit plus field overhead. Only
// EnqueueBatch legitimately arrives at MaxRequestBytes, so a service that never
// receives a batch is held to one task's worth of bytes. It is what the webhook
// callback service is mounted with: that service authenticates per delivery
// with a lease token instead of a bearer token, so anyone who can reach the port
// can post to it, and it must not be handed a batch-sized decode budget.
const MaxMessageBytes = broker.InlinePayloadBytes + perTaskOverheadBytes

// errInvalidUpload marks an EnqueueStream upload rejected for its shape or
// size, mapped to invalid_argument.
var errInvalidUpload = errors.New("invalid upload")

// TaskService serves the enqueue-side API.
type TaskService struct {
	// engine commits tasks and wakes their queue grains.
	engine *actors.Engine
	// taskLog reads task state for GetTask.
	taskLog broker.Broker
	// timeSource resolves relative delays to absolute times.
	timeSource clock.Clock
	// defaultMaxRetry applies when a request leaves max_retry at zero.
	defaultMaxRetry int32
}

// enforce interface compliance at compile time.
var _ conveyorv1connect.TaskServiceHandler = (*TaskService)(nil)

// NewTaskService assembles the enqueue-side API service.
func NewTaskService(engine *actors.Engine, taskLog broker.Broker, timeSource clock.Clock, defaultMaxRetry int32) *TaskService {
	return &TaskService{
		engine:          engine,
		taskLog:         taskLog,
		timeSource:      timeSource,
		defaultMaxRetry: defaultMaxRetry,
	}
}

// inlinePayloadLimit is the largest payload a request may carry inline: the
// broker's inline limit, or its payload cap when that is lower.
func inlinePayloadLimit(maxPayloadBytes int) int {
	return min(maxPayloadBytes, broker.InlinePayloadBytes)
}

// checkPayloadSize rejects a payload larger than maxPayloadBytes.
func checkPayloadSize(payload []byte, maxPayloadBytes int) error {
	if len(payload) > maxPayloadBytes {
		return fmt.Errorf("payload is %d bytes, the maximum is %d; store large blobs elsewhere and enqueue a reference", len(payload), maxPayloadBytes)
	}

	return nil
}

// checkInlinePayload rejects a payload above the inline limit. When the
// payload cap would admit it, the error points at EnqueueStream.
func checkInlinePayload(payload []byte, maxPayloadBytes int) error {
	inlineLimit := inlinePayloadLimit(maxPayloadBytes)
	if len(payload) > inlineLimit && len(payload) <= maxPayloadBytes {
		return fmt.Errorf("payload is %d bytes, above the %d-byte inline limit; send it with EnqueueStream", len(payload), inlineLimit)
	}

	return checkPayloadSize(payload, inlineLimit)
}

// Enqueue durably commits one task and reports its initial state.
func (s *TaskService) Enqueue(ctx context.Context, request *connect.Request[conveyorv1.EnqueueRequest]) (*connect.Response[conveyorv1.EnqueueResponse], error) {
	ctx, span := otel.Tracer(tracerName).Start(ctx, "conveyor.enqueue", trace.WithSpanKind(trace.SpanKindProducer))
	defer span.End()

	envelope, err := s.envelopeFromRequest(request.Msg)
	if err != nil {
		span.RecordError(err)

		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	// Stamp the enqueue trace into the envelope before it is persisted so the
	// worker's execution span links back to this span.
	injectTaskTrace(ctx, envelope)

	if err := s.engine.Enqueue(ctx, envelope); err != nil {
		span.RecordError(err)

		return nil, enqueueError(err)
	}

	span.SetAttributes(
		attribute.String("conveyor.task.id", envelope.GetId()),
		attribute.String("conveyor.task.type", envelope.GetType()),
		attribute.String("conveyor.queue", envelope.GetQueue()),
	)

	return connect.NewResponse(&conveyorv1.EnqueueResponse{
		Task: taskInfo(envelope, s.initialState(envelope)),
	}), nil
}

// EnqueueBatch commits many tasks in one call. Items fail independently:
// each result carries either the committed task or that item's error.
func (s *TaskService) EnqueueBatch(ctx context.Context, request *connect.Request[conveyorv1.EnqueueBatchRequest]) (*connect.Response[conveyorv1.EnqueueBatchResponse], error) {
	items := request.Msg.GetTasks()
	if len(items) == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("batch must contain at least one task"))
	}

	if len(items) > maxBatchTasks {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("batch holds %d tasks, the maximum is %d; split it into smaller batches", len(items), maxBatchTasks))
	}

	results := make([]*conveyorv1.EnqueueResult, 0, len(items))

	for _, item := range items {
		envelope, err := s.envelopeFromRequest(item)
		if err == nil {
			err = s.engine.Enqueue(ctx, envelope)
		}

		if err != nil {
			results = append(results, &conveyorv1.EnqueueResult{Error: err.Error()})
			continue
		}

		results = append(results, &conveyorv1.EnqueueResult{
			Task: taskInfo(envelope, s.initialState(envelope)),
		})
	}

	return connect.NewResponse(&conveyorv1.EnqueueBatchResponse{Results: results}), nil
}

// EnqueueTx commits every task atomically: either all are enqueued or none are.
// A validation failure on any task rejects the whole request before the broker
// is touched; a broker failure rolls the whole set back. On success it returns
// the committed tasks in request order.
func (s *TaskService) EnqueueTx(ctx context.Context, request *connect.Request[conveyorv1.EnqueueTxRequest]) (*connect.Response[conveyorv1.EnqueueTxResponse], error) {
	items := request.Msg.GetTasks()
	if len(items) == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("batch must contain at least one task"))
	}

	if len(items) > maxBatchTasks {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("batch holds %d tasks, the maximum is %d; split it into smaller batches", len(items), maxBatchTasks))
	}

	ctx, span := otel.Tracer(tracerName).Start(ctx, "conveyor.enqueue_tx", trace.WithSpanKind(trace.SpanKindProducer))
	defer span.End()

	envelopes := make([]*conveyorv1.TaskEnvelope, len(items))

	for index, item := range items {
		envelope, err := s.envelopeFromRequest(item)
		if err != nil {
			span.RecordError(err)
			return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("task %d: %w", index, err))
		}

		// Stamp the enqueue trace into each envelope before it is persisted so
		// each worker's execution span links back to this span.
		injectTaskTrace(ctx, envelope)
		envelopes[index] = envelope
	}

	if err := s.engine.EnqueueBatch(ctx, envelopes); err != nil {
		span.RecordError(err)
		return nil, enqueueError(err)
	}

	span.SetAttributes(attribute.Int("conveyor.task.count", len(envelopes)))

	infos := make([]*conveyorv1.TaskInfo, len(envelopes))

	for index, envelope := range envelopes {
		infos[index] = taskInfo(envelope, s.initialState(envelope))
	}

	return connect.NewResponse(&conveyorv1.EnqueueTxResponse{Tasks: infos}), nil
}

// GetTask returns the current state of one task.
func (s *TaskService) GetTask(ctx context.Context, request *connect.Request[conveyorv1.GetTaskRequest]) (*connect.Response[conveyorv1.GetTaskResponse], error) {
	if request.Msg.GetId() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("task id is required"))
	}

	envelope, state, err := s.taskLog.GetTask(ctx, request.Msg.GetId())

	if errors.Is(err, broker.ErrTaskNotFound) {
		return nil, connect.NewError(connect.CodeNotFound, err)
	}

	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&conveyorv1.GetTaskResponse{Task: taskInfo(envelope, state)}), nil
}

// EnqueueStream durably commits one task whose payload arrives in chunks after
// its header. The chunks flow straight into the broker as they arrive, so the
// server never holds the whole payload; the task commits only once the client
// closes the stream.
func (s *TaskService) EnqueueStream(ctx context.Context, stream *connect.ClientStream[conveyorv1.EnqueueStreamRequest]) (*connect.Response[conveyorv1.EnqueueResponse], error) {
	ctx, span := otel.Tracer(tracerName).Start(ctx, "conveyor.enqueue", trace.WithSpanKind(trace.SpanKindProducer))
	defer span.End()

	envelope, err := s.streamHeader(stream)
	if err != nil {
		span.RecordError(err)

		return nil, err
	}

	injectTaskTrace(ctx, envelope)

	var received int

	if err = s.engine.EnqueueChunked(ctx, envelope, receiveUpload(stream, s.taskLog.MaxPayloadBytes(), &received)); err != nil {
		span.RecordError(err)

		return nil, streamEnqueueError(err)
	}

	span.SetAttributes(
		attribute.String("conveyor.task.id", envelope.GetId()),
		attribute.String("conveyor.task.type", envelope.GetType()),
		attribute.String("conveyor.queue", envelope.GetQueue()),
		attribute.Int("conveyor.task.payload_bytes", received),
	)

	// Answer with the task as stored: for an idempotent retry of an existing
	// id that is the original task, not this upload, which was never read.
	// The task is committed either way, so a failed read is reported and the
	// client's retry of the same id is a no-op.
	stored, state, err := s.taskLog.GetTask(ctx, envelope.GetId())
	if err != nil {
		span.RecordError(err)

		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&conveyorv1.EnqueueResponse{Task: taskInfo(stored, state)}), nil
}

// streamHeader reads and validates the first message of an EnqueueStream
// upload: the task header, with no inline payload.
func (s *TaskService) streamHeader(stream *connect.ClientStream[conveyorv1.EnqueueStreamRequest]) (*conveyorv1.TaskEnvelope, error) {
	if !stream.Receive() {
		if err := stream.Err(); err != nil {
			return nil, err
		}

		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("%w: the stream carried no task header", errInvalidUpload))
	}

	header := stream.Msg().GetHeader()
	if header == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("%w: the first message must be the task header", errInvalidUpload))
	}

	if len(header.GetPayload()) > 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("%w: the header must not carry a payload; send it as chunks", errInvalidUpload))
	}

	envelope, err := s.envelopeFromRequest(header)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	return envelope, nil
}

// receiveUpload yields the chunks of an EnqueueStream upload after its header,
// counting the bytes received into received. The upload must end with a
// PayloadCommit whose size matches the bytes received and nothing after it;
// otherwise, and on any invalid chunk or a broken stream, it yields an error,
// so the broker commits nothing. Requiring the commit is what tells a complete
// upload from one cut short: a client stream can end cleanly without the
// producer having finished, for example when a canceled HTTP/2 request is
// half-closed before it is reset.
func receiveUpload(stream *connect.ClientStream[conveyorv1.EnqueueStreamRequest], maxPayloadBytes int, received *int) iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {
		committed := false

		for stream.Receive() {
			message := stream.Msg()

			if committed {
				yield(nil, fmt.Errorf("%w: nothing may follow the commit", errInvalidUpload))

				return
			}

			if commit := message.GetCommit(); commit != nil {
				if commit.GetSize() != uint64(*received) {
					yield(nil, fmt.Errorf("%w: the commit states %d bytes, %d were received", errInvalidUpload, commit.GetSize(), *received))

					return
				}

				committed = true

				continue
			}

			chunk, err := uploadChunk(message, *received, maxPayloadBytes)
			if err != nil {
				yield(nil, err)

				return
			}

			*received += len(chunk)

			if !yield(chunk, nil) {
				return
			}
		}

		if err := stream.Err(); err != nil {
			yield(nil, err)

			return
		}

		if !committed {
			yield(nil, fmt.Errorf("%w: the upload ended without a commit", errInvalidUpload))
		}
	}
}

// uploadChunk validates one chunk message: it must be a chunk of at most
// maxPayloadChunkBytes that keeps the running total within the payload cap.
func uploadChunk(message *conveyorv1.EnqueueStreamRequest, received, maxPayloadBytes int) ([]byte, error) {
	if message.GetHeader() != nil {
		return nil, fmt.Errorf("%w: the task header must be sent once, first", errInvalidUpload)
	}

	chunk := message.GetChunk()
	if len(chunk) > maxPayloadChunkBytes {
		return nil, fmt.Errorf("%w: chunk is %d bytes, the maximum is %d", errInvalidUpload, len(chunk), maxPayloadChunkBytes)
	}

	if received+len(chunk) > maxPayloadBytes {
		return nil, fmt.Errorf("%w: payload exceeds the maximum of %d bytes; store large blobs elsewhere and enqueue a reference", errInvalidUpload, maxPayloadBytes)
	}

	return chunk, nil
}

// streamEnqueueError maps a chunked enqueue failure to an API error: a rejected
// upload is invalid_argument, a broken client stream keeps its own code, and
// everything else maps as for Enqueue.
func streamEnqueueError(err error) error {
	if errors.Is(err, errInvalidUpload) {
		return connect.NewError(connect.CodeInvalidArgument, err)
	}

	if connectErr := new(connect.Error); errors.As(err, &connectErr) {
		return connectErr
	}

	return enqueueError(err)
}

// envelopeFromRequest validates one enqueue request and builds the durable
// envelope, applying the server-side defaults.
func (s *TaskService) envelopeFromRequest(request *conveyorv1.EnqueueRequest) (*conveyorv1.TaskEnvelope, error) {
	if request.GetType() == "" {
		return nil, errors.New("task type is required")
	}

	queue := request.GetQueue()
	if queue == "" {
		queue = defaultQueueName
	}

	if !queueNamePattern.MatchString(queue) {
		return nil, fmt.Errorf("invalid queue name %q", queue)
	}

	if err := checkInlinePayload(request.GetPayload(), s.taskLog.MaxPayloadBytes()); err != nil {
		return nil, err
	}

	if request.GetProcessAt().IsValid() && request.GetProcessIn().IsValid() {
		return nil, errors.New("process_at and process_in are mutually exclusive")
	}

	if request.GetExpiresAt().IsValid() && request.GetExpiresIn().IsValid() {
		return nil, errors.New("expires_at and expires_in are mutually exclusive")
	}

	if request.GetGroup() != "" && (request.GetProcessAt().IsValid() || request.GetProcessIn().IsValid()) {
		return nil, errors.New("group and process_at/process_in are mutually exclusive")
	}

	if request.GetGroup() != "" && request.GetConcurrencyKey() != "" {
		return nil, errors.New("group and concurrency_key are mutually exclusive")
	}

	if request.GetMaxRetry() < 0 {
		return nil, fmt.Errorf("max_retry must not be negative, got %d", request.GetMaxRetry())
	}

	if request.GetPriority() < 0 || request.GetPriority() > maxPriority {
		return nil, fmt.Errorf("priority must be in 0..%d, got %d", maxPriority, request.GetPriority())
	}

	if len(request.GetDependsOn()) > maxDependencies {
		return nil, fmt.Errorf("a task may depend on at most %d tasks, got %d", maxDependencies, len(request.GetDependsOn()))
	}

	for _, dependency := range request.GetDependsOn() {
		if dependency.GetTaskId() == "" {
			return nil, errors.New("a dependency must name a task id")
		}
	}

	maxRetry := request.GetMaxRetry()
	if maxRetry == 0 {
		maxRetry = s.defaultMaxRetry
	}

	priority := request.GetPriority()
	if priority == 0 {
		priority = defaultPriority
	}

	var processAt *timestamppb.Timestamp

	switch {
	case request.GetProcessAt().IsValid():
		processAt = request.GetProcessAt()

	case request.GetProcessIn().IsValid():
		processAt = timestamppb.New(s.timeSource.Now().Add(request.GetProcessIn().AsDuration()))
	}

	var expiresAt *timestamppb.Timestamp

	switch {
	case request.GetExpiresAt().IsValid():
		expiresAt = request.GetExpiresAt()

	case request.GetExpiresIn().IsValid():
		expiresAt = timestamppb.New(s.timeSource.Now().Add(request.GetExpiresIn().AsDuration()))
	}

	return &conveyorv1.TaskEnvelope{
		Id:          request.GetTaskId(),
		Queue:       queue,
		Type:        request.GetType(),
		Payload:     request.GetPayload(),
		ContentType: request.GetContentType(),
		Metadata:    request.GetMetadata(),
		EnqueuedAt:  timestamppb.New(s.timeSource.Now()),
		Options: &conveyorv1.TaskOptions{
			MaxRetry:       maxRetry,
			Timeout:        request.GetTimeout(),
			Deadline:       request.GetDeadline(),
			ProcessAt:      processAt,
			UniqueKey:      request.GetUniqueKey(),
			UniqueTtl:      request.GetUniqueTtl(),
			Retention:      request.GetRetention(),
			Priority:       priority,
			Group:          request.GetGroup(),
			ExpiresAt:      expiresAt,
			DependsOn:      request.GetDependsOn(),
			ConcurrencyKey: request.GetConcurrencyKey(),
			RetryPolicy:    request.GetRetryPolicy(),
		},
	}, nil
}

// initialState reports the state a freshly committed task starts in. A task
// that declares dependencies starts blocked; the broker promotes it once every
// dependency reaches a terminal success.
func (s *TaskService) initialState(envelope *conveyorv1.TaskEnvelope) conveyorv1.TaskState {
	if len(envelope.GetOptions().GetDependsOn()) > 0 {
		return conveyorv1.TaskState_TASK_STATE_BLOCKED
	}

	if envelope.GetOptions().GetGroup() != "" {
		return conveyorv1.TaskState_TASK_STATE_AGGREGATING
	}

	processAt := envelope.GetOptions().GetProcessAt()

	if processAt.IsValid() && processAt.AsTime().After(s.timeSource.Now()) {
		return conveyorv1.TaskState_TASK_STATE_SCHEDULED
	}

	return conveyorv1.TaskState_TASK_STATE_PENDING
}

// taskInfo maps a task envelope and state to the external task view. The
// broker overlays started_at and completed_at onto the envelope on reads;
// they stay unset for tasks that have not yet been dispatched or finished.
func taskInfo(envelope *conveyorv1.TaskEnvelope, state conveyorv1.TaskState) *conveyorv1.TaskInfo {
	return &conveyorv1.TaskInfo{
		Id:              envelope.GetId(),
		Queue:           envelope.GetQueue(),
		Type:            envelope.GetType(),
		State:           state,
		Priority:        envelope.GetOptions().GetPriority(),
		Retried:         envelope.GetRetried(),
		MaxRetry:        envelope.GetOptions().GetMaxRetry(),
		LastError:       envelope.GetLastError(),
		EnqueuedAt:      envelope.GetEnqueuedAt(),
		ProcessAt:       envelope.GetOptions().GetProcessAt(),
		StartedAt:       envelope.GetStartedAt(),
		CompletedAt:     envelope.GetCompletedAt(),
		Payload:         envelope.GetPayload(),
		ContentType:     envelope.GetContentType(),
		Progress:        envelope.GetProgress(),
		ProgressMessage: envelope.GetProgressMessage(),
		PayloadChunked:  envelope.GetPayloadChunked(),
		PayloadSize:     payloadSize(envelope),
	}
}

// payloadSize is the stored payload size: the recorded size of a chunked
// payload, or the length of an inline one.
func payloadSize(envelope *conveyorv1.TaskEnvelope) uint64 {
	if envelope.GetPayloadChunked() {
		return envelope.GetPayloadSize()
	}

	return uint64(len(envelope.GetPayload()))
}

// enqueueError maps engine enqueue failures to API error codes.
func enqueueError(err error) error {
	if errors.Is(err, broker.ErrDuplicateTask) {
		return connect.NewError(connect.CodeAlreadyExists, err)
	}

	return connect.NewError(connect.CodeInternal, err)
}
