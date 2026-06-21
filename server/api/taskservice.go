// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"context"
	"errors"
	"fmt"

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

// maxPayloadBytes caps one task payload. Larger blobs belong in object
// storage with a reference in the payload.
const maxPayloadBytes = 1 << 20

// maxDependencies caps how many tasks one task may depend on, bounding the
// dependency-edge fan-in committed per enqueue and the work one resolution does.
const maxDependencies = 1000

// maxBatchTasks caps the number of items in one EnqueueBatch request.
const maxBatchTasks = 1000

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

	if len(request.GetPayload()) > maxPayloadBytes {
		return nil, fmt.Errorf("payload is %d bytes, the maximum is %d; store large blobs elsewhere and enqueue a reference", len(request.GetPayload()), maxPayloadBytes)
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
			MaxRetry:  maxRetry,
			Timeout:   request.GetTimeout(),
			Deadline:  request.GetDeadline(),
			ProcessAt: processAt,
			UniqueKey: request.GetUniqueKey(),
			UniqueTtl: request.GetUniqueTtl(),
			Retention: request.GetRetention(),
			Priority:  priority,
			Group:     request.GetGroup(),
			ExpiresAt: expiresAt,
			DependsOn: request.GetDependsOn(),
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
		Id:          envelope.GetId(),
		Queue:       envelope.GetQueue(),
		Type:        envelope.GetType(),
		State:       state,
		Priority:    envelope.GetOptions().GetPriority(),
		Retried:     envelope.GetRetried(),
		MaxRetry:    envelope.GetOptions().GetMaxRetry(),
		LastError:   envelope.GetLastError(),
		EnqueuedAt:  envelope.GetEnqueuedAt(),
		ProcessAt:   envelope.GetOptions().GetProcessAt(),
		StartedAt:   envelope.GetStartedAt(),
		CompletedAt: envelope.GetCompletedAt(),
		Payload:     envelope.GetPayload(),
		ContentType: envelope.GetContentType(),
	}
}

// enqueueError maps engine enqueue failures to API error codes.
func enqueueError(err error) error {
	if errors.Is(err, broker.ErrDuplicateTask) {
		return connect.NewError(connect.CodeAlreadyExists, err)
	}

	return connect.NewError(connect.CodeInternal, err)
}
