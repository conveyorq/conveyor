// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package conveyor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
	"github.com/conveyorq/conveyor/sdk/internal/transport"
)

// TaskState is the lifecycle state of a task as reported by the server.
type TaskState string

// Task lifecycle states.
const (
	// TaskStateScheduled is a task whose start time lies in the future.
	TaskStateScheduled TaskState = "scheduled"
	// TaskStatePending is a due task waiting for a worker slot.
	TaskStatePending TaskState = "pending"
	// TaskStateActive is a task currently executing on a worker.
	TaskStateActive TaskState = "active"
	// TaskStateRetry is a failed task waiting for its backoff to elapse.
	TaskStateRetry TaskState = "retry"
	// TaskStateCompleted is a successfully executed task.
	TaskStateCompleted TaskState = "completed"
	// TaskStateArchived is a dead-lettered task.
	TaskStateArchived TaskState = "archived"
	// TaskStateCanceled is a task canceled before completion.
	TaskStateCanceled TaskState = "canceled"
	// TaskStateAggregating is a group member accumulating until its group fires.
	TaskStateAggregating TaskState = "aggregating"
	// TaskStateUnknown reports a state this SDK version does not know.
	TaskStateUnknown TaskState = "unknown"
)

// TaskInfo is the client-visible view of a task.
type TaskInfo struct {
	// ID is the task ULID.
	ID string
	// Queue is the queue the task belongs to.
	Queue string
	// Type is the handler routing key.
	Type string
	// State is the task's current lifecycle state.
	State TaskState
	// Priority is the dispatch priority within the queue.
	Priority int
	// Retried is how many times the task has been retried.
	Retried int
	// MaxRetry is the task's retry budget.
	MaxRetry int
	// LastError is the message of the most recent failure, if any.
	LastError string
	// EnqueuedAt is when the task was committed.
	EnqueuedAt time.Time
	// ProcessAt is when the task becomes due; zero means immediately.
	ProcessAt time.Time
}

// Client enqueues tasks and inspects their state over the Conveyor API.
type Client struct {
	// wire is the ConnectRPC transport.
	wire *transport.Client
}

// NewClient builds a Client for the Conveyor server at baseURL, e.g.
// "http://localhost:8080".
func NewClient(baseURL string, opts ...Option) (*Client, error) {
	if baseURL == "" {
		return nil, errors.New("conveyor: base URL is required")
	}

	settings := &options{}

	for _, opt := range opts {
		opt(settings)
	}

	return &Client{wire: transport.New(baseURL, settings.token)}, nil
}

// Enqueue durably commits one task and returns its initial state.
func (c *Client) Enqueue(ctx context.Context, task *Task, opts ...EnqueueOption) (*TaskInfo, error) {
	if task == nil {
		return nil, errors.New("conveyor: task is required")
	}

	if task.payloadErr != nil {
		return nil, task.payloadErr
	}

	if task.taskType == "" {
		return nil, errors.New("conveyor: task type is required")
	}

	settings := &enqueueOptions{}

	for _, opt := range opts {
		opt(settings)
	}

	if !settings.processAt.IsZero() && settings.processIn > 0 {
		return nil, errors.New("conveyor: ProcessAt and ProcessIn are mutually exclusive")
	}

	uniqueKey := settings.uniqueKey
	if uniqueKey == "" && settings.uniqueTTL > 0 {
		uniqueKey = derivedUniqueKey(task.taskType, task.payload)
	}

	request := &conveyorv1.EnqueueRequest{
		TaskId:      settings.taskID,
		Queue:       settings.queue,
		Type:        task.taskType,
		Payload:     task.payload,
		ContentType: task.contentType,
		Metadata:    task.metadata,
		MaxRetry:    int32(settings.maxRetry),
		Priority:    int32(settings.priority),
		UniqueKey:   uniqueKey,
		Group:       settings.group,
	}

	if settings.timeout > 0 {
		request.Timeout = durationpb.New(settings.timeout)
	}

	if !settings.deadline.IsZero() {
		request.Deadline = timestamppb.New(settings.deadline)
	}

	if !settings.processAt.IsZero() {
		request.ProcessAt = timestamppb.New(settings.processAt)
	}

	if settings.processIn > 0 {
		request.ProcessIn = durationpb.New(settings.processIn)
	}

	if settings.retention > 0 {
		request.Retention = durationpb.New(settings.retention)
	}

	if settings.uniqueTTL > 0 {
		request.UniqueTtl = durationpb.New(settings.uniqueTTL)
	}

	info, err := c.wire.Enqueue(ctx, request)
	if err != nil {
		return nil, wireError(err)
	}

	return taskInfoFromProto(info), nil
}

// GetTask returns the current state of one task.
func (c *Client) GetTask(ctx context.Context, id string) (*TaskInfo, error) {
	if id == "" {
		return nil, errors.New("conveyor: task id is required")
	}

	info, err := c.wire.GetTask(ctx, id)
	if err != nil {
		return nil, wireError(err)
	}

	return taskInfoFromProto(info), nil
}

// derivedUniqueKey computes the default uniqueness key of a task: a hash
// over its type and payload, so re-enqueueing the same work collides.
func derivedUniqueKey(taskType string, payload []byte) string {
	digest := sha256.New()
	// hash.Hash.Write never returns an error.
	_, _ = digest.Write([]byte(taskType))
	_, _ = digest.Write([]byte{0})
	_, _ = digest.Write(payload)

	return hex.EncodeToString(digest.Sum(nil))
}

// wireError maps transport failures to the SDK sentinel errors.
func wireError(err error) error {
	switch connect.CodeOf(err) {
	case connect.CodeAlreadyExists:
		return fmt.Errorf("%w: %v", ErrDuplicateTask, err)

	case connect.CodeNotFound:
		return fmt.Errorf("%w: %v", ErrTaskNotFound, err)

	default:
		return fmt.Errorf("conveyor: %w", err)
	}
}

// taskInfoFromProto maps the wire task view to the SDK type.
func taskInfoFromProto(info *conveyorv1.TaskInfo) *TaskInfo {
	result := &TaskInfo{
		ID:        info.GetId(),
		Queue:     info.GetQueue(),
		Type:      info.GetType(),
		State:     taskStateFromProto(info.GetState()),
		Priority:  int(info.GetPriority()),
		Retried:   int(info.GetRetried()),
		MaxRetry:  int(info.GetMaxRetry()),
		LastError: info.GetLastError(),
	}

	if info.GetEnqueuedAt().IsValid() {
		result.EnqueuedAt = info.GetEnqueuedAt().AsTime()
	}

	if info.GetProcessAt().IsValid() {
		result.ProcessAt = info.GetProcessAt().AsTime()
	}

	return result
}

// taskStateFromProto maps the wire task state to the SDK type.
func taskStateFromProto(state conveyorv1.TaskState) TaskState {
	switch state {
	case conveyorv1.TaskState_TASK_STATE_SCHEDULED:
		return TaskStateScheduled
	case conveyorv1.TaskState_TASK_STATE_PENDING:
		return TaskStatePending
	case conveyorv1.TaskState_TASK_STATE_ACTIVE:
		return TaskStateActive
	case conveyorv1.TaskState_TASK_STATE_RETRY:
		return TaskStateRetry
	case conveyorv1.TaskState_TASK_STATE_COMPLETED:
		return TaskStateCompleted
	case conveyorv1.TaskState_TASK_STATE_ARCHIVED:
		return TaskStateArchived
	case conveyorv1.TaskState_TASK_STATE_CANCELED:
		return TaskStateCanceled
	case conveyorv1.TaskState_TASK_STATE_AGGREGATING:
		return TaskStateAggregating
	default:
		return TaskStateUnknown
	}
}
