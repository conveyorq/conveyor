// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"context"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/conveyorq/conveyor/internal/clock"
	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
)

// newTestTaskService builds a TaskService over a fresh engine.
func newTestTaskService(t *testing.T) *TaskService {
	t.Helper()

	engine, taskLog := startTestEngine(t)

	return NewTaskService(engine, taskLog, clock.System(), testDefaultMaxRetry)
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
		"oversized payload":  {Type: "test:ok", Payload: make([]byte, maxPayloadBytes+1)},
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
