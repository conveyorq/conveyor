// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/conveyorq/conveyor/internal/broker"
	"github.com/conveyorq/conveyor/internal/clock"
	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
)

// newTestAdminService builds an AdminService and a TaskService over one
// fresh engine, so tests can enqueue through the public path.
func newTestAdminService(t *testing.T) (*AdminService, *TaskService, broker.Broker) {
	t.Helper()

	engine, taskLog := startTestEngine(t)
	admin := NewAdminService(engine, taskLog, clock.System(), stubSessions(nil), true)
	tasks := NewTaskService(engine, taskLog, clock.System(), testDefaultMaxRetry)

	return admin, tasks, taskLog
}

// stubSessions is a SessionLister returning a fixed snapshot set.
type stubSessions []SessionSnapshot

// Sessions implements SessionLister.
func (s stubSessions) Sessions() []SessionSnapshot { return s }

// mustEnqueueType enqueues one task through the TaskService and returns
// its id.
func mustEnqueueType(t *testing.T, tasks *TaskService, request *conveyorv1.EnqueueRequest) string {
	t.Helper()

	response, err := tasks.Enqueue(context.Background(), connect.NewRequest(request))
	require.NoError(t, err)

	return response.Msg.GetTask().GetId()
}

func TestListQueuesReportsStats(t *testing.T) {
	admin, tasks, taskLog := newTestAdminService(t)
	ctx := context.Background()

	mustEnqueueType(t, tasks, &conveyorv1.EnqueueRequest{Type: "test:a"})
	mustEnqueueType(t, tasks, &conveyorv1.EnqueueRequest{Type: "test:b", Queue: "reports"})
	mustEnqueueType(t, tasks, &conveyorv1.EnqueueRequest{Type: "test:c", Queue: "reports", ProcessIn: durationpb.New(time.Hour)})
	require.NoError(t, taskLog.SetQueuePaused(ctx, "reports", true))

	response, err := admin.ListQueues(ctx, connect.NewRequest(&conveyorv1.ListQueuesRequest{}))
	require.NoError(t, err)

	queues := response.Msg.GetQueues()
	require.Len(t, queues, 2)
	require.Equal(t, defaultQueueName, queues[0].GetName())
	require.EqualValues(t, 1, queues[0].GetPending())
	require.Equal(t, "reports", queues[1].GetName())
	require.True(t, queues[1].GetPaused())
	require.EqualValues(t, 1, queues[1].GetPending())
	require.EqualValues(t, 1, queues[1].GetScheduled())
}

func TestPauseResumeQueueValidation(t *testing.T) {
	admin, _, _ := newTestAdminService(t)
	ctx := context.Background()

	_, err := admin.PauseQueue(ctx, connect.NewRequest(&conveyorv1.PauseQueueRequest{}))
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

	_, err = admin.ResumeQueue(ctx, connect.NewRequest(&conveyorv1.ResumeQueueRequest{Queue: "no spaces"}))
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
}

func TestPauseResumeQueuePersistsFlag(t *testing.T) {
	admin, _, taskLog := newTestAdminService(t)
	ctx := context.Background()

	_, err := admin.PauseQueue(ctx, connect.NewRequest(&conveyorv1.PauseQueueRequest{Queue: defaultQueueName}))
	require.NoError(t, err)

	paused, err := taskLog.QueuePaused(ctx, defaultQueueName)
	require.NoError(t, err)
	require.True(t, paused)

	_, err = admin.ResumeQueue(ctx, connect.NewRequest(&conveyorv1.ResumeQueueRequest{Queue: defaultQueueName}))
	require.NoError(t, err)

	paused, err = taskLog.QueuePaused(ctx, defaultQueueName)
	require.NoError(t, err)
	require.False(t, paused)
}

func TestSetQueueRateLimitValidation(t *testing.T) {
	admin, _, _ := newTestAdminService(t)
	ctx := context.Background()

	_, err := admin.SetQueueRateLimit(ctx, connect.NewRequest(&conveyorv1.SetQueueRateLimitRequest{RatePerSec: 10, Burst: 5}))
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err), "empty queue is rejected")

	_, err = admin.SetQueueRateLimit(ctx, connect.NewRequest(&conveyorv1.SetQueueRateLimitRequest{Queue: defaultQueueName, RatePerSec: 0, Burst: 5}))
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err), "a non-positive rate is rejected")

	_, err = admin.SetQueueRateLimit(ctx, connect.NewRequest(&conveyorv1.SetQueueRateLimitRequest{Queue: defaultQueueName, RatePerSec: 10, Burst: 0}))
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err), "a burst below one is rejected")

	_, err = admin.SetQueueRateLimit(ctx, connect.NewRequest(&conveyorv1.SetQueueRateLimitRequest{Queue: defaultQueueName, RatePerSec: math.NaN(), Burst: 5}))
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err), "a NaN rate is rejected")

	_, err = admin.SetQueueRateLimit(ctx, connect.NewRequest(&conveyorv1.SetQueueRateLimitRequest{Queue: defaultQueueName, RatePerSec: math.Inf(1), Burst: 5}))
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err), "an infinite rate is rejected")
}

func TestRateLimitSetListDelete(t *testing.T) {
	admin, _, taskLog := newTestAdminService(t)
	ctx := context.Background()

	_, err := admin.SetQueueRateLimit(ctx, connect.NewRequest(&conveyorv1.SetQueueRateLimitRequest{Queue: defaultQueueName, RatePerSec: 50, Burst: 10}))
	require.NoError(t, err)

	limit, ok, err := taskLog.QueueRateLimit(ctx, defaultQueueName)
	require.NoError(t, err)
	require.True(t, ok)
	require.EqualValues(t, 50, limit.RatePerSec)
	require.Equal(t, 10, limit.Burst)

	list, err := admin.ListRateLimits(ctx, connect.NewRequest(&conveyorv1.ListRateLimitsRequest{}))
	require.NoError(t, err)
	require.Len(t, list.Msg.GetLimits(), 1)
	require.Equal(t, defaultQueueName, list.Msg.GetLimits()[0].GetQueue())
	require.EqualValues(t, 50, list.Msg.GetLimits()[0].GetRatePerSec())
	require.EqualValues(t, 10, list.Msg.GetLimits()[0].GetBurst())

	_, err = admin.DeleteQueueRateLimit(ctx, connect.NewRequest(&conveyorv1.DeleteQueueRateLimitRequest{Queue: defaultQueueName}))
	require.NoError(t, err)

	_, ok, err = taskLog.QueueRateLimit(ctx, defaultQueueName)
	require.NoError(t, err)
	require.False(t, ok, "delete clears the override")
}

func TestSetQueueConcurrencyLimitValidation(t *testing.T) {
	admin, _, _ := newTestAdminService(t)
	ctx := context.Background()

	_, err := admin.SetQueueConcurrencyLimit(ctx, connect.NewRequest(&conveyorv1.SetQueueConcurrencyLimitRequest{MaxActive: 5}))
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err), "empty queue is rejected")

	_, err = admin.SetQueueConcurrencyLimit(ctx, connect.NewRequest(&conveyorv1.SetQueueConcurrencyLimitRequest{Queue: defaultQueueName, MaxActive: 0}))
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err), "a max-active below one is rejected")
}

func TestConcurrencyLimitSetListDelete(t *testing.T) {
	admin, _, taskLog := newTestAdminService(t)
	ctx := context.Background()

	_, err := admin.SetQueueConcurrencyLimit(ctx, connect.NewRequest(&conveyorv1.SetQueueConcurrencyLimitRequest{Queue: defaultQueueName, MaxActive: 5}))
	require.NoError(t, err)

	limit, ok, err := taskLog.QueueConcurrencyLimit(ctx, defaultQueueName)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, 5, limit.MaxActive)

	list, err := admin.ListConcurrencyLimits(ctx, connect.NewRequest(&conveyorv1.ListConcurrencyLimitsRequest{}))
	require.NoError(t, err)
	require.Len(t, list.Msg.GetLimits(), 1)
	require.Equal(t, defaultQueueName, list.Msg.GetLimits()[0].GetQueue())
	require.EqualValues(t, 5, list.Msg.GetLimits()[0].GetMaxActive())

	_, err = admin.DeleteQueueConcurrencyLimit(ctx, connect.NewRequest(&conveyorv1.DeleteQueueConcurrencyLimitRequest{Queue: defaultQueueName}))
	require.NoError(t, err)

	_, ok, err = taskLog.QueueConcurrencyLimit(ctx, defaultQueueName)
	require.NoError(t, err)
	require.False(t, ok, "delete clears the limit")
}

func TestSetGroupConfigValidation(t *testing.T) {
	admin, _, _ := newTestAdminService(t)
	ctx := context.Background()

	_, err := admin.SetGroupConfig(ctx, connect.NewRequest(&conveyorv1.SetGroupConfigRequest{
		Group: "emails", MaxSize: 20, MaxDelay: durationpb.New(time.Minute), GracePeriod: durationpb.New(5 * time.Second),
	}))
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err), "empty queue is rejected")

	_, err = admin.SetGroupConfig(ctx, connect.NewRequest(&conveyorv1.SetGroupConfigRequest{
		Queue: defaultQueueName, Group: "emails", MaxSize: 0, MaxDelay: durationpb.New(time.Minute), GracePeriod: durationpb.New(5 * time.Second),
	}))
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err), "a max-size below one is rejected")

	_, err = admin.SetGroupConfig(ctx, connect.NewRequest(&conveyorv1.SetGroupConfigRequest{
		Queue: defaultQueueName, Group: "emails", MaxSize: 20, MaxDelay: durationpb.New(0), GracePeriod: durationpb.New(5 * time.Second),
	}))
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err), "a non-positive max-delay is rejected")

	_, err = admin.SetGroupConfig(ctx, connect.NewRequest(&conveyorv1.SetGroupConfigRequest{
		Queue: defaultQueueName, Group: "emails", MaxSize: 20, MaxDelay: durationpb.New(time.Minute), GracePeriod: durationpb.New(0),
	}))
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err), "a non-positive grace period is rejected")
}

func TestGroupConfigSetListDelete(t *testing.T) {
	admin, _, taskLog := newTestAdminService(t)
	ctx := context.Background()

	_, err := admin.SetGroupConfig(ctx, connect.NewRequest(&conveyorv1.SetGroupConfigRequest{
		Queue:       defaultQueueName,
		Group:       "emails",
		MaxSize:     20,
		MaxDelay:    durationpb.New(2 * time.Minute),
		GracePeriod: durationpb.New(5 * time.Second),
	}))
	require.NoError(t, err)

	configs, err := taskLog.GroupConfigs(ctx)
	require.NoError(t, err)
	require.Len(t, configs, 1)
	require.Equal(t, defaultQueueName, configs[0].Queue)
	require.Equal(t, "emails", configs[0].Group)
	require.Equal(t, 20, configs[0].MaxSize)
	require.Equal(t, 2*time.Minute, configs[0].MaxDelay)
	require.Equal(t, 5*time.Second, configs[0].GracePeriod)

	list, err := admin.ListGroupConfigs(ctx, connect.NewRequest(&conveyorv1.ListGroupConfigsRequest{}))
	require.NoError(t, err)
	require.Len(t, list.Msg.GetConfigs(), 1)
	require.Equal(t, "emails", list.Msg.GetConfigs()[0].GetGroup())
	require.EqualValues(t, 20, list.Msg.GetConfigs()[0].GetMaxSize())
	require.Equal(t, 2*time.Minute, list.Msg.GetConfigs()[0].GetMaxDelay().AsDuration())

	_, err = admin.DeleteGroupConfig(ctx, connect.NewRequest(&conveyorv1.DeleteGroupConfigRequest{Group: "emails"}))
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err), "delete rejects an empty queue")

	_, err = admin.DeleteGroupConfig(ctx, connect.NewRequest(&conveyorv1.DeleteGroupConfigRequest{Queue: defaultQueueName, Group: "emails"}))
	require.NoError(t, err)

	configs, err = taskLog.GroupConfigs(ctx)
	require.NoError(t, err)
	require.Empty(t, configs, "delete clears the override")
}

func TestListTasksPaginationAndFilters(t *testing.T) {
	admin, tasks, _ := newTestAdminService(t)
	ctx := context.Background()

	for sequence := 1; sequence <= 3; sequence++ {
		mustEnqueueType(t, tasks, &conveyorv1.EnqueueRequest{Type: fmt.Sprintf("test:%d", sequence)})
	}

	_, err := admin.ListTasks(ctx, connect.NewRequest(&conveyorv1.ListTasksRequest{Limit: -1}))
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

	first, err := admin.ListTasks(ctx, connect.NewRequest(&conveyorv1.ListTasksRequest{Limit: 2}))
	require.NoError(t, err)
	require.Len(t, first.Msg.GetTasks(), 2)
	require.NotEmpty(t, first.Msg.GetNextPageToken())
	require.Equal(t, conveyorv1.TaskState_TASK_STATE_PENDING, first.Msg.GetTasks()[0].GetState())

	second, err := admin.ListTasks(ctx, connect.NewRequest(&conveyorv1.ListTasksRequest{
		Limit:     2,
		PageToken: first.Msg.GetNextPageToken(),
	}))
	require.NoError(t, err)
	require.Len(t, second.Msg.GetTasks(), 1)
	require.Empty(t, second.Msg.GetNextPageToken())

	none, err := admin.ListTasks(ctx, connect.NewRequest(&conveyorv1.ListTasksRequest{
		State: conveyorv1.TaskState_TASK_STATE_ARCHIVED,
	}))
	require.NoError(t, err)
	require.Empty(t, none.Msg.GetTasks())
}

func TestListTasksSurfacesPayloadAndContentType(t *testing.T) {
	admin, tasks, _ := newTestAdminService(t)
	ctx := context.Background()

	mustEnqueueType(t, tasks, &conveyorv1.EnqueueRequest{
		Type:        "test:payload",
		Payload:     []byte(`{"hello":"world"}`),
		ContentType: "application/json",
	})

	listed, err := admin.ListTasks(ctx, connect.NewRequest(&conveyorv1.ListTasksRequest{}))
	require.NoError(t, err)
	require.Len(t, listed.Msg.GetTasks(), 1)

	task := listed.Msg.GetTasks()[0]
	require.Equal(t, []byte(`{"hello":"world"}`), task.GetPayload())
	require.Equal(t, "application/json", task.GetContentType())
	// Never dispatched: execution timestamps stay unset.
	require.Nil(t, task.GetStartedAt())
	require.Nil(t, task.GetCompletedAt())
}

func TestCancelTaskTransitions(t *testing.T) {
	admin, tasks, taskLog := newTestAdminService(t)
	ctx := context.Background()

	id := mustEnqueueType(t, tasks, &conveyorv1.EnqueueRequest{Type: "test:cancel", ProcessIn: durationpb.New(time.Hour)})

	_, err := admin.CancelTask(ctx, connect.NewRequest(&conveyorv1.CancelTaskRequest{}))
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

	_, err = admin.CancelTask(ctx, connect.NewRequest(&conveyorv1.CancelTaskRequest{Id: "missing"}))
	require.Equal(t, connect.CodeNotFound, connect.CodeOf(err))

	_, err = admin.CancelTask(ctx, connect.NewRequest(&conveyorv1.CancelTaskRequest{Id: id}))
	require.NoError(t, err)

	_, state, err := taskLog.GetTask(ctx, id)
	require.NoError(t, err)
	require.Equal(t, conveyorv1.TaskState_TASK_STATE_CANCELED, state)

	// A canceled task cannot be canceled again.
	_, err = admin.CancelTask(ctx, connect.NewRequest(&conveyorv1.CancelTaskRequest{Id: id}))
	require.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))
}

func TestCancelActiveTaskIsBestEffort(t *testing.T) {
	admin, tasks, taskLog := newTestAdminService(t)
	ctx := context.Background()

	// Pausing first keeps the grain from dispatching, so the manual lease
	// below is the only active delivery.
	require.NoError(t, taskLog.SetQueuePaused(ctx, defaultQueueName, true))

	id := mustEnqueueType(t, tasks, &conveyorv1.EnqueueRequest{Type: "test:active"})

	leased, err := taskLog.Lease(ctx, defaultQueueName, 1, time.Minute, "lease-1")
	require.NoError(t, err)
	require.Len(t, leased, 1)

	// The durable cancel is impossible while active; the RPC still
	// succeeds by routing a best-effort Cancel frame through the grain.
	_, err = admin.CancelTask(ctx, connect.NewRequest(&conveyorv1.CancelTaskRequest{Id: id}))
	require.NoError(t, err)

	_, state, err := taskLog.GetTask(ctx, id)
	require.NoError(t, err)
	require.Equal(t, conveyorv1.TaskState_TASK_STATE_ACTIVE, state)
}

func TestDeleteTaskRemovesRow(t *testing.T) {
	admin, tasks, taskLog := newTestAdminService(t)
	ctx := context.Background()

	id := mustEnqueueType(t, tasks, &conveyorv1.EnqueueRequest{Type: "test:delete", ProcessIn: durationpb.New(time.Hour)})

	_, err := admin.DeleteTask(ctx, connect.NewRequest(&conveyorv1.DeleteTaskRequest{Id: id}))
	require.NoError(t, err)

	_, _, err = taskLog.GetTask(ctx, id)
	require.ErrorIs(t, err, broker.ErrTaskNotFound)

	_, err = admin.DeleteTask(ctx, connect.NewRequest(&conveyorv1.DeleteTaskRequest{Id: id}))
	require.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
}

func TestRunTaskMakesScheduledDue(t *testing.T) {
	admin, tasks, taskLog := newTestAdminService(t)
	ctx := context.Background()

	id := mustEnqueueType(t, tasks, &conveyorv1.EnqueueRequest{Type: "test:run-now", ProcessIn: durationpb.New(time.Hour)})

	_, state, err := taskLog.GetTask(ctx, id)
	require.NoError(t, err)
	require.Equal(t, conveyorv1.TaskState_TASK_STATE_SCHEDULED, state)

	_, err = admin.RunTask(ctx, connect.NewRequest(&conveyorv1.RunTaskRequest{Id: id}))
	require.NoError(t, err)

	_, state, err = taskLog.GetTask(ctx, id)
	require.NoError(t, err)
	require.Equal(t, conveyorv1.TaskState_TASK_STATE_PENDING, state)

	_, err = admin.RunTask(ctx, connect.NewRequest(&conveyorv1.RunTaskRequest{Id: "missing"}))
	require.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
}

func TestRescheduleTaskMovesDueTime(t *testing.T) {
	admin, tasks, taskLog := newTestAdminService(t)
	ctx := context.Background()

	id := mustEnqueueType(t, tasks, &conveyorv1.EnqueueRequest{Type: "test:reschedule", ProcessIn: durationpb.New(time.Hour)})

	future := timestamppb.New(time.Date(2999, time.January, 1, 0, 0, 0, 0, time.UTC))

	// An absolute future time keeps the task scheduled.
	_, err := admin.RescheduleTask(ctx, connect.NewRequest(&conveyorv1.RescheduleTaskRequest{Id: id, ProcessAt: future}))
	require.NoError(t, err)

	_, state, err := taskLog.GetTask(ctx, id)
	require.NoError(t, err)
	require.Equal(t, conveyorv1.TaskState_TASK_STATE_SCHEDULED, state)

	// A relative delay (process_in) resolves against the server clock and, when
	// positive, also leaves the task scheduled.
	_, err = admin.RescheduleTask(ctx, connect.NewRequest(&conveyorv1.RescheduleTaskRequest{Id: id, ProcessIn: durationpb.New(time.Hour)}))
	require.NoError(t, err)

	_, state, err = taskLog.GetTask(ctx, id)
	require.NoError(t, err)
	require.Equal(t, conveyorv1.TaskState_TASK_STATE_SCHEDULED, state)

	// A past time makes the task due immediately.
	_, err = admin.RescheduleTask(ctx, connect.NewRequest(&conveyorv1.RescheduleTaskRequest{
		Id:        id,
		ProcessAt: timestamppb.New(time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)),
	}))
	require.NoError(t, err)

	_, state, err = taskLog.GetTask(ctx, id)
	require.NoError(t, err)
	require.Equal(t, conveyorv1.TaskState_TASK_STATE_PENDING, state)

	// Neither form set is rejected.
	_, err = admin.RescheduleTask(ctx, connect.NewRequest(&conveyorv1.RescheduleTaskRequest{Id: id}))
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

	// Both forms set is rejected.
	_, err = admin.RescheduleTask(ctx, connect.NewRequest(&conveyorv1.RescheduleTaskRequest{
		Id:        id,
		ProcessAt: future,
		ProcessIn: durationpb.New(time.Hour),
	}))
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

	// An empty id is rejected.
	_, err = admin.RescheduleTask(ctx, connect.NewRequest(&conveyorv1.RescheduleTaskRequest{ProcessAt: future}))
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

	// An unknown id is a not-found.
	_, err = admin.RescheduleTask(ctx, connect.NewRequest(&conveyorv1.RescheduleTaskRequest{Id: "missing", ProcessAt: future}))
	require.Equal(t, connect.CodeNotFound, connect.CodeOf(err))

	// A task in a terminal state cannot be rescheduled.
	canceled := mustEnqueueType(t, tasks, &conveyorv1.EnqueueRequest{Type: "test:reschedule-canceled"})

	_, err = admin.CancelTask(ctx, connect.NewRequest(&conveyorv1.CancelTaskRequest{Id: canceled}))
	require.NoError(t, err)

	_, err = admin.RescheduleTask(ctx, connect.NewRequest(&conveyorv1.RescheduleTaskRequest{Id: canceled, ProcessAt: future}))
	require.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))
}

func TestArchiveTaskDeadLetters(t *testing.T) {
	admin, tasks, taskLog := newTestAdminService(t)
	ctx := context.Background()

	id := mustEnqueueType(t, tasks, &conveyorv1.EnqueueRequest{Type: "test:archive"})

	_, err := admin.ArchiveTask(ctx, connect.NewRequest(&conveyorv1.ArchiveTaskRequest{Id: id}))
	require.NoError(t, err)

	_, state, err := taskLog.GetTask(ctx, id)
	require.NoError(t, err)
	require.Equal(t, conveyorv1.TaskState_TASK_STATE_ARCHIVED, state)

	_, err = admin.ArchiveTask(ctx, connect.NewRequest(&conveyorv1.ArchiveTaskRequest{Id: "missing"}))
	require.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
}

func TestBatchTaskActions(t *testing.T) {
	admin, tasks, taskLog := newTestAdminService(t)
	ctx := context.Background()

	first := mustEnqueueType(t, tasks, &conveyorv1.EnqueueRequest{Type: "test:batch-1"})
	second := mustEnqueueType(t, tasks, &conveyorv1.EnqueueRequest{Type: "test:batch-2"})

	// An empty batch is a whole-request error.
	_, err := admin.BatchDeleteTasks(ctx, connect.NewRequest(&conveyorv1.BatchTasksRequest{}))
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

	// A mixed batch reports a per-id error for the unknown id but still
	// applies the operation to the valid ones.
	resp, err := admin.BatchArchiveTasks(ctx, connect.NewRequest(&conveyorv1.BatchTasksRequest{
		Ids: []string{first, second, "missing"},
	}))
	require.NoError(t, err)
	require.Len(t, resp.Msg.GetResults(), 3)
	require.Empty(t, resp.Msg.GetResults()[0].GetError())
	require.Empty(t, resp.Msg.GetResults()[1].GetError())
	require.NotEmpty(t, resp.Msg.GetResults()[2].GetError())

	for _, id := range []string{first, second} {
		_, state, err := taskLog.GetTask(ctx, id)
		require.NoError(t, err)
		require.Equal(t, conveyorv1.TaskState_TASK_STATE_ARCHIVED, state)
	}
}

func TestCronLifecycle(t *testing.T) {
	admin, _, _ := newTestAdminService(t)
	ctx := context.Background()

	invalid := map[string]*conveyorv1.CronEntry{
		"missing id":        {Spec: "0 * * * * *", TaskType: "report:hourly"},
		"missing task type": {Id: "hourly", Spec: "0 * * * * *"},
		"bad spec":          {Id: "hourly", Spec: "not-a-spec", TaskType: "report:hourly"},
		"bad queue":         {Id: "hourly", Spec: "0 * * * * *", TaskType: "report:hourly", Queue: "no spaces"},
	}

	for name, entry := range invalid {
		_, err := admin.UpsertCron(ctx, connect.NewRequest(&conveyorv1.UpsertCronRequest{Entry: entry}))
		require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err), "case %s", name)
	}

	_, err := admin.UpsertCron(ctx, connect.NewRequest(&conveyorv1.UpsertCronRequest{
		Entry: &conveyorv1.CronEntry{Id: "hourly", Spec: "0 * * * * *", TaskType: "report:hourly"},
	}))
	require.NoError(t, err)

	listed, err := admin.ListCron(ctx, connect.NewRequest(&conveyorv1.ListCronRequest{}))
	require.NoError(t, err)
	require.Len(t, listed.Msg.GetEntries(), 1)
	require.Equal(t, "hourly", listed.Msg.GetEntries()[0].GetId())
	require.Equal(t, defaultQueueName, listed.Msg.GetEntries()[0].GetQueue())

	_, err = admin.PauseCron(ctx, connect.NewRequest(&conveyorv1.PauseCronRequest{Id: "hourly"}))
	require.NoError(t, err)

	listed, _ = admin.ListCron(ctx, connect.NewRequest(&conveyorv1.ListCronRequest{}))
	require.True(t, listed.Msg.GetEntries()[0].GetPaused())

	_, err = admin.ResumeCron(ctx, connect.NewRequest(&conveyorv1.ResumeCronRequest{Id: "hourly"}))
	require.NoError(t, err)

	listed, _ = admin.ListCron(ctx, connect.NewRequest(&conveyorv1.ListCronRequest{}))
	require.False(t, listed.Msg.GetEntries()[0].GetPaused())

	_, err = admin.PauseCron(ctx, connect.NewRequest(&conveyorv1.PauseCronRequest{Id: "missing"}))
	require.Equal(t, connect.CodeNotFound, connect.CodeOf(err))

	_, err = admin.DeleteCron(ctx, connect.NewRequest(&conveyorv1.DeleteCronRequest{Id: "hourly"}))
	require.NoError(t, err)

	listed, _ = admin.ListCron(ctx, connect.NewRequest(&conveyorv1.ListCronRequest{}))
	require.Empty(t, listed.Msg.GetEntries())
}

func TestBrokerInfoReportsDriver(t *testing.T) {
	admin, _, _ := newTestAdminService(t)

	resp, err := admin.BrokerInfo(context.Background(), connect.NewRequest(&conveyorv1.BrokerInfoRequest{}))
	require.NoError(t, err)
	require.Equal(t, "memory", resp.Msg.GetDriver())
	require.Contains(t, resp.Msg.GetMetrics(), "tasks")
}

func TestClusterInfoReportsSelf(t *testing.T) {
	admin, _, _ := newTestAdminService(t)

	response, err := admin.ClusterInfo(context.Background(), connect.NewRequest(&conveyorv1.ClusterInfoRequest{}))
	require.NoError(t, err)

	nodes := response.Msg.GetNodes()
	require.NotEmpty(t, nodes)
	require.NotEmpty(t, nodes[0].GetAddress())
	require.True(t, nodes[0].GetStartedAt().IsValid())
}

// TestListWorkerSessions maps the session lister's snapshots into the response.
func TestListWorkerSessions(t *testing.T) {
	engine, taskLog := startTestEngine(t)

	connected := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)
	admin := NewAdminService(engine, taskLog, clock.System(), stubSessions{
		{ID: "s1", Queues: []string{"default", "critical"}, Concurrency: 8, SDKVersion: "v1.2.3", ConnectedAt: connected},
	}, true)

	resp, err := admin.ListWorkerSessions(context.Background(), connect.NewRequest(&conveyorv1.ListWorkerSessionsRequest{}))
	require.NoError(t, err)

	sessions := resp.Msg.GetSessions()
	require.Len(t, sessions, 1)
	require.Equal(t, "s1", sessions[0].GetId())
	require.Equal(t, []string{"default", "critical"}, sessions[0].GetQueues())
	require.EqualValues(t, 8, sessions[0].GetConcurrency())
	require.Equal(t, "v1.2.3", sessions[0].GetSdkVersion())
	require.True(t, sessions[0].GetConnectedAt().AsTime().Equal(connected))
}
