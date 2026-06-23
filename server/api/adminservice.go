// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"connectrpc.com/connect"
	"github.com/reugn/go-quartz/quartz"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/conveyorq/conveyor/internal/actors"
	"github.com/conveyorq/conveyor/internal/broker"
	"github.com/conveyorq/conveyor/internal/clock"
	"github.com/conveyorq/conveyor/internal/events"
	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
	"github.com/conveyorq/conveyor/internal/proto/conveyor/v1/conveyorv1connect"
)

// peersLookupTimeout bounds the best-effort cluster membership lookup in
// ClusterInfo. It is deliberately short: the snapshot is advisory, and a
// slow or partitioned membership view must not stall admin polling.
const peersLookupTimeout = 500 * time.Millisecond

// SessionLister reports the worker sessions connected to this node. The
// WorkerService implements it; AdminService surfaces it for the dashboard.
type SessionLister interface {
	// Sessions returns a snapshot of the live worker sessions.
	Sessions() []SessionSnapshot
}

// AdminService serves the inspection and operations API.
type AdminService struct {
	// engine reaches the queue grains for pause, resume, and wake-ups.
	engine *actors.Engine
	// taskLog is the durable task log all admin operations act on.
	taskLog broker.Broker
	// timeSource anchors derived instants such as node start times.
	timeSource clock.Clock
	// sessions lists the worker sessions connected to this node.
	sessions SessionLister
}

// enforce interface compliance at compile time.
var _ conveyorv1connect.AdminServiceHandler = (*AdminService)(nil)

// NewAdminService assembles the admin API service.
func NewAdminService(engine *actors.Engine, taskLog broker.Broker, timeSource clock.Clock, sessions SessionLister) *AdminService {
	return &AdminService{
		engine:     engine,
		taskLog:    taskLog,
		timeSource: timeSource,
		sessions:   sessions,
	}
}

// ListQueues reports every known queue with its pause flag and per-state
// task counts.
func (s *AdminService) ListQueues(ctx context.Context, _ *connect.Request[conveyorv1.ListQueuesRequest]) (*connect.Response[conveyorv1.ListQueuesResponse], error) {
	stats, err := s.taskLog.QueueStats(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	queues := make([]*conveyorv1.QueueInfo, 0, len(stats))

	for _, stat := range stats {
		queues = append(queues, &conveyorv1.QueueInfo{
			Name:        stat.Queue,
			Paused:      stat.Paused,
			Scheduled:   stat.Scheduled,
			Pending:     stat.Pending,
			Active:      stat.Active,
			Retry:       stat.Retry,
			Completed:   stat.Completed,
			Archived:    stat.Archived,
			Aggregating: stat.Aggregating,
			Blocked:     stat.Blocked,
		})
	}

	return connect.NewResponse(&conveyorv1.ListQueuesResponse{Queues: queues}), nil
}

// PauseQueue persists the pause flag and drains the live queue grain so
// dispatching stops immediately; queued work stays in the broker.
func (s *AdminService) PauseQueue(ctx context.Context, request *connect.Request[conveyorv1.PauseQueueRequest]) (*connect.Response[conveyorv1.PauseQueueResponse], error) {
	queue, err := validQueueName(request.Msg.GetQueue())
	if err != nil {
		return nil, err
	}

	if err := s.taskLog.SetQueuePaused(ctx, queue, true); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	if err := s.engine.TellQueue(ctx, queue, &conveyorv1.DrainQueue{Queue: queue}); err != nil {
		return nil, connect.NewError(connect.CodeInternal,
			fmt.Errorf("pause persisted but draining the live dispatcher failed, retry to take effect immediately: %w", err))
	}

	return connect.NewResponse(&conveyorv1.PauseQueueResponse{}), nil
}

// ResumeQueue clears the pause flag and triggers an immediate lease cycle
// on the queue grain.
func (s *AdminService) ResumeQueue(ctx context.Context, request *connect.Request[conveyorv1.ResumeQueueRequest]) (*connect.Response[conveyorv1.ResumeQueueResponse], error) {
	queue, err := validQueueName(request.Msg.GetQueue())
	if err != nil {
		return nil, err
	}

	if err := s.taskLog.SetQueuePaused(ctx, queue, false); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	if err := s.engine.TellQueue(ctx, queue, &conveyorv1.ResumeQueue{Queue: queue}); err != nil {
		return nil, connect.NewError(connect.CodeInternal,
			fmt.Errorf("resume persisted but waking the live dispatcher failed, retry to take effect immediately: %w", err))
	}

	return connect.NewResponse(&conveyorv1.ResumeQueueResponse{}), nil
}

// ListRateLimits returns every per-queue dispatch-rate override, ordered by
// queue name.
func (s *AdminService) ListRateLimits(ctx context.Context, _ *connect.Request[conveyorv1.ListRateLimitsRequest]) (*connect.Response[conveyorv1.ListRateLimitsResponse], error) {
	limits, err := s.taskLog.QueueRateLimits(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	infos := make([]*conveyorv1.RateLimitInfo, 0, len(limits))

	for _, limit := range limits {
		infos = append(infos, &conveyorv1.RateLimitInfo{
			Queue:      limit.Queue,
			RatePerSec: limit.RatePerSec,
			Burst:      int32(limit.Burst),
		})
	}

	return connect.NewResponse(&conveyorv1.ListRateLimitsResponse{Limits: infos}), nil
}

// SetQueueRateLimit persists a queue's dispatch-rate override and pushes the new
// values to the live queue grain so the limit takes effect immediately.
func (s *AdminService) SetQueueRateLimit(ctx context.Context, request *connect.Request[conveyorv1.SetQueueRateLimitRequest]) (*connect.Response[conveyorv1.SetQueueRateLimitResponse], error) {
	queue, err := validQueueName(request.Msg.GetQueue())
	if err != nil {
		return nil, err
	}

	rate := request.Msg.GetRatePerSec()
	if rate <= 0 || math.IsNaN(rate) || math.IsInf(rate, 0) {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("rate_per_sec must be a positive finite number, got %v", rate))
	}

	burst := request.Msg.GetBurst()
	if burst < 1 {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("burst must be at least 1, got %d", burst))
	}

	if err := s.taskLog.SetQueueRateLimit(ctx, queue, rate, int(burst)); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	if err := s.engine.TellQueue(ctx, queue, &conveyorv1.RateLimitChanged{Queue: queue, RatePerSec: rate, Burst: burst}); err != nil {
		return nil, connect.NewError(connect.CodeInternal,
			fmt.Errorf("rate limit persisted but updating the live dispatcher failed, retry to take effect immediately: %w", err))
	}

	return connect.NewResponse(&conveyorv1.SetQueueRateLimitResponse{}), nil
}

// DeleteQueueRateLimit clears a queue's override and reverts the live queue
// grain to the server's global default (a zero rate in the change message).
func (s *AdminService) DeleteQueueRateLimit(ctx context.Context, request *connect.Request[conveyorv1.DeleteQueueRateLimitRequest]) (*connect.Response[conveyorv1.DeleteQueueRateLimitResponse], error) {
	queue, err := validQueueName(request.Msg.GetQueue())
	if err != nil {
		return nil, err
	}

	if err := s.taskLog.DeleteQueueRateLimit(ctx, queue); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	if err := s.engine.TellQueue(ctx, queue, &conveyorv1.RateLimitChanged{Queue: queue}); err != nil {
		return nil, connect.NewError(connect.CodeInternal,
			fmt.Errorf("rate limit cleared but updating the live dispatcher failed, retry to take effect immediately: %w", err))
	}

	return connect.NewResponse(&conveyorv1.DeleteQueueRateLimitResponse{}), nil
}

// ListConcurrencyLimits returns every per-queue concurrency limit, ordered by
// queue name.
func (s *AdminService) ListConcurrencyLimits(ctx context.Context, _ *connect.Request[conveyorv1.ListConcurrencyLimitsRequest]) (*connect.Response[conveyorv1.ListConcurrencyLimitsResponse], error) {
	limits, err := s.taskLog.QueueConcurrencyLimits(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	infos := make([]*conveyorv1.ConcurrencyLimitInfo, 0, len(limits))

	for _, limit := range limits {
		infos = append(infos, &conveyorv1.ConcurrencyLimitInfo{
			Queue:     limit.Queue,
			MaxActive: int32(limit.MaxActive),
		})
	}

	return connect.NewResponse(&conveyorv1.ListConcurrencyLimitsResponse{Limits: infos}), nil
}

// SetQueueConcurrencyLimit persists a queue's per-key concurrency limit and
// pushes the new value to the live queue grain so it takes effect immediately.
func (s *AdminService) SetQueueConcurrencyLimit(ctx context.Context, request *connect.Request[conveyorv1.SetQueueConcurrencyLimitRequest]) (*connect.Response[conveyorv1.SetQueueConcurrencyLimitResponse], error) {
	queue, err := validQueueName(request.Msg.GetQueue())
	if err != nil {
		return nil, err
	}

	maxActive := request.Msg.GetMaxActive()
	if maxActive < 1 {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("max_active must be at least 1, got %d", maxActive))
	}

	if err := s.taskLog.SetQueueConcurrencyLimit(ctx, queue, int(maxActive)); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	if err := s.engine.TellQueue(ctx, queue, &conveyorv1.ConcurrencyLimitChanged{Queue: queue, MaxActive: maxActive}); err != nil {
		return nil, connect.NewError(connect.CodeInternal,
			fmt.Errorf("concurrency limit persisted but updating the live dispatcher failed, retry to take effect immediately: %w", err))
	}

	return connect.NewResponse(&conveyorv1.SetQueueConcurrencyLimitResponse{}), nil
}

// DeleteQueueConcurrencyLimit clears a queue's concurrency limit and reverts the
// live queue grain to unbounded keys (a zero max-active in the change message).
func (s *AdminService) DeleteQueueConcurrencyLimit(ctx context.Context, request *connect.Request[conveyorv1.DeleteQueueConcurrencyLimitRequest]) (*connect.Response[conveyorv1.DeleteQueueConcurrencyLimitResponse], error) {
	queue, err := validQueueName(request.Msg.GetQueue())
	if err != nil {
		return nil, err
	}

	if err := s.taskLog.DeleteQueueConcurrencyLimit(ctx, queue); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	if err := s.engine.TellQueue(ctx, queue, &conveyorv1.ConcurrencyLimitChanged{Queue: queue}); err != nil {
		return nil, connect.NewError(connect.CodeInternal,
			fmt.Errorf("concurrency limit cleared but updating the live dispatcher failed, retry to take effect immediately: %w", err))
	}

	return connect.NewResponse(&conveyorv1.DeleteQueueConcurrencyLimitResponse{}), nil
}

// ListGroupConfigs returns every per-group aggregation override, ordered by
// queue then group.
func (s *AdminService) ListGroupConfigs(ctx context.Context, _ *connect.Request[conveyorv1.ListGroupConfigsRequest]) (*connect.Response[conveyorv1.ListGroupConfigsResponse], error) {
	configs, err := s.taskLog.GroupConfigs(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	infos := make([]*conveyorv1.GroupConfigInfo, 0, len(configs))

	for _, config := range configs {
		infos = append(infos, &conveyorv1.GroupConfigInfo{
			Queue:       config.Queue,
			Group:       config.Group,
			MaxSize:     int32(config.MaxSize),
			MaxDelay:    durationpb.New(config.MaxDelay),
			GracePeriod: durationpb.New(config.GracePeriod),
		})
	}

	return connect.NewResponse(&conveyorv1.ListGroupConfigsResponse{Configs: infos}), nil
}

// SetGroupConfig persists a group's aggregation override. The group sweeper
// reads the new value on its next tick (firing is poll-based), so no live actor
// wake is needed, unlike the queue-grain rate-limit path. An empty group sets
// the queue-wide default applied to every group on the queue without its own
// override.
func (s *AdminService) SetGroupConfig(ctx context.Context, request *connect.Request[conveyorv1.SetGroupConfigRequest]) (*connect.Response[conveyorv1.SetGroupConfigResponse], error) {
	queue, err := validQueueName(request.Msg.GetQueue())
	if err != nil {
		return nil, err
	}

	maxSize := request.Msg.GetMaxSize()
	if maxSize < 1 {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("max_size must be at least 1, got %d", maxSize))
	}

	maxDelay := request.Msg.GetMaxDelay().AsDuration()
	if maxDelay <= 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("max_delay must be positive, got %v", maxDelay))
	}

	gracePeriod := request.Msg.GetGracePeriod().AsDuration()
	if gracePeriod <= 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("grace_period must be positive, got %v", gracePeriod))
	}

	if err := s.taskLog.SetGroupConfig(ctx, queue, request.Msg.GetGroup(), int(maxSize), maxDelay, gracePeriod); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&conveyorv1.SetGroupConfigResponse{}), nil
}

// DeleteGroupConfig clears a group's override, reverting it to the queue-wide
// or global default. The sweeper picks up the change on its next tick.
func (s *AdminService) DeleteGroupConfig(ctx context.Context, request *connect.Request[conveyorv1.DeleteGroupConfigRequest]) (*connect.Response[conveyorv1.DeleteGroupConfigResponse], error) {
	queue, err := validQueueName(request.Msg.GetQueue())
	if err != nil {
		return nil, err
	}

	if err := s.taskLog.DeleteGroupConfig(ctx, queue, request.Msg.GetGroup()); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&conveyorv1.DeleteGroupConfigResponse{}), nil
}

// ListTasks pages through tasks newest first, optionally filtered by queue
// and state.
func (s *AdminService) ListTasks(ctx context.Context, request *connect.Request[conveyorv1.ListTasksRequest]) (*connect.Response[conveyorv1.ListTasksResponse], error) {
	if request.Msg.GetLimit() < 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("limit must not be negative, got %d", request.Msg.GetLimit()))
	}

	records, err := s.taskLog.ListTasks(ctx, broker.TaskQuery{
		Queue:   request.Msg.GetQueue(),
		State:   request.Msg.GetState(),
		Limit:   int(request.Msg.GetLimit()),
		AfterID: request.Msg.GetPageToken(),
	})
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	tasks := make([]*conveyorv1.TaskInfo, 0, len(records))
	for _, record := range records {
		tasks = append(tasks, taskInfo(record.Envelope, record.State))
	}

	response := &conveyorv1.ListTasksResponse{Tasks: tasks}

	// A full page may have more results behind it; an underfull page is
	// definitively the last one.
	if len(records) == broker.EffectiveListLimit(int(request.Msg.GetLimit())) {
		response.NextPageToken = records[len(records)-1].Envelope.GetId()
	}

	return connect.NewResponse(response), nil
}

// CancelTask cancels a scheduled, pending, or retry task durably. An
// active task instead receives a best-effort Cancel frame through its
// executing worker session: the handler's context is canceled and the
// aborted attempt is archived instead of retried; only a genuine success
// reported before the cancel lands still completes the task.
func (s *AdminService) CancelTask(ctx context.Context, request *connect.Request[conveyorv1.CancelTaskRequest]) (*connect.Response[conveyorv1.CancelTaskResponse], error) {
	if err := requireTaskID(request.Msg.GetId()); err != nil {
		return nil, err
	}

	err := s.taskLog.CancelTask(ctx, request.Msg.GetId())

	if errors.Is(err, broker.ErrInvalidState) {
		if frameErr := s.cancelActive(ctx, request.Msg.GetId()); frameErr == nil {
			return connect.NewResponse(&conveyorv1.CancelTaskResponse{}), nil
		}
	}

	if err != nil {
		return nil, adminTaskError(err)
	}

	return connect.NewResponse(&conveyorv1.CancelTaskResponse{}), nil
}

// cancelActive routes a best-effort cancel request for an executing task
// through its queue grain. It reports an error when the task is not
// active, letting the caller surface the original state error instead.
func (s *AdminService) cancelActive(ctx context.Context, id string) error {
	envelope, state, err := s.taskLog.GetTask(ctx, id)
	if err != nil {
		return err
	}

	if state != conveyorv1.TaskState_TASK_STATE_ACTIVE {
		return broker.ErrInvalidState
	}

	queue := envelope.GetQueue()

	return s.engine.TellQueue(ctx, queue, &conveyorv1.CancelActive{TaskId: id})
}

// DeleteTask removes a task in any state except active.
func (s *AdminService) DeleteTask(ctx context.Context, request *connect.Request[conveyorv1.DeleteTaskRequest]) (*connect.Response[conveyorv1.DeleteTaskResponse], error) {
	if err := requireTaskID(request.Msg.GetId()); err != nil {
		return nil, err
	}

	if err := s.taskLog.DeleteTask(ctx, request.Msg.GetId()); err != nil {
		return nil, adminTaskError(err)
	}

	return connect.NewResponse(&conveyorv1.DeleteTaskResponse{}), nil
}

// RunTask makes a scheduled, pending, retry, or archived task due immediately
// and wakes its queue grain.
func (s *AdminService) RunTask(ctx context.Context, request *connect.Request[conveyorv1.RunTaskRequest]) (*connect.Response[conveyorv1.RunTaskResponse], error) {
	if err := requireTaskID(request.Msg.GetId()); err != nil {
		return nil, err
	}

	if err := s.runTask(ctx, request.Msg.GetId()); err != nil {
		return nil, adminTaskError(err)
	}

	return connect.NewResponse(&conveyorv1.RunTaskResponse{}), nil
}

// RescheduleTask moves a waiting (scheduled, pending, or retry) task's due time
// to a new instant. A future time leaves the task scheduled; a past or present
// time makes it due immediately and wakes its queue grain.
func (s *AdminService) RescheduleTask(ctx context.Context, request *connect.Request[conveyorv1.RescheduleTaskRequest]) (*connect.Response[conveyorv1.RescheduleTaskResponse], error) {
	if err := requireTaskID(request.Msg.GetId()); err != nil {
		return nil, err
	}

	processAt, err := s.rescheduleDueTime(request.Msg)
	if err != nil {
		return nil, err
	}

	if err := s.rescheduleTask(ctx, request.Msg.GetId(), processAt); err != nil {
		return nil, adminTaskError(err)
	}

	return connect.NewResponse(&conveyorv1.RescheduleTaskResponse{}), nil
}

// rescheduleDueTime resolves the new due time from the mutually exclusive
// process_at and process_in forms; exactly one must be set. A relative delay is
// anchored to the server clock, matching the enqueue path.
func (s *AdminService) rescheduleDueTime(request *conveyorv1.RescheduleTaskRequest) (time.Time, error) {
	at := request.GetProcessAt()
	in := request.GetProcessIn()

	if at.IsValid() && in.IsValid() {
		return time.Time{}, connect.NewError(connect.CodeInvalidArgument, errors.New("process_at and process_in are mutually exclusive"))
	}

	switch {
	case at.IsValid():
		return at.AsTime(), nil

	case in.IsValid():
		return s.timeSource.Now().Add(in.AsDuration()), nil

	default:
		return time.Time{}, connect.NewError(connect.CodeInvalidArgument, errors.New("one of process_at or process_in is required"))
	}
}

// ArchiveTask dead-letters a waiting (scheduled, pending, or retry) task.
func (s *AdminService) ArchiveTask(ctx context.Context, request *connect.Request[conveyorv1.ArchiveTaskRequest]) (*connect.Response[conveyorv1.ArchiveTaskResponse], error) {
	if err := requireTaskID(request.Msg.GetId()); err != nil {
		return nil, err
	}

	if err := s.taskLog.ArchiveTask(ctx, request.Msg.GetId()); err != nil {
		return nil, adminTaskError(err)
	}

	return connect.NewResponse(&conveyorv1.ArchiveTaskResponse{}), nil
}

// BatchDeleteTasks deletes each listed task, reporting per-id outcomes.
func (s *AdminService) BatchDeleteTasks(ctx context.Context, request *connect.Request[conveyorv1.BatchTasksRequest]) (*connect.Response[conveyorv1.BatchTasksResponse], error) {
	return s.batchTasks(request.Msg.GetIds(), func(id string) error {
		return s.taskLog.DeleteTask(ctx, id)
	})
}

// BatchRunTasks makes each listed task due immediately.
func (s *AdminService) BatchRunTasks(ctx context.Context, request *connect.Request[conveyorv1.BatchTasksRequest]) (*connect.Response[conveyorv1.BatchTasksResponse], error) {
	return s.batchTasks(request.Msg.GetIds(), func(id string) error {
		return s.runTask(ctx, id)
	})
}

// BatchCancelTasks cancels each listed task.
func (s *AdminService) BatchCancelTasks(ctx context.Context, request *connect.Request[conveyorv1.BatchTasksRequest]) (*connect.Response[conveyorv1.BatchTasksResponse], error) {
	return s.batchTasks(request.Msg.GetIds(), func(id string) error {
		return s.taskLog.CancelTask(ctx, id)
	})
}

// BatchArchiveTasks dead-letters each listed task.
func (s *AdminService) BatchArchiveTasks(ctx context.Context, request *connect.Request[conveyorv1.BatchTasksRequest]) (*connect.Response[conveyorv1.BatchTasksResponse], error) {
	return s.batchTasks(request.Msg.GetIds(), func(id string) error {
		return s.taskLog.ArchiveTask(ctx, id)
	})
}

// batchTasks applies op to each id, collecting a per-id result. A whole-batch
// failure (empty or oversized id list) returns an error; per-id failures are
// reported in the response so a partial batch still succeeds.
func (s *AdminService) batchTasks(ids []string, op func(string) error) (*connect.Response[conveyorv1.BatchTasksResponse], error) {
	if len(ids) == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("at least one task id is required"))
	}

	if len(ids) > maxBatchTasks {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("batch of %d exceeds the %d-task limit", len(ids), maxBatchTasks))
	}

	results := make([]*conveyorv1.TaskActionResult, 0, len(ids))

	for _, id := range ids {
		result := &conveyorv1.TaskActionResult{Id: id}

		if id == "" {
			result.Error = "task id is required"
		} else if err := op(id); err != nil {
			result.Error = err.Error()
		}

		results = append(results, result)
	}

	return connect.NewResponse(&conveyorv1.BatchTasksResponse{Results: results}), nil
}

// runTask makes one task due immediately and best-effort wakes its queue
// grain. It backs both RunTask and BatchRunTasks.
func (s *AdminService) runTask(ctx context.Context, id string) error {
	envelope, _, err := s.taskLog.GetTask(ctx, id)
	if err != nil {
		return err
	}

	if err := s.taskLog.RunTaskNow(ctx, id); err != nil {
		return err
	}

	// The wake-up is a best-effort hint; the reaper sweep recovers lost ones.
	queue := envelope.GetQueue()
	_ = s.engine.TellQueue(ctx, queue, &conveyorv1.TaskEnqueued{Queue: queue})

	return nil
}

// rescheduleTask moves one task's due time to processAt. A task that is now due
// gets a best-effort wake so it dispatches at once; a future task is left for
// the scheduler's promotion sweep to pick up, so it needs neither a lookup nor
// a wake.
func (s *AdminService) rescheduleTask(ctx context.Context, id string, processAt time.Time) error {
	if err := s.taskLog.RescheduleTask(ctx, id, processAt); err != nil {
		return err
	}

	if processAt.After(s.timeSource.Now()) {
		return nil
	}

	// Best-effort wake; the reaper sweep recovers a lost hint, and a lookup
	// failure here does not undo the reschedule that already committed.
	if envelope, _, err := s.taskLog.GetTask(ctx, id); err == nil {
		queue := envelope.GetQueue()
		_ = s.engine.TellQueue(ctx, queue, &conveyorv1.TaskEnqueued{Queue: queue})
	}

	return nil
}

// ListCron returns all persisted cron entries ordered by id.
func (s *AdminService) ListCron(ctx context.Context, _ *connect.Request[conveyorv1.ListCronRequest]) (*connect.Response[conveyorv1.ListCronResponse], error) {
	entries, err := s.taskLog.ListCronEntries(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	infos := make([]*conveyorv1.CronEntry, 0, len(entries))

	for _, entry := range entries {
		info := &conveyorv1.CronEntry{
			Id:          entry.ID,
			Spec:        entry.Spec,
			TaskType:    entry.TaskType,
			Queue:       entry.Queue,
			Payload:     entry.Payload,
			ContentType: entry.ContentType,
			Options:     entry.Options,
			Paused:      entry.Paused,
		}

		if !entry.NextRunAt.IsZero() {
			info.NextRunAt = timestamppb.New(entry.NextRunAt)
		}

		infos = append(infos, info)
	}

	return connect.NewResponse(&conveyorv1.ListCronResponse{Entries: infos}), nil
}

// UpsertCron validates and persists a cron entry by id.
func (s *AdminService) UpsertCron(ctx context.Context, request *connect.Request[conveyorv1.UpsertCronRequest]) (*connect.Response[conveyorv1.UpsertCronResponse], error) {
	entry := request.Msg.GetEntry()

	if entry.GetId() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("cron entry id is required"))
	}

	if entry.GetTaskType() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("cron entry task type is required"))
	}

	if err := quartz.ValidateCronExpression(entry.GetSpec()); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("invalid cron spec %q (6-field quartz format): %w", entry.GetSpec(), err))
	}

	queue := entry.GetQueue()
	if queue == "" {
		queue = defaultQueueName
	}

	if !queueNamePattern.MatchString(queue) {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid queue name %q", queue))
	}

	if entry.GetOptions().GetGroup() != "" && entry.GetOptions().GetConcurrencyKey() != "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("group and concurrency_key are mutually exclusive"))
	}

	err := s.taskLog.UpsertCronEntry(ctx, &broker.CronEntry{
		ID:          entry.GetId(),
		Spec:        entry.GetSpec(),
		TaskType:    entry.GetTaskType(),
		Queue:       queue,
		Payload:     entry.GetPayload(),
		ContentType: entry.GetContentType(),
		Options:     entry.GetOptions(),
		Paused:      entry.GetPaused(),
	})
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&conveyorv1.UpsertCronResponse{}), nil
}

// PauseCron suspends materialization of one cron entry.
func (s *AdminService) PauseCron(ctx context.Context, request *connect.Request[conveyorv1.PauseCronRequest]) (*connect.Response[conveyorv1.PauseCronResponse], error) {
	if err := s.setCronPaused(ctx, request.Msg.GetId(), true); err != nil {
		return nil, err
	}

	return connect.NewResponse(&conveyorv1.PauseCronResponse{}), nil
}

// ResumeCron resumes materialization of one cron entry.
func (s *AdminService) ResumeCron(ctx context.Context, request *connect.Request[conveyorv1.ResumeCronRequest]) (*connect.Response[conveyorv1.ResumeCronResponse], error) {
	if err := s.setCronPaused(ctx, request.Msg.GetId(), false); err != nil {
		return nil, err
	}

	return connect.NewResponse(&conveyorv1.ResumeCronResponse{}), nil
}

// DeleteCron removes a cron entry; deleting an absent id succeeds.
func (s *AdminService) DeleteCron(ctx context.Context, request *connect.Request[conveyorv1.DeleteCronRequest]) (*connect.Response[conveyorv1.DeleteCronResponse], error) {
	if request.Msg.GetId() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("cron entry id is required"))
	}

	if err := s.taskLog.DeleteCronEntry(ctx, request.Msg.GetId()); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&conveyorv1.DeleteCronResponse{}), nil
}

// ClusterInfo reports this node and its best-effort view of cluster peers.
func (s *AdminService) ClusterInfo(ctx context.Context, _ *connect.Request[conveyorv1.ClusterInfoRequest]) (*connect.Response[conveyorv1.ClusterInfoResponse], error) {
	system := s.engine.System()

	nodes := []*conveyorv1.NodeInfo{{
		Address:   system.PeersAddress(),
		StartedAt: timestamppb.New(s.timeSource.Now().Add(-time.Duration(system.Uptime()) * time.Second)),
	}}

	// Membership is an eventually consistent snapshot; a failed lookup
	// still reports this node.
	peers, err := system.Peers(ctx, peersLookupTimeout)
	if err == nil {
		for _, peer := range peers {
			node := &conveyorv1.NodeInfo{Address: fmt.Sprintf("%s:%d", peer.Host, peer.PeersPort)}

			// CreatedAt is the peer's discovery time in Unix nanoseconds; it is
			// the join time visible from this node (peers do not report their
			// own uptime). Zero means unknown.
			if peer.CreatedAt > 0 {
				node.StartedAt = timestamppb.New(time.Unix(0, peer.CreatedAt))
			}

			nodes = append(nodes, node)
		}
	}

	return connect.NewResponse(&conveyorv1.ClusterInfoResponse{Nodes: nodes}), nil
}

// ListWorkerSessions reports the worker sessions connected to this node.
func (s *AdminService) ListWorkerSessions(_ context.Context, _ *connect.Request[conveyorv1.ListWorkerSessionsRequest]) (*connect.Response[conveyorv1.ListWorkerSessionsResponse], error) {
	snapshots := s.sessions.Sessions()

	sessions := make([]*conveyorv1.WorkerSession, 0, len(snapshots))

	for _, snapshot := range snapshots {
		sessions = append(sessions, &conveyorv1.WorkerSession{
			Id:          snapshot.ID,
			Queues:      snapshot.Queues,
			Concurrency: snapshot.Concurrency,
			SdkVersion:  snapshot.SDKVersion,
			ConnectedAt: timestamppb.New(snapshot.ConnectedAt),
		})
	}

	return connect.NewResponse(&conveyorv1.ListWorkerSessionsResponse{Sessions: sessions}), nil
}

// WatchEvents streams task lifecycle transitions to the caller until the client
// disconnects. It subscribes to the node-local event bus with the request's
// queue and event-type filters; a watcher too slow to keep up has events
// dropped (counted by the events.dropped metric), never stalling dispatch.
func (s *AdminService) WatchEvents(ctx context.Context, request *connect.Request[conveyorv1.WatchEventsRequest], stream *connect.ServerStream[conveyorv1.TaskEvent]) error {
	if !s.engine.EventsEnabled() {
		return connect.NewError(connect.CodeUnavailable, errors.New("the lifecycle event stream is disabled on this server"))
	}

	filter := events.NewFilter(request.Msg.GetQueues(), request.Msg.GetEventTypes())

	channel, cancel := s.engine.EventBus().Subscribe(filter)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			return nil

		case event, ok := <-channel:
			if !ok {
				return nil
			}

			if err := stream.Send(event); err != nil {
				return err
			}
		}
	}
}

// BrokerInfo reports the storage engine's driver and runtime statistics.
func (s *AdminService) BrokerInfo(ctx context.Context, _ *connect.Request[conveyorv1.BrokerInfoRequest]) (*connect.Response[conveyorv1.BrokerInfoResponse], error) {
	info, err := s.taskLog.Info(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&conveyorv1.BrokerInfoResponse{
		Driver:  info.Driver,
		Metrics: info.Metrics,
	}), nil
}

// setCronPaused validates the id and persists the entry pause flag.
func (s *AdminService) setCronPaused(ctx context.Context, id string, paused bool) error {
	if id == "" {
		return connect.NewError(connect.CodeInvalidArgument, errors.New("cron entry id is required"))
	}

	err := s.taskLog.SetCronPaused(ctx, id, paused)

	if errors.Is(err, broker.ErrTaskNotFound) {
		return connect.NewError(connect.CodeNotFound, fmt.Errorf("cron entry %q does not exist", id))
	}

	if err != nil {
		return connect.NewError(connect.CodeInternal, err)
	}

	return nil
}

// validQueueName validates an admin-supplied queue name.
func validQueueName(queue string) (string, error) {
	if queue == "" {
		return "", connect.NewError(connect.CodeInvalidArgument, errors.New("queue name is required"))
	}

	if !queueNamePattern.MatchString(queue) {
		return "", connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid queue name %q", queue))
	}

	return queue, nil
}

// requireTaskID rejects task operations that carry no id.
func requireTaskID(id string) error {
	if id == "" {
		return connect.NewError(connect.CodeInvalidArgument, errors.New("task id is required"))
	}

	return nil
}

// adminTaskError maps broker task-operation failures to API error codes.
func adminTaskError(err error) error {
	switch {
	case errors.Is(err, broker.ErrTaskNotFound):
		return connect.NewError(connect.CodeNotFound, err)

	case errors.Is(err, broker.ErrInvalidState):
		return connect.NewError(connect.CodeFailedPrecondition, err)

	default:
		return connect.NewError(connect.CodeInternal, err)
	}
}
