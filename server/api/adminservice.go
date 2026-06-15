// MIT License
//
// Copyright (c) 2026 ConveyorQ
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package api

import (
	"context"
	"errors"
	"fmt"
	"time"

	"connectrpc.com/connect"
	"github.com/reugn/go-quartz/quartz"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/conveyorq/conveyor/internal/actors"
	"github.com/conveyorq/conveyor/internal/broker"
	"github.com/conveyorq/conveyor/internal/clock"
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
			Name:      stat.Queue,
			Paused:    stat.Paused,
			Scheduled: stat.Scheduled,
			Pending:   stat.Pending,
			Active:    stat.Active,
			Retry:     stat.Retry,
			Completed: stat.Completed,
			Archived:  stat.Archived,
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

// RunTask makes a scheduled, pending, or retry task due immediately and
// wakes its queue grain.
func (s *AdminService) RunTask(ctx context.Context, request *connect.Request[conveyorv1.RunTaskRequest]) (*connect.Response[conveyorv1.RunTaskResponse], error) {
	if err := requireTaskID(request.Msg.GetId()); err != nil {
		return nil, err
	}

	envelope, _, err := s.taskLog.GetTask(ctx, request.Msg.GetId())
	if err != nil {
		return nil, adminTaskError(err)
	}

	if err := s.taskLog.RunTaskNow(ctx, request.Msg.GetId()); err != nil {
		return nil, adminTaskError(err)
	}

	// The wake-up is a best-effort hint; the reaper sweep recovers lost
	// ones.
	queue := envelope.GetQueue()
	_ = s.engine.TellQueue(ctx, queue, &conveyorv1.TaskEnqueued{Queue: queue})

	return connect.NewResponse(&conveyorv1.RunTaskResponse{}), nil
}

// ListCron returns all persisted cron entries ordered by id.
func (s *AdminService) ListCron(ctx context.Context, _ *connect.Request[conveyorv1.ListCronRequest]) (*connect.Response[conveyorv1.ListCronResponse], error) {
	entries, err := s.taskLog.ListCronEntries(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	infos := make([]*conveyorv1.CronEntry, 0, len(entries))

	for _, entry := range entries {
		infos = append(infos, &conveyorv1.CronEntry{
			Id:          entry.ID,
			Spec:        entry.Spec,
			TaskType:    entry.TaskType,
			Queue:       entry.Queue,
			Payload:     entry.Payload,
			ContentType: entry.ContentType,
			Options:     entry.Options,
			Paused:      entry.Paused,
		})
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
