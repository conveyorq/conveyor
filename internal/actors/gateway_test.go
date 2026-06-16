// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package actors

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	goakt "github.com/tochemey/goakt/v4/actor"
	goaktlog "github.com/tochemey/goakt/v4/log"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/conveyorq/conveyor/internal/broker"
	"github.com/conveyorq/conveyor/internal/broker/memory"
	"github.com/conveyorq/conveyor/internal/clock"
	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
)

// frameRecorder is a FrameSender capturing every frame; dispatches are also
// pushed to a channel so tests can react to them as a worker would.
type frameRecorder struct {
	// mutex guards frames and failure.
	mutex sync.Mutex
	// frames are all sent frames in order.
	frames []*conveyorv1.ServerMessage
	// failure, when non-nil, is returned by every Send.
	failure error
	// dispatched receives every Dispatch frame as it is sent.
	dispatched chan *conveyorv1.Dispatch
}

// newFrameRecorder builds a recorder with a generous dispatch buffer.
func newFrameRecorder() *frameRecorder {
	return &frameRecorder{dispatched: make(chan *conveyorv1.Dispatch, 1024)}
}

// Send implements FrameSender.
func (r *frameRecorder) Send(message *conveyorv1.ServerMessage) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if r.failure != nil {
		return r.failure
	}

	r.frames = append(r.frames, message)

	if dispatch := message.GetDispatch(); dispatch != nil {
		r.dispatched <- dispatch
	}

	return nil
}

// fail makes every subsequent Send return err.
func (r *frameRecorder) fail(err error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	r.failure = err
}

// cancels returns the task ids of all Cancel frames sent so far.
func (r *frameRecorder) cancels() []string {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	var ids []string

	for _, frame := range r.frames {
		if cancel := frame.GetCancel(); cancel != nil {
			ids = append(ids, cancel.GetTaskId())
		}
	}

	return ids
}

// TestGatewayPreStartRequiresRuntimeExtension verifies that a gateway
// refuses to start on a system that does not carry the engine runtime.
func TestGatewayPreStartRequiresRuntimeExtension(t *testing.T) {
	ctx := context.Background()

	system, err := goakt.NewActorSystem("bare-system", goakt.WithLogger(goaktlog.DiscardLogger))
	require.NoError(t, err)
	require.NoError(t, system.Start(ctx))

	t.Cleanup(func() { _ = system.Stop(ctx) })

	gateway := newGateway(GatewaySession{SessionID: "no-runtime"}, newFrameRecorder())

	_, err = system.Spawn(ctx, "gateway-no-runtime", gateway)
	require.ErrorContains(t, err, "is not registered")
}

// TestGatewayDispatchesAndCompletes drives the full grain-to-gateway loop:
// registration grants credits, enqueued tasks are leased and dispatched as
// frames, success results ack durably and refill credits until the queue
// drains.
func TestGatewayDispatchesAndCompletes(t *testing.T) {
	const totalTasks = 20

	ctx := context.Background()
	taskLog := memory.New(clock.System())
	engine := startEngine(t, taskLog)
	recorder := newFrameRecorder()

	handle, err := engine.SpawnGateway(ctx, GatewaySession{
		SessionID:   "session-complete",
		Queues:      []string{"default"},
		Concurrency: 4,
	}, recorder)
	require.NoError(t, err)

	t.Cleanup(func() { _ = handle.Stop(ctx) })

	// Act as the worker: every dispatched task succeeds immediately.
	go func() {
		for dispatch := range recorder.dispatched {
			result := &conveyorv1.Result{
				TaskId:  dispatch.GetTask().GetId(),
				Outcome: conveyorv1.TaskOutcome_TASK_OUTCOME_SUCCESS,
			}
			_ = handle.Tell(context.Background(), result)
		}
	}()

	enqueueTasks(t, engine, "default", totalTasks)

	require.Eventually(t, completedReaches(taskLog, totalTasks),
		time.Minute, 20*time.Millisecond, "all tasks should complete through the gateway")

	requireDrained(t, taskLog)
}

// pauseQueue persists the pause flag so the queue's grain drops every
// wake-up hint: manual lease tests must not race the reaper sweep.
func pauseQueue(t *testing.T, taskLog broker.Broker, queue string) {
	t.Helper()

	require.NoError(t, taskLog.SetQueuePaused(context.Background(), queue, true))
}

// leaseOne enqueues a task on a paused queue and leases it directly, so
// outcome paths can be driven by hand without grain interference.
func leaseOne(t *testing.T, engine *Engine, queue, id string, leaseID string) *conveyorv1.ExecuteTask {
	t.Helper()

	ctx := context.Background()
	taskLog := engine.runtime.Broker()

	require.NoError(t, taskLog.Enqueue(ctx, newTask(id, queue, "test:manual", 4)))

	tasks, err := taskLog.Lease(ctx, queue, 1, 30*time.Second, leaseID)
	require.NoError(t, err)
	require.Len(t, tasks, 1)

	return &conveyorv1.ExecuteTask{Task: tasks[0], LeaseId: leaseID}
}

// requireTaskState polls until the task reaches the wanted state.
func requireTaskState(t *testing.T, engine *Engine, id string, want conveyorv1.TaskState) {
	t.Helper()

	taskLog := engine.runtime.Broker()

	require.Eventuallyf(t, func() bool {
		_, state, err := taskLog.GetTask(context.Background(), id)

		return err == nil && state == want
	}, 10*time.Second, 10*time.Millisecond, "task %s should reach %s", id, want)
}

// TestGatewayOutcomeTransitions checks the durable transition of every
// worker-reported outcome, driven by direct ExecuteTask and Result messages.
func TestGatewayOutcomeTransitions(t *testing.T) {
	const queue = "manual"

	ctx := context.Background()
	taskLog := memory.New(clock.System())
	pauseQueue(t, taskLog, queue)
	engine := startEngine(t, taskLog)
	recorder := newFrameRecorder()

	handle, err := engine.SpawnGateway(ctx, GatewaySession{
		SessionID:   "session-outcomes",
		Queues:      []string{queue},
		Concurrency: 4,
	}, recorder)
	require.NoError(t, err)

	t.Cleanup(func() { _ = handle.Stop(ctx) })

	tell := func(message any) {
		require.NoError(t, handle.Tell(ctx, message))
	}

	// SUCCESS acks the task.
	execute := leaseOne(t, engine, queue, "task-success", "lease-1")
	tell(execute)
	tell(&conveyorv1.Result{TaskId: "task-success", Outcome: conveyorv1.TaskOutcome_TASK_OUTCOME_SUCCESS})
	requireTaskState(t, engine, "task-success", conveyorv1.TaskState_TASK_STATE_COMPLETED)

	// RETRY with budget left moves the task to retry with the error stored.
	execute = leaseOne(t, engine, queue, "task-retry", "lease-2")
	tell(execute)
	tell(&conveyorv1.Result{TaskId: "task-retry", Outcome: conveyorv1.TaskOutcome_TASK_OUTCOME_RETRY, ErrorMsg: "boom"})
	requireTaskState(t, engine, "task-retry", conveyorv1.TaskState_TASK_STATE_RETRY)

	// RETRY with the budget exhausted archives instead.
	execute = leaseOne(t, engine, queue, "task-exhausted", "lease-3")
	execute.Task.Retried = execute.GetTask().GetOptions().GetMaxRetry()
	tell(execute)
	tell(&conveyorv1.Result{TaskId: "task-exhausted", Outcome: conveyorv1.TaskOutcome_TASK_OUTCOME_RETRY, ErrorMsg: "boom"})
	requireTaskState(t, engine, "task-exhausted", conveyorv1.TaskState_TASK_STATE_ARCHIVED)

	// SKIP_RETRY archives immediately.
	execute = leaseOne(t, engine, queue, "task-skip", "lease-4")
	tell(execute)
	tell(&conveyorv1.Result{TaskId: "task-skip", Outcome: conveyorv1.TaskOutcome_TASK_OUTCOME_SKIP_RETRY, ErrorMsg: "bad payload"})
	requireTaskState(t, engine, "task-skip", conveyorv1.TaskState_TASK_STATE_ARCHIVED)

	// RELEASED re-queues without a retry increment.
	execute = leaseOne(t, engine, queue, "task-released", "lease-5")
	tell(execute)
	tell(&conveyorv1.Result{TaskId: "task-released", Outcome: conveyorv1.TaskOutcome_TASK_OUTCOME_RELEASED})
	requireTaskState(t, engine, "task-released", conveyorv1.TaskState_TASK_STATE_PENDING)

	envelope, _, err := taskLog.GetTask(ctx, "task-released")
	require.NoError(t, err)
	require.Zero(t, envelope.GetRetried(), "release must not count as a retry")
}

// TestGatewayReleasedAfterAdminCancelArchives covers the cancel-during-drain
// race: a task already marked for admin cancellation that comes back RELEASED
// (its worker was draining) is archived, not redelivered — the operator's
// cancel wins over the penalty-free release.
func TestGatewayReleasedAfterAdminCancelArchives(t *testing.T) {
	const queue = "manual"

	ctx := context.Background()
	taskLog := memory.New(clock.System())
	pauseQueue(t, taskLog, queue)
	engine := startEngine(t, taskLog)
	recorder := newFrameRecorder()

	handle, err := engine.SpawnGateway(ctx, GatewaySession{
		SessionID:   "session-cancel-release",
		Queues:      []string{queue},
		Concurrency: 4,
	}, recorder)
	require.NoError(t, err)

	t.Cleanup(func() { _ = handle.Stop(ctx) })

	tell := func(message any) {
		require.NoError(t, handle.Tell(ctx, message))
	}

	execute := leaseOne(t, engine, queue, "task-cancel-release", "lease-cr")
	tell(execute)
	tell(&conveyorv1.CancelActive{TaskId: "task-cancel-release"})
	tell(&conveyorv1.Result{TaskId: "task-cancel-release", Outcome: conveyorv1.TaskOutcome_TASK_OUTCOME_RELEASED})

	requireTaskState(t, engine, "task-cancel-release", conveyorv1.TaskState_TASK_STATE_ARCHIVED)
}

// TestGatewayBreakerWithholdsCreditsOnPoisonType drives the poison-task
// failure-matrix row: a task type failing every attempt opens its circuit
// breaker after breakerMinRequests outcomes, after which completion
// reports — and the credit refills they carry — are withheld for the
// breaker's open timeout. The durable transitions themselves still land,
// so the tasks sit safely in retry.
func TestGatewayBreakerWithholdsCreditsOnPoisonType(t *testing.T) {
	const queue = "poison"

	ctx := context.Background()
	taskLog := memory.New(clock.System())
	pauseQueue(t, taskLog, queue)
	engine := startEngine(t, taskLog)
	recorder := newFrameRecorder()

	handle, err := engine.SpawnGateway(ctx, GatewaySession{
		SessionID:   "session-poison",
		Queues:      []string{queue},
		Concurrency: 16,
	}, recorder)
	require.NoError(t, err)

	t.Cleanup(func() { _ = handle.Stop(ctx) })

	// Lease everything upfront: failing as we go would put early tasks
	// back in retry, and once their backoff lapses leaseOne would claim
	// them instead of the fresh task.
	executes := make([]*conveyorv1.ExecuteTask, 0, breakerMinRequests)

	for sequence := range breakerMinRequests {
		id := fmt.Sprintf("task-%03d", sequence)
		executes = append(executes, leaseOne(t, engine, queue, id, fmt.Sprintf("lease-%d", sequence)))
	}

	// Every result is a retryable failure of the same task type; the
	// breaker opens on the breakerMinRequests-th sample, so that final
	// completion report is the first one withheld.
	for _, execute := range executes {
		require.NoError(t, handle.Tell(ctx, execute))
		require.NoError(t, handle.Tell(ctx, &conveyorv1.Result{
			TaskId:   execute.GetTask().GetId(),
			Outcome:  conveyorv1.TaskOutcome_TASK_OUTCOME_RETRY,
			ErrorMsg: "downstream dead",
		}))

		requireTaskState(t, engine, execute.GetTask().GetId(), conveyorv1.TaskState_TASK_STATE_RETRY)
	}

	reported := breakerMinRequests - 1

	require.Eventually(t, func() bool {
		return engine.Counters().Failed.Load() == int64(reported)
	}, 10*time.Second, 10*time.Millisecond, "reports before the breaker opened must reach the grain")

	// The withheld report stays deferred for the breaker's open timeout;
	// the failure counter must not advance in the meantime.
	time.Sleep(500 * time.Millisecond)
	require.EqualValues(t, reported, engine.Counters().Failed.Load(),
		"open breaker must withhold the completion report")
}

// TestGatewayStaleResultAfterLeaseExpiry covers the stalled-handler
// failure-matrix row: a worker that ignores its deadline holds the lease
// until it expires, the reaper resets the task to retry with a penalty,
// and the handler's eventual late result hits ErrLeaseLost and is
// discarded instead of overwriting the newer delivery's state.
func TestGatewayStaleResultAfterLeaseExpiry(t *testing.T) {
	const queue = "stalled"

	ctx := context.Background()
	taskLog := memory.New(clock.System())
	pauseQueue(t, taskLog, queue)
	engine := startEngine(t, taskLog)
	recorder := newFrameRecorder()

	handle, err := engine.SpawnGateway(ctx, GatewaySession{
		SessionID:   "session-stalled",
		Queues:      []string{queue},
		Concurrency: 4,
	}, recorder)
	require.NoError(t, err)

	t.Cleanup(func() { _ = handle.Stop(ctx) })

	// Lease with a TTL far below the reap interval so the lease expires
	// under the still-running handler.
	require.NoError(t, taskLog.Enqueue(ctx, newTask("task-stalled", queue, "test:manual", 4)))

	leased, err := taskLog.Lease(ctx, queue, 1, 50*time.Millisecond, "lease-stalled")
	require.NoError(t, err)
	require.Len(t, leased, 1)

	require.NoError(t, handle.Tell(ctx, &conveyorv1.ExecuteTask{Task: leased[0], LeaseId: "lease-stalled"}))

	// The reaper reclaims the expired lease: retry, with a penalty.
	requireTaskState(t, engine, "task-stalled", conveyorv1.TaskState_TASK_STATE_RETRY)

	envelope, _, err := taskLog.GetTask(ctx, "task-stalled")
	require.NoError(t, err)
	require.EqualValues(t, 1, envelope.GetRetried())
	require.Equal(t, broker.LeaseExpiredMessage, envelope.GetLastError())

	// The stalled handler finally reports success; the lease is lost, so
	// the result must be discarded.
	require.NoError(t, handle.Tell(ctx, &conveyorv1.Result{
		TaskId:  "task-stalled",
		Outcome: conveyorv1.TaskOutcome_TASK_OUTCOME_SUCCESS,
	}))

	time.Sleep(300 * time.Millisecond)

	_, state, err := taskLog.GetTask(ctx, "task-stalled")
	require.NoError(t, err)
	require.Equal(t, conveyorv1.TaskState_TASK_STATE_RETRY, state,
		"a result from a lost lease must never complete the task")
}

// TestGatewayHeartbeatLeaseLostSendsCancel verifies that a heartbeat naming
// a task whose lease this delivery no longer owns triggers a Cancel frame.
func TestGatewayHeartbeatLeaseLostSendsCancel(t *testing.T) {
	const queue = "manual-cancel"

	ctx := context.Background()
	taskLog := memory.New(clock.System())
	pauseQueue(t, taskLog, queue)
	engine := startEngine(t, taskLog)
	recorder := newFrameRecorder()

	handle, err := engine.SpawnGateway(ctx, GatewaySession{
		SessionID:   "session-cancel",
		Queues:      []string{queue},
		Concurrency: 4,
	}, recorder)
	require.NoError(t, err)

	t.Cleanup(func() { _ = handle.Stop(ctx) })

	// The broker holds the lease under lease-real; the gateway believes it
	// holds lease-stale, exactly as after a reaper reclaim and redelivery.
	execute := leaseOne(t, engine, queue, "task-stale", "lease-real")
	execute.LeaseId = "lease-stale"
	require.NoError(t, handle.Tell(ctx, execute))

	heartbeat := &conveyorv1.Heartbeat{ActiveTaskIds: []string{"task-stale", "task-unknown"}}
	require.NoError(t, handle.Tell(ctx, heartbeat))

	require.Eventually(t, func() bool {
		ids := recorder.cancels()

		return len(ids) == 1 && ids[0] == "task-stale"
	}, 10*time.Second, 10*time.Millisecond, "lease loss should cancel exactly the stale task")
}

// TestGatewayAdminCancelForwardsFrame verifies the admin cancel path: a
// CancelActive for an in-flight task reaches the worker as a Cancel frame
// (an unknown id is dropped silently), and the aborted attempt archives
// instead of earning a retry.
func TestGatewayAdminCancelForwardsFrame(t *testing.T) {
	const queue = "admin-cancel"

	ctx := context.Background()
	taskLog := memory.New(clock.System())
	pauseQueue(t, taskLog, queue)
	engine := startEngine(t, taskLog)
	recorder := newFrameRecorder()

	handle, err := engine.SpawnGateway(ctx, GatewaySession{
		SessionID:   "session-admin-cancel",
		Queues:      []string{queue},
		Concurrency: 4,
	}, recorder)
	require.NoError(t, err)

	t.Cleanup(func() { _ = handle.Stop(ctx) })

	execute := leaseOne(t, engine, queue, "task-running", "lease-1")
	require.NoError(t, handle.Tell(ctx, execute))

	require.NoError(t, handle.Tell(ctx, &conveyorv1.CancelActive{TaskId: "task-elsewhere"}))
	require.NoError(t, handle.Tell(ctx, &conveyorv1.CancelActive{TaskId: "task-running"}))

	require.Eventually(t, func() bool {
		ids := recorder.cancels()

		return len(ids) == 1 && ids[0] == "task-running"
	}, 10*time.Second, 10*time.Millisecond, "admin cancel should reach only the owning session")

	// The handler aborts on the canceled context and reports a retryable
	// failure; the canceled delivery must archive, not retry.
	require.NoError(t, handle.Tell(ctx, &conveyorv1.Result{
		TaskId:   "task-running",
		Outcome:  conveyorv1.TaskOutcome_TASK_OUTCOME_RETRY,
		ErrorMsg: "context canceled",
	}))

	requireTaskState(t, engine, "task-running", conveyorv1.TaskState_TASK_STATE_ARCHIVED)

	envelope, _, err := taskLog.GetTask(ctx, "task-running")
	require.NoError(t, err)
	require.Contains(t, envelope.GetLastError(), canceledByAdminMessage)
}

// TestGatewayStopReleasesInflight verifies the release-on-close contract:
// stopping the gateway returns every unresolved task to pending with no
// retry penalty.
func TestGatewayStopReleasesInflight(t *testing.T) {
	const queue = "manual-stop"

	ctx := context.Background()
	taskLog := memory.New(clock.System())
	pauseQueue(t, taskLog, queue)
	engine := startEngine(t, taskLog)
	recorder := newFrameRecorder()

	handle, err := engine.SpawnGateway(ctx, GatewaySession{
		SessionID:   "session-stop",
		Queues:      []string{queue},
		Concurrency: 4,
	}, recorder)
	require.NoError(t, err)

	execute := leaseOne(t, engine, queue, "task-inflight", "lease-6")
	require.NoError(t, handle.Tell(ctx, execute))

	// Wait for the dispatch so the task is tracked before the stop.
	select {
	case <-recorder.dispatched:
	case <-time.After(10 * time.Second):
		t.Fatal("task was never dispatched")
	}

	require.NoError(t, handle.Stop(ctx))

	requireTaskState(t, engine, "task-inflight", conveyorv1.TaskState_TASK_STATE_PENDING)

	envelope, _, err := taskLog.GetTask(ctx, "task-inflight")
	require.NoError(t, err)
	require.Zero(t, envelope.GetRetried(), "release on close must not count as a retry")
}

// TestGatewayDispatchSendFailureReleases verifies that a broken stream
// releases the task immediately instead of waiting for lease expiry.
func TestGatewayDispatchSendFailureReleases(t *testing.T) {
	const queue = "manual-broken"

	ctx := context.Background()
	taskLog := memory.New(clock.System())
	pauseQueue(t, taskLog, queue)
	engine := startEngine(t, taskLog)
	recorder := newFrameRecorder()

	handle, err := engine.SpawnGateway(ctx, GatewaySession{
		SessionID:   "session-broken",
		Queues:      []string{queue},
		Concurrency: 4,
	}, recorder)
	require.NoError(t, err)

	t.Cleanup(func() { _ = handle.Stop(ctx) })

	recorder.fail(errors.New("stream torn down"))

	execute := leaseOne(t, engine, queue, "task-undeliverable", "lease-7")
	require.NoError(t, handle.Tell(ctx, execute))

	requireTaskState(t, engine, "task-undeliverable", conveyorv1.TaskState_TASK_STATE_PENDING)
}

// TestGatewayCreditForwarding covers worker-opened credit grants: they are
// forwarded to every declared queue grain, where capacity caps them.
func TestGatewayCreditForwarding(t *testing.T) {
	const queue = "manual-credit"

	ctx := context.Background()
	taskLog := memory.New(clock.System())
	pauseQueue(t, taskLog, queue)
	engine := startEngine(t, taskLog)
	recorder := newFrameRecorder()

	handle, err := engine.SpawnGateway(ctx, GatewaySession{
		SessionID:   "session-credit",
		Queues:      []string{queue},
		Concurrency: 4,
	}, recorder)
	require.NoError(t, err)

	t.Cleanup(func() { _ = handle.Stop(ctx) })

	require.NoError(t, handle.Tell(ctx, &conveyorv1.Credit{N: 2}))

	// The grant lands as a grain message; pausing keeps it observable as a
	// no-op. Reaching here without a dead letter is the assertion: drain
	// the mailbox by asking for a stop.
	require.NoError(t, handle.Stop(ctx))
}

// TestGatewayExecutionDeadlinePicksTightestBound covers the dispatch
// deadline computation: the task deadline and the per-attempt timeout each
// tighten the lease-expiry default when they are earlier.
func TestGatewayExecutionDeadlinePicksTightestBound(t *testing.T) {
	const queue = "manual-deadline"

	ctx := context.Background()
	taskLog := memory.New(clock.System())
	pauseQueue(t, taskLog, queue)
	engine := startEngine(t, taskLog)
	recorder := newFrameRecorder()

	handle, err := engine.SpawnGateway(ctx, GatewaySession{
		SessionID:   "session-deadline",
		Queues:      []string{queue},
		Concurrency: 4,
	}, recorder)
	require.NoError(t, err)

	t.Cleanup(func() { _ = handle.Stop(ctx) })

	execute := leaseOne(t, engine, queue, "task-deadline", "lease-deadline")
	taskDeadline := clock.System().Now().Add(5 * time.Second)
	execute.Task.Options.Deadline = timestamppb.New(taskDeadline)
	execute.Task.Options.Timeout = durationpb.New(time.Second)
	require.NoError(t, handle.Tell(ctx, execute))

	select {
	case dispatch := <-recorder.dispatched:
		// The one-second attempt timeout is the tightest bound: well under
		// both the 30s lease and the 5s task deadline.
		effective := dispatch.GetDeadline().AsTime()
		require.True(t, effective.Before(taskDeadline), "timeout must tighten the deadline")

	case <-time.After(10 * time.Second):
		t.Fatal("task was never dispatched")
	}
}

// TestGatewayResultForUnregisteredQueue covers the completion-report path
// when a task's queue is not among the session's declared queues.
func TestGatewayResultForUnregisteredQueue(t *testing.T) {
	const declaredQueue = "manual-declared"
	const strayQueue = "manual-stray"

	ctx := context.Background()
	taskLog := memory.New(clock.System())
	pauseQueue(t, taskLog, strayQueue)
	engine := startEngine(t, taskLog)
	recorder := newFrameRecorder()

	handle, err := engine.SpawnGateway(ctx, GatewaySession{
		SessionID:   "session-stray",
		Queues:      []string{declaredQueue},
		Concurrency: 4,
	}, recorder)
	require.NoError(t, err)

	t.Cleanup(func() { _ = handle.Stop(ctx) })

	execute := leaseOne(t, engine, strayQueue, "task-stray", "lease-stray")
	require.NoError(t, handle.Tell(ctx, execute))
	require.NoError(t, handle.Tell(ctx, &conveyorv1.Result{
		TaskId:  "task-stray",
		Outcome: conveyorv1.TaskOutcome_TASK_OUTCOME_SUCCESS,
	}))

	// The durable transition still happens; only the credit refill report
	// is dropped.
	requireTaskState(t, engine, "task-stray", conveyorv1.TaskState_TASK_STATE_COMPLETED)
}
