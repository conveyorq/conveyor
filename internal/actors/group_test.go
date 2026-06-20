// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package actors

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/conveyorq/conveyor/internal/broker"
	"github.com/conveyorq/conveyor/internal/broker/memory"
	"github.com/conveyorq/conveyor/internal/clock"
	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
)

// groupedTask builds an aggregation-group member of the given type.
func groupedTask(id, queue, taskType, group string) *conveyorv1.TaskEnvelope {
	task := newTask(id, queue, taskType, 4)
	task.Options.Group = group

	return task
}

// batchDispatchFor returns the first BatchDispatch frame the recorder captured
// for the given group, or nil.
func batchDispatchFor(r *frameRecorder, group string) *conveyorv1.BatchDispatch {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	for _, frame := range r.frames {
		if batch := frame.GetBatchDispatch(); batch != nil && batch.GetGroup() == group {
			return batch
		}
	}

	return nil
}

// TestGroupDue covers the firing decision: a group fires on size, on max-delay
// since its first member, or on grace since its last, and not otherwise.
func TestGroupDue(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	settings := Settings{GroupMaxSize: 100, GroupMaxDelay: time.Minute, GroupGracePeriod: 10 * time.Second}

	require.True(t, groupDue(broker.GroupStat{Count: 100, Oldest: now, Newest: now}, settings, now),
		"size threshold")
	require.True(t, groupDue(broker.GroupStat{Count: 2, Oldest: now.Add(-time.Minute), Newest: now}, settings, now),
		"max-delay since first member")
	require.True(t, groupDue(broker.GroupStat{Count: 2, Oldest: now.Add(-30 * time.Second), Newest: now.Add(-10 * time.Second)}, settings, now),
		"grace since last member")
	require.False(t, groupDue(broker.GroupStat{Count: 2, Oldest: now.Add(-time.Second), Newest: now.Add(-time.Second)}, settings, now),
		"small and recent: not due")
}

// TestTightenDeadline covers the batch deadline computation: a member's own
// deadline and its per-attempt timeout each narrow the running batch deadline
// when they fall earlier, while a member with neither leaves it untouched.
func TestTightenDeadline(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	base := now.Add(time.Minute)

	// A member without bounds leaves the deadline as is.
	plain := newTask("plain", "q", "test:ok", 0)
	require.Equal(t, base, tightenDeadline(base, plain, now), "no bounds: deadline unchanged")

	// An earlier task deadline tightens the bound.
	deadlined := newTask("deadlined", "q", "test:ok", 0)
	deadlined.Options.Deadline = timestamppb.New(now.Add(10 * time.Second))
	require.Equal(t, now.Add(10*time.Second), tightenDeadline(base, deadlined, now),
		"an earlier task deadline wins")

	// A short per-attempt timeout tightens further still.
	timed := newTask("timed", "q", "test:ok", 0)
	timed.Options.Timeout = durationpb.New(2 * time.Second)
	require.Equal(t, now.Add(2*time.Second), tightenDeadline(base, timed, now),
		"now+timeout wins when it is the earliest")

	// A deadline later than the running bound does not loosen it.
	later := newTask("later", "q", "test:ok", 0)
	later.Options.Deadline = timestamppb.New(base.Add(time.Hour))
	require.Equal(t, base, tightenDeadline(base, later, now), "a later deadline never loosens the bound")
}

// TestQueueGrainBatchDispatchAndCapabilityGating drives a fired group end to
// end through the grain and gateway: a batch-capable gateway receives the
// group's members as one BatchDispatch and acknowledging them completes the
// tasks, while a group whose type the gateway does not advertise is never
// dispatched and stays aggregating.
func TestQueueGrainBatchDispatchAndCapabilityGating(t *testing.T) {
	const (
		queue       = "manual"
		capableType = "batch:cap"
		otherType   = "batch:nocap"
	)

	ctx := context.Background()
	taskLog := memory.New(clock.System())
	engine := startEngine(t, taskLog)
	recorder := newFrameRecorder()

	handle, err := engine.SpawnGateway(ctx, GatewaySession{
		SessionID:   "session-batch",
		Queues:      []string{queue},
		Concurrency: 4,
		BatchTypes:  []string{capableType},
	}, recorder)
	require.NoError(t, err)

	t.Cleanup(func() { _ = handle.Stop(ctx) })

	for _, id := range []string{"cap-001", "cap-002", "cap-003"} {
		require.NoError(t, taskLog.Enqueue(ctx, groupedTask(id, queue, capableType, "G-cap")))
	}

	for _, id := range []string{"nc-001", "nc-002"} {
		require.NoError(t, taskLog.Enqueue(ctx, groupedTask(id, queue, otherType, "G-nocap")))
	}

	// Gateway registration is asynchronous, so fire both groups until the
	// capable one is delivered; the incapable one must never be.
	require.Eventually(t, func() bool {
		fireGroup(ctx, engine.system, engine.runtime, queue, "G-cap", capableType)
		fireGroup(ctx, engine.system, engine.runtime, queue, "G-nocap", otherType)

		return batchDispatchFor(recorder, "G-cap") != nil
	}, 5*time.Second, 50*time.Millisecond)

	batch := batchDispatchFor(recorder, "G-cap")
	require.Len(t, batch.GetTasks(), 3, "the whole group is delivered as one batch")
	require.Nil(t, batchDispatchFor(recorder, "G-nocap"), "an unadvertised type is never batch-dispatched")

	// The incapable group's members stay aggregating.
	requireTaskState(t, engine, "nc-001", conveyorv1.TaskState_TASK_STATE_AGGREGATING)
	requireTaskState(t, engine, "nc-002", conveyorv1.TaskState_TASK_STATE_AGGREGATING)

	// The worker acknowledges the batch; every member completes.
	results := make([]*conveyorv1.Result, 0, len(batch.GetTasks()))
	for _, task := range batch.GetTasks() {
		results = append(results, &conveyorv1.Result{TaskId: task.GetId(), Outcome: conveyorv1.TaskOutcome_TASK_OUTCOME_SUCCESS})
	}

	require.NoError(t, handle.Tell(ctx, &conveyorv1.BatchResult{Results: results}))

	for _, id := range []string{"cap-001", "cap-002", "cap-003"} {
		requireTaskState(t, engine, id, conveyorv1.TaskState_TASK_STATE_COMPLETED)
	}
}

// TestGroupSweeperFiresDueGroup exercises the sweeper end to end: with a short
// grace period its tick reads GroupStats, finds the group due, and fires it —
// the worker receives the members as one BatchDispatch with no manual fire.
func TestGroupSweeperFiresDueGroup(t *testing.T) {
	const (
		queue    = "manual"
		taskType = "test:batch"
	)

	ctx := context.Background()
	taskLog := memory.New(clock.System())

	settings := testSettings
	settings.GroupGracePeriod = 50 * time.Millisecond
	settings.GroupSweepInterval = 20 * time.Millisecond

	engine := newNode(taskLog, settings, freePorts(t, 3), nil)
	require.NoError(t, engine.Start(ctx))

	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = engine.Stop(stopCtx)
	})

	recorder := newFrameRecorder()

	handle, err := engine.SpawnGateway(ctx, GatewaySession{
		SessionID:   "session-sweep",
		Queues:      []string{queue},
		Concurrency: 4,
		BatchTypes:  []string{taskType},
	}, recorder)
	require.NoError(t, err)

	t.Cleanup(func() { _ = handle.Stop(ctx) })

	for _, id := range []string{"s-001", "s-002"} {
		require.NoError(t, taskLog.Enqueue(ctx, groupedTask(id, queue, taskType, "G")))
	}

	require.Eventually(t, func() bool {
		batch := batchDispatchFor(recorder, "G")

		return batch != nil && len(batch.GetTasks()) == 2
	}, 5*time.Second, 50*time.Millisecond, "the sweeper fires the due group automatically")
}

// TestGatewayBatchDispatchAndResult drives the gateway's batch path directly
// (as TestGatewayOutcomeTransitions does for singles): an ExecuteBatch is
// delivered as one BatchDispatch, and a mixed BatchResult applies each member's
// durable transition — success acks, retry re-queues, and a member the worker
// omits is released without penalty.
func TestGatewayBatchDispatchAndResult(t *testing.T) {
	const (
		queue    = "manual"
		taskType = "test:batch"
		leaseID  = "batch-lease-1"
	)

	ctx := context.Background()
	taskLog := memory.New(clock.System())
	pauseQueue(t, taskLog, queue)
	engine := startEngine(t, taskLog)
	recorder := newFrameRecorder()

	handle, err := engine.SpawnGateway(ctx, GatewaySession{
		SessionID:   "session-batch-direct",
		Queues:      []string{queue},
		Concurrency: 4,
		BatchTypes:  []string{taskType},
	}, recorder)
	require.NoError(t, err)

	t.Cleanup(func() { _ = handle.Stop(ctx) })

	for _, id := range []string{"b-001", "b-002", "b-003"} {
		require.NoError(t, taskLog.Enqueue(ctx, groupedTask(id, queue, taskType, "G")))
	}

	batch, err := taskLog.LeaseGroup(ctx, queue, "G", 10, 30*time.Second, leaseID)
	require.NoError(t, err)
	require.Len(t, batch, 3)

	expiresAt := timestamppb.New(engine.runtime.Clock().Now().Add(30 * time.Second))
	require.NoError(t, handle.Tell(ctx, &conveyorv1.ExecuteBatch{
		Tasks: batch, LeaseId: leaseID, LeaseExpiresAt: expiresAt, Group: "G",
	}))

	require.Eventually(t, func() bool {
		frame := batchDispatchFor(recorder, "G")

		return frame != nil && len(frame.GetTasks()) == 3
	}, 2*time.Second, 20*time.Millisecond, "the batch is delivered as one BatchDispatch")

	// b-003 is omitted from the result: it is released, no retry penalty.
	require.NoError(t, handle.Tell(ctx, &conveyorv1.BatchResult{Results: []*conveyorv1.Result{
		{TaskId: "b-001", Outcome: conveyorv1.TaskOutcome_TASK_OUTCOME_SUCCESS},
		{TaskId: "b-002", Outcome: conveyorv1.TaskOutcome_TASK_OUTCOME_RETRY, ErrorMsg: "boom"},
	}}))

	requireTaskState(t, engine, "b-001", conveyorv1.TaskState_TASK_STATE_COMPLETED)
	requireTaskState(t, engine, "b-002", conveyorv1.TaskState_TASK_STATE_RETRY)
	requireTaskState(t, engine, "b-003", conveyorv1.TaskState_TASK_STATE_PENDING)
}

// TestGatewayBatchDispatchSendFailureReleases mirrors the single-task broken
// stream path for batches: when the worker stream is down, the whole leased
// group is released back to pending immediately rather than stranding its
// members as leased until the lease expires.
func TestGatewayBatchDispatchSendFailureReleases(t *testing.T) {
	const (
		queue    = "manual-batch-broken"
		taskType = "test:batch"
		leaseID  = "batch-lease-broken"
	)

	ctx := context.Background()
	taskLog := memory.New(clock.System())
	pauseQueue(t, taskLog, queue)
	engine := startEngine(t, taskLog)
	recorder := newFrameRecorder()

	handle, err := engine.SpawnGateway(ctx, GatewaySession{
		SessionID:   "session-batch-broken",
		Queues:      []string{queue},
		Concurrency: 4,
		BatchTypes:  []string{taskType},
	}, recorder)
	require.NoError(t, err)

	t.Cleanup(func() { _ = handle.Stop(ctx) })

	recorder.fail(errors.New("stream torn down"))

	for _, id := range []string{"bb-001", "bb-002"} {
		require.NoError(t, taskLog.Enqueue(ctx, groupedTask(id, queue, taskType, "G")))
	}

	batch, err := taskLog.LeaseGroup(ctx, queue, "G", 10, 30*time.Second, leaseID)
	require.NoError(t, err)
	require.Len(t, batch, 2)

	expiresAt := timestamppb.New(engine.runtime.Clock().Now().Add(30 * time.Second))
	require.NoError(t, handle.Tell(ctx, &conveyorv1.ExecuteBatch{
		Tasks: batch, LeaseId: leaseID, LeaseExpiresAt: expiresAt, Group: "G",
	}))

	for _, id := range []string{"bb-001", "bb-002"} {
		requireTaskState(t, engine, id, conveyorv1.TaskState_TASK_STATE_PENDING)
	}
}

// TestGatewayBatchReleaseBrokerError covers the release-failure branch of
// releaseBatch: when both the worker stream is down and the broker cannot
// return the batch to pending, the members stay leased until the reaper
// reclaims them on lease expiry, rather than the gateway failing.
func TestGatewayBatchReleaseBrokerError(t *testing.T) {
	const (
		queue    = "manual-batch-release-fault"
		taskType = "test:batch"
		leaseID  = "batch-lease-release-fault"
	)

	ctx := context.Background()
	taskLog := newFaultBroker(memory.New(clock.System()))
	pauseQueue(t, taskLog, queue)
	engine := startEngine(t, taskLog)
	recorder := newFrameRecorder()

	handle, err := engine.SpawnGateway(ctx, GatewaySession{
		SessionID:   "session-batch-release-fault",
		Queues:      []string{queue},
		Concurrency: 4,
		BatchTypes:  []string{taskType},
	}, recorder)
	require.NoError(t, err)

	t.Cleanup(func() { _ = handle.Stop(ctx) })

	for _, id := range []string{"rf-001", "rf-002"} {
		require.NoError(t, taskLog.Enqueue(ctx, groupedTask(id, queue, taskType, "G")))
	}

	batch, err := taskLog.LeaseGroup(ctx, queue, "G", 10, 30*time.Second, leaseID)
	require.NoError(t, err)
	require.Len(t, batch, 2)

	// The stream is down so the batch is undeliverable, and the release that
	// would return it to pending also fails.
	recorder.fail(errors.New("stream torn down"))
	taskLog.fault(methodRelease, errors.New("release down"))

	expiresAt := timestamppb.New(engine.runtime.Clock().Now().Add(30 * time.Second))
	require.NoError(t, handle.Tell(ctx, &conveyorv1.ExecuteBatch{
		Tasks: batch, LeaseId: leaseID, LeaseExpiresAt: expiresAt, Group: "G",
	}))

	time.Sleep(500 * time.Millisecond)

	for _, id := range []string{"rf-001", "rf-002"} {
		_, state, err := taskLog.GetTask(ctx, id)
		require.NoError(t, err)
		require.Equal(t, conveyorv1.TaskState_TASK_STATE_ACTIVE, state,
			"a batch whose release failed stays leased until the reaper reclaims it")
	}
}
