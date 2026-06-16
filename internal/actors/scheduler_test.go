// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package actors

import (
	"context"
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

func TestSchedulerPreStartRequiresRuntimeExtension(t *testing.T) {
	ctx := context.Background()

	system, err := goakt.NewActorSystem("bare-scheduler-system", goakt.WithLogger(goaktlog.DiscardLogger))
	require.NoError(t, err)
	require.NoError(t, system.Start(ctx))

	t.Cleanup(func() { _ = system.Stop(ctx) })

	_, err = system.Spawn(ctx, "scheduler-no-runtime", NewScheduler())
	require.ErrorContains(t, err, "is not registered")
}

// TestSchedulerPromotesScheduledTasks verifies the promotion loop: a task
// scheduled slightly in the future becomes pending once due. The queue is
// paused so no grain leases it away from the assertion.
func TestSchedulerPromotesScheduledTasks(t *testing.T) {
	const queue = "promotion"

	ctx := context.Background()
	taskLog := memory.New(clock.System())
	pauseQueue(t, taskLog, queue)
	engine := startEngine(t, taskLog)

	task := newTask("task-scheduled", queue, "test:later", 4)
	task.Options.ProcessAt = timestamppb.New(clock.System().Now().Add(300 * time.Millisecond))
	require.NoError(t, taskLog.Enqueue(ctx, task))

	_, state, err := taskLog.GetTask(ctx, "task-scheduled")
	require.NoError(t, err)
	require.Equal(t, conveyorv1.TaskState_TASK_STATE_SCHEDULED, state)

	requireTaskState(t, engine, "task-scheduled", conveyorv1.TaskState_TASK_STATE_PENDING)
}

// TestSchedulerFiresCronOnSchedule is the Phase 6 cron acceptance test: an
// entry with a one-second spec materializes a fresh task each second, which a
// worker gateway then completes. Seeing several completions proves the
// cluster-singleton scheduler fires on cadence — not just once.
func TestSchedulerFiresCronOnSchedule(t *testing.T) {
	const cronQueue = "cronq"

	taskLog := memory.New(clock.System())
	engine := startEngine(t, taskLog)

	spawnGateway(t, engine, &mockGateway{queue: cronQueue, capacity: 20})

	entry := &broker.CronEntry{
		ID:          "every-second",
		Spec:        "* * * * * *",
		TaskType:    "test:ok",
		Queue:       cronQueue,
		Payload:     []byte(`{}`),
		ContentType: "application/json",
		// Retain completed tasks so each fire's completion accumulates rather
		// than being purged the instant it finishes.
		Options: &conveyorv1.TaskOptions{MaxRetry: 1, Retention: durationpb.New(time.Hour)},
	}
	require.NoError(t, taskLog.UpsertCronEntry(context.Background(), entry))

	// A new entry is armed without firing, then fires once per second; three
	// completions within the window confirm recurring materialization.
	require.Eventually(t, completedReaches(taskLog, 3),
		10*time.Second, 50*time.Millisecond, "cron entry must fire repeatedly on its one-second cadence")
}

// TestSchedulerSkipsPausedCron verifies a paused entry never materializes.
func TestSchedulerSkipsPausedCron(t *testing.T) {
	const cronQueue = "pausedq"

	taskLog := memory.New(clock.System())
	engine := startEngine(t, taskLog)

	spawnGateway(t, engine, &mockGateway{queue: cronQueue, capacity: 20})

	entry := &broker.CronEntry{
		ID:       "paused",
		Spec:     "* * * * * *",
		TaskType: "test:ok",
		Queue:    cronQueue,
		Paused:   true,
	}
	require.NoError(t, taskLog.UpsertCronEntry(context.Background(), entry))

	// Give the scheduler several ticks; nothing should be enqueued or run.
	time.Sleep(2 * time.Second)

	count, err := taskLog.PendingCount(context.Background())
	require.NoError(t, err)
	require.Zero(t, count[cronQueue], "paused cron entry must not materialize tasks")
}
