// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package actors

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
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

// stubGrainSystem embeds goakt.ActorSystem and overrides only the two methods
// wakeQueue calls, so its resolve-failure and tell-failure branches are
// deterministically reachable without a live cluster. The embedded interface is
// nil: any other method call would panic, which is the intended guard since
// wakeQueue must only touch these two.
type stubGrainSystem struct {
	goakt.ActorSystem

	// identityErr, when set, fails grain-identity resolution.
	identityErr error
	// tellErr, when set, fails the wake-up tell after a successful resolve.
	tellErr error
}

func (s stubGrainSystem) GrainIdentity(context.Context, string, goakt.GrainFactory, ...goakt.GrainOption) (*goakt.GrainIdentity, error) {
	if s.identityErr != nil {
		return nil, s.identityErr
	}

	return &goakt.GrainIdentity{}, nil
}

func (s stubGrainSystem) TellGrain(context.Context, *goakt.GrainIdentity, any) error {
	return s.tellErr
}

// TestWakeQueueLogsResolveAndTellFailures covers wakeQueue's two best-effort
// error branches: a failed grain-identity resolve and a failed wake-up tell are
// each logged and swallowed rather than propagated.
func TestWakeQueueLogsResolveAndTellFailures(t *testing.T) {
	ctx := context.Background()

	var logs bytes.Buffer

	taskLog := memory.New(clock.System())
	t.Cleanup(func() { _ = taskLog.Close() })

	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelWarn}))
	runtime := NewRuntime(taskLog, clock.System(), testSettings, logger)

	wakeQueue(ctx, stubGrainSystem{identityErr: errors.New("resolve down")}, runtime, "q", 0)
	wakeQueue(ctx, stubGrainSystem{tellErr: errors.New("tell down")}, runtime, "q", 0)

	output := logs.String()
	require.Contains(t, output, "resolving queue grain failed")
	require.Contains(t, output, "waking queue grain failed")
}

func TestSchedulerIgnoresUnknownMessage(t *testing.T) {
	requireUnhandled(t, spawnIsolated(t, "extra-scheduler", NewScheduler()), new(conveyorv1.ReapTick))
}

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

// TestSchedulerArmCronInvalidSpecIsSkipped covers arming an entry whose spec
// does not parse: the error is logged and no next-run time is persisted, so the
// entry never becomes due rather than crashing the promotion loop.
func TestSchedulerArmCronInvalidSpecIsSkipped(t *testing.T) {
	runtime := newTestRuntime(t)
	scheduler := &Scheduler{runtime: runtime}

	entry := &broker.CronEntry{ID: "cron-bad", Spec: "this is not a cron spec", Queue: "q"}

	scheduler.armCron(context.Background(), entry, runtime.Clock().Now())

	due, err := runtime.Broker().ListDueCronEntries(context.Background(), runtime.Clock().Now())
	require.NoError(t, err)
	require.Empty(t, due, "an unparseable spec must not be armed as due work")
}

// TestSchedulerArmCronPersistErrorLeavesEntryUnarmed covers the persistence
// failure inside armCron: when storing the computed first-fire time fails, the
// error is logged and the entry's next run stays zero, so the next tick retries
// arming rather than the entry being silently lost.
func TestSchedulerArmCronPersistErrorLeavesEntryUnarmed(t *testing.T) {
	ctx := context.Background()
	inner := memory.New(clock.System())
	t.Cleanup(func() { _ = inner.Close() })

	taskLog := newFaultBroker(inner)
	runtime := NewRuntime(taskLog, clock.System(), testSettings, quietLogger())
	scheduler := &Scheduler{runtime: runtime}

	entry := &broker.CronEntry{ID: "cron-persist", Spec: "* * * * * *", TaskType: "test:ok", Queue: "q"}
	require.NoError(t, inner.UpsertCronEntry(ctx, entry))

	taskLog.fault(methodUpdateCronNextRun, errors.New("persist down"))
	scheduler.armCron(ctx, entry, runtime.Clock().Now())

	stored, err := inner.ListCronEntries(ctx)
	require.NoError(t, err)
	require.Len(t, stored, 1)
	require.True(t, stored[0].NextRunAt.IsZero(), "a failed persist must leave the entry unarmed for a retry")
}
