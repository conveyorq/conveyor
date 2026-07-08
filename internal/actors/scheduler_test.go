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

// tellFailSystem embeds a live actor system so GrainOf resolves the queue grain
// through the real grain engine, then forces the wake-up tell to fail. It
// isolates wakeQueue's tell-failure branch, which a healthy system never reaches
// on its own.
type tellFailSystem struct {
	goakt.ActorSystem

	// err is returned by every TellGrain call.
	err error
}

// TellGrain fails deterministically so wakeQueue's best-effort tell branch runs.
func (s tellFailSystem) TellGrain(context.Context, *goakt.GrainIdentity, any) error {
	return s.err
}

// newBufferRuntime builds a runtime whose logger writes to the returned buffer,
// so wakeQueue's swallowed warnings can be asserted on.
func newBufferRuntime(t *testing.T) (*bytes.Buffer, *Runtime) {
	t.Helper()

	taskLog := memory.New(clock.System())
	t.Cleanup(func() { _ = taskLog.Close() })

	logs := new(bytes.Buffer)
	logger := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelWarn}))

	return logs, NewRuntime(taskLog, clock.System(), testSettings, logger)
}

// startWakeSystem starts a live single-node actor system carrying the runtime
// extension, so GrainOf can activate the queue grain locally.
func startWakeSystem(t *testing.T, runtime *Runtime) goakt.ActorSystem {
	t.Helper()

	ctx := context.Background()

	system, err := goakt.NewActorSystem("wake-live-system",
		goakt.WithLogger(goaktlog.DiscardLogger), goakt.WithExtensions(runtime))
	require.NoError(t, err)
	require.NoError(t, system.Start(ctx))

	t.Cleanup(func() { _ = system.Stop(ctx) })

	return system
}

// TestWakeQueueLogsResolveAndTellFailures covers wakeQueue's two best-effort
// error branches: a failed grain resolve and a failed wake-up tell are each
// logged and swallowed rather than propagated.
func TestWakeQueueLogsResolveAndTellFailures(t *testing.T) {
	ctx := context.Background()

	// Resolve-failure branch: an unstarted system fails grain activation.
	resolveLogs, resolveRuntime := newBufferRuntime(t)

	unstarted, err := goakt.NewActorSystem("wake-unstarted-system", goakt.WithLogger(goaktlog.DiscardLogger))
	require.NoError(t, err)

	wakeQueue(ctx, unstarted, resolveRuntime, "q", 0)
	require.Contains(t, resolveLogs.String(), "resolving queue grain failed")

	// Tell-failure branch: resolution succeeds against a live system, but the
	// wake-up tell fails.
	tellLogs, tellRuntime := newBufferRuntime(t)
	system := startWakeSystem(t, tellRuntime)

	wakeQueue(ctx, tellFailSystem{ActorSystem: system, err: errors.New("tell down")}, tellRuntime, "q", 0)
	require.Contains(t, tellLogs.String(), "waking queue grain failed")
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
