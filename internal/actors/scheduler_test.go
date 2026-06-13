package actors

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	goakt "github.com/tochemey/goakt/v4/actor"
	goaktlog "github.com/tochemey/goakt/v4/log"
	"google.golang.org/protobuf/types/known/timestamppb"

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
