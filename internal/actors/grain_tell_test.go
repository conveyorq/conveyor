// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package actors

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/conveyorq/conveyor/internal/broker/memory"
	"github.com/conveyorq/conveyor/internal/clock"
	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
)

// TestActorsNeverWaitOnGrainsInsideTurns pins the rule the helpers in
// grain_tell.go exist for. GoAkt runs actors and grains on one dispatcher pool
// sized to the CPU count, floored at two, and a tell to a grain returns only
// once the grain has run the message. An actor that waited for that inside its
// turn held a pool worker while waiting; with the pool at its floor, the reaper
// waking a queue and the sweeper firing a group at the same time left no
// worker for either grain, and every grain on the node stalled until the waits
// timed out. Pending work on queues no worker serves keeps the reaper telling
// their grains on every tick, aggregating groups no gateway can take keep the
// sweeper doing the same, and a served queue must keep dispatching throughout.
func TestActorsNeverWaitOnGrainsInsideTurns(t *testing.T) {
	const (
		queue    = "served"
		unserved = 5
		tasks    = 30
	)

	settings := testSettings
	settings.ReapInterval = 100 * time.Millisecond
	settings.GroupGracePeriod = 50 * time.Millisecond
	settings.GroupSweepInterval = 50 * time.Millisecond

	ctx := context.Background()
	taskLog := memory.New(clock.System())
	engine := newNode(taskLog, settings, freePorts(t, 3), nil)

	// The dispatcher pool is sized from GOMAXPROCS when the actor system
	// starts; one CPU gives the floor of two workers.
	previous := runtime.GOMAXPROCS(1)
	err := engine.Start(ctx)
	runtime.GOMAXPROCS(previous)
	require.NoError(t, err)

	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = engine.Stop(stopCtx)
	})

	for index := range unserved {
		idle := fmt.Sprintf("idle-%d", index)
		require.NoError(t, taskLog.Enqueue(ctx, newTask(fmt.Sprintf("pending-%d", index), idle, "test:manual", 4)))
		require.NoError(t, taskLog.Enqueue(ctx, groupedTask(fmt.Sprintf("member-%d", index), idle, "test:batch", "G")))
	}

	recorder := newFrameRecorder()

	handle, err := engine.SpawnGateway(ctx, GatewaySession{
		SessionID:   "session-pool-floor",
		Queues:      []string{queue},
		Concurrency: 4,
	}, recorder)
	require.NoError(t, err)

	t.Cleanup(func() { _ = handle.Stop(ctx) })

	for index := range tasks {
		require.NoError(t, taskLog.Enqueue(ctx, newTask(fmt.Sprintf("served-%d", index), queue, "test:manual", 4)))
	}

	// Every completion is reported to the grain, which refills the credit the
	// next dispatch needs, and each is replaced by a new task, so the served
	// queue keeps flowing through the window only while no turn waits on a
	// grain: a parked turn stalls dispatch for the grain request timeout, five
	// seconds, and the maintenance ticks overlap many times in eight.
	const (
		window = 8 * time.Second
		stall  = 3 * time.Second
	)

	closing := time.After(window)
	next := tasks
	dispatched := 0

	for {
		select {
		case dispatch := <-recorder.dispatched:
			dispatched++
			require.NoError(t, handle.Tell(ctx, &conveyorv1.Result{
				TaskId:  dispatch.GetTask().GetId(),
				Outcome: conveyorv1.TaskOutcome_TASK_OUTCOME_SUCCESS,
			}))
			require.NoError(t, taskLog.Enqueue(ctx, newTask(fmt.Sprintf("served-%d", next), queue, "test:manual", 4)))
			next++

		case <-time.After(stall):
			t.Fatalf("no dispatch for %s after %d: a turn is waiting on a grain", stall, dispatched)

		case <-closing:
			require.GreaterOrEqual(t, dispatched, tasks, "the served queue kept flowing through the window")

			return
		}
	}
}
