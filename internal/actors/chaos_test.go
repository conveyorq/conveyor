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

package actors

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	goakt "github.com/tochemey/goakt/v4/actor"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/conveyorq/conveyor/internal/broker"
	"github.com/conveyorq/conveyor/internal/broker/memory"
	"github.com/conveyorq/conveyor/internal/clock"
	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
)

// TestThreeNodeChaosLosesNothing is the Phase 5 chaos acceptance test: three
// in-process nodes share one durable store; the queue grain is pinned to one
// node and a worker gateway lives on another, while a third survivor also
// runs a gateway. Both the grain's host and the gateway's host are killed
// mid-load. Recovery must come from grain re-activation on the survivor plus
// lease expiry re-delivering the dead nodes' in-flight tasks, and every task
// must complete exactly once.
func TestThreeNodeChaosLosesNothing(t *testing.T) {
	if testing.Short() {
		t.Skip("cluster chaos test skipped in -short mode")
	}

	const (
		totalTasks = 600
		chaosQueue = "chaos"
	)

	ctx := context.Background()

	// One shared broker stands in for the shared durable store.
	taskLog := memory.New(clock.System())

	survivorPorts := freePorts(t, 3)
	grainPorts := freePorts(t, 3)
	sessionPorts := freePorts(t, 3)

	// All three nodes know all three gossip hosts up front (static discovery).
	peers := []string{
		fmt.Sprintf("%s:%d", testBindAddr, survivorPorts[1]),
		fmt.Sprintf("%s:%d", testBindAddr, grainPorts[1]),
		fmt.Sprintf("%s:%d", testBindAddr, sessionPorts[1]),
	}

	survivor := newNode(taskLog, recoverySettings, survivorPorts, peers)
	require.NoError(t, survivor.Start(ctx))

	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		_ = survivor.Stop(stopCtx)
	})

	grainHost := newNode(taskLog, recoverySettings, grainPorts, peers)
	require.NoError(t, grainHost.Start(ctx))

	sessionHost := newNode(taskLog, recoverySettings, sessionPorts, peers)
	require.NoError(t, sessionHost.Start(ctx))

	// Pin the queue grain to the node that will die, so the kill always
	// exercises relocation rather than passing trivially.
	_, err := grainHost.System().GrainIdentity(ctx, QueueGrainName(chaosQueue), queueGrainFactory,
		goakt.WithGrainDeactivateAfter(recoverySettings.PassivateAfter),
		goakt.WithActivationStrategy(goakt.LocalActivation))
	require.NoError(t, err)

	slowHandler := func(*conveyorv1.TaskEnvelope) error {
		time.Sleep(2 * time.Millisecond)

		return nil
	}

	// Two gateways serve the queue: one on the survivor and one on the node
	// that will be killed. The survivor's gateway must absorb all work once
	// the session host dies and its in-flight leases expire.
	// Distinct names: actor names are cluster-unique, so two gateways serving
	// the same queue on different nodes cannot share the default name.
	spawnGateway(t, survivor, &mockGateway{name: "gateway-chaos-survivor", queue: chaosQueue, capacity: 20, handler: slowHandler})
	spawnGateway(t, sessionHost, &mockGateway{name: "gateway-chaos-session", queue: chaosQueue, capacity: 20, handler: slowHandler})

	for sequence := range totalTasks {
		task := newTask(fmt.Sprintf("task-%05d", sequence), chaosQueue, "test:ok", int32(sequence%10))
		require.NoError(t, survivor.Enqueue(ctx, task))
	}

	require.Eventually(t, completedReaches(taskLog, 120),
		time.Minute, 20*time.Millisecond, "partial progress before the kill")

	// Kill both the grain's host and a session host at once. Recovery must
	// re-activate the grain on the survivor and re-deliver the dead nodes'
	// in-flight leases to the survivor's gateway.
	killNode(grainHost)
	killNode(sessionHost)

	require.Eventually(t, completedReaches(taskLog, totalTasks),
		2*time.Minute, 50*time.Millisecond, "all tasks must complete on the survivor")

	requireDrained(t, taskLog)
}

// TestCronSurvivesSchedulerFailover is the Phase 6 cron failover acceptance:
// across the loss of the node hosting the scheduler singleton, cron keeps
// firing on the survivor. The per-slot unique key (covered by the cron and
// broker tests) guarantees no slot fires twice during the relocation; this
// test proves the schedule does not stall.
func TestCronSurvivesSchedulerFailover(t *testing.T) {
	if testing.Short() {
		t.Skip("cron failover test skipped in -short mode")
	}

	const cronQueue = "cronfailover"

	ctx := context.Background()
	taskLog := memory.New(clock.System())

	survivorPorts := freePorts(t, 3)
	doomedPorts1 := freePorts(t, 3)
	doomedPorts2 := freePorts(t, 3)

	peers := []string{
		fmt.Sprintf("%s:%d", testBindAddr, survivorPorts[1]),
		fmt.Sprintf("%s:%d", testBindAddr, doomedPorts1[1]),
		fmt.Sprintf("%s:%d", testBindAddr, doomedPorts2[1]),
	}

	survivor := newNode(taskLog, recoverySettings, survivorPorts, peers)
	require.NoError(t, survivor.Start(ctx))

	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		_ = survivor.Stop(stopCtx)
	})

	doomed1 := newNode(taskLog, recoverySettings, doomedPorts1, peers)
	require.NoError(t, doomed1.Start(ctx))

	doomed2 := newNode(taskLog, recoverySettings, doomedPorts2, peers)
	require.NoError(t, doomed2.Start(ctx))

	// The gateway that runs cron tasks lives on the survivor.
	spawnGateway(t, survivor, &mockGateway{queue: cronQueue, capacity: 20})

	entry := &broker.CronEntry{
		ID:          "failover-cron",
		Spec:        "* * * * * *",
		TaskType:    "test:ok",
		Queue:       cronQueue,
		Payload:     []byte(`{}`),
		ContentType: "application/json",
		Options:     &conveyorv1.TaskOptions{MaxRetry: 1, Retention: durationpb.New(time.Hour)},
	}
	require.NoError(t, taskLog.UpsertCronEntry(ctx, entry))

	// Cron is firing before the failover.
	require.Eventually(t, completedReaches(taskLog, 2),
		30*time.Second, 50*time.Millisecond, "cron must fire before the failover")

	before, err := tasksInState(taskLog, conveyorv1.TaskState_TASK_STATE_COMPLETED)
	require.NoError(t, err)

	// Kill every node but the survivor. Whichever node hosted the scheduler
	// singleton, it must relocate to the survivor and resume firing.
	killNode(doomed1)
	killNode(doomed2)

	require.Eventually(t, completedReaches(taskLog, before+3),
		2*time.Minute, 100*time.Millisecond, "cron must keep firing after losing the scheduler's host")
}
