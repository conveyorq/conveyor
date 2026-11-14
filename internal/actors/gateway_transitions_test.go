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

	"github.com/conveyorq/conveyor/internal/broker"
	"github.com/conveyorq/conveyor/internal/broker/memory"
	"github.com/conveyorq/conveyor/internal/clock"
	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
)

func TestExtendLeasesReportsOnlyLostLeases(t *testing.T) {
	ctx := context.Background()
	gateway, faults := faultGateway(t)
	refs := []leaseRef{{taskID: "task-1", leaseID: "lease-1"}}

	faults.fault(methodExtendLease, broker.ErrLeaseLost)
	require.Equal(t, refs, extendLeases(ctx, faults, gateway.runtime.Logger(), refs, time.Minute))

	faults.clear(methodExtendLease)
	faults.fault(methodExtendLease, errors.New("store down"))
	require.Empty(t, extendLeases(ctx, faults, gateway.runtime.Logger(), refs, time.Minute),
		"a failure other than a lost lease leaves the lease to the next heartbeat")
}

func TestGatewayIgnoresLostLeasesOfResolvedOrReleasedDeliveries(t *testing.T) {
	gateway, _ := faultGateway(t)
	current := &inflightTask{leaseID: "lease-new", queue: "q"}
	gateway.inflight = map[string]*inflightTask{"task-1": current}

	// Neither report matches a tracked delivery, so nothing is touched and no
	// receive context is needed.
	gateway.leasesExtended(nil, leasesExtended{lost: []leaseRef{
		{taskID: "task-1", leaseID: "lease-old"},
		{taskID: "task-unknown", leaseID: "lease-1"},
	}})

	require.Same(t, current, gateway.inflight["task-1"], "a delivery re-leased since keeps its tracking")
}

func TestApplyBatchOutcomesCountsSuccessesAndTerminals(t *testing.T) {
	const (
		queue   = "batch-outcomes"
		leaseID = "lease-batch"
	)

	ctx := context.Background()
	taskLog := memory.New(clock.System())
	runtime := NewRuntime(taskLog, clock.System(), testSettings, quietLogger())

	for _, id := range []string{"m-1", "m-2", "m-3"} {
		require.NoError(t, taskLog.Enqueue(ctx, groupedTask(id, queue, "test:batch", "G")))
	}

	_, err := taskLog.LeaseGroup(ctx, queue, "G", 3, time.Minute, leaseID)
	require.NoError(t, err)

	entry := func() *inflightTask {
		return &inflightTask{leaseID: leaseID, queue: queue, taskType: "test:batch", maxRetry: 3, dispatchedAt: runtime.Clock().Now()}
	}

	applied := applyBatchOutcomes(ctx, runtime, queue, []batchMember{
		{entry: entry(), result: &conveyorv1.Result{TaskId: "m-1", Outcome: conveyorv1.TaskOutcome_TASK_OUTCOME_SUCCESS}},
		{entry: entry(), result: &conveyorv1.Result{TaskId: "m-2", Outcome: conveyorv1.TaskOutcome_TASK_OUTCOME_SKIP_RETRY}},
		{entry: entry(), result: &conveyorv1.Result{TaskId: "m-3", Outcome: conveyorv1.TaskOutcome_TASK_OUTCOME_RELEASED}},
	})

	require.Equal(t, queue, applied.queue)
	require.Equal(t, 3, applied.total)
	require.Equal(t, 1, applied.succeeded)
	require.ElementsMatch(t, []string{"m-1", "m-2"}, applied.terminal, "the acked and the archived members are terminal; the released one is not")
}

func TestAckBatchWithNothingToAck(t *testing.T) {
	runtime := NewRuntime(memory.New(clock.System()), clock.System(), testSettings, quietLogger())

	require.Empty(t, ackBatch(context.Background(), runtime, nil, nil))
}
