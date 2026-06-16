// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package conveyor

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
)

func TestMuxHandleBatch(t *testing.T) {
	mux := NewMux()
	mux.HandleBatch("digest", func(context.Context, []*Task) error { return nil })

	require.Equal(t, []string{"digest"}, mux.batchTypes())

	_, ok := mux.batchHandler("digest")
	require.True(t, ok)

	// A type registered as a batch handler is also served on single delivery.
	_, ok = mux.handler("digest")
	require.True(t, ok)

	// A type registered twice — as either kind — panics, as HandleFunc does.
	require.Panics(t, func() { mux.HandleBatch("digest", func(context.Context, []*Task) error { return nil }) })
	require.Panics(t, func() { mux.HandleFunc("digest", func(context.Context, *Task) error { return nil }) })
	require.Panics(t, func() { mux.HandleBatch("", func(context.Context, []*Task) error { return nil }) })
	require.Panics(t, func() { mux.HandleBatch("x", nil) })
}

// TestMuxUseBatchWrapsHandlersInOrder mirrors TestMuxUseWrapsHandlersInOrder
// for the batch path: the first middleware registered runs outermost, and
// UseBatch after HandleBatch still applies.
func TestMuxUseBatchWrapsHandlersInOrder(t *testing.T) {
	var calls []string

	record := func(name string) BatchMiddlewareFunc {
		return func(next BatchHandlerFunc) BatchHandlerFunc {
			return func(ctx context.Context, batch []*Task) error {
				calls = append(calls, name)

				return next(ctx, batch)
			}
		}
	}

	mux := NewMux()
	mux.UseBatch(record("first"))

	mux.HandleBatch("digest", func(context.Context, []*Task) error {
		calls = append(calls, "handler")

		return nil
	})

	mux.UseBatch(record("second"))

	handler, ok := mux.batchHandler("digest")
	require.True(t, ok)
	require.NoError(t, handler(context.Background(), []*Task{}))
	require.Equal(t, []string{"first", "second", "handler"}, calls)
}

func TestMuxUseBatchPanicsOnNilMiddleware(t *testing.T) {
	require.PanicsWithValue(t, "conveyor: UseBatch with nil middleware", func() {
		NewMux().UseBatch(nil)
	})
}

// TestMuxUseBatchOnlyWrapsBatchDelivery verifies the boundary rule: batch
// middleware decorates a multi-member delivery, while a group member redelivered
// as a batch of one travels the single-task path and runs MiddlewareFunc only.
func TestMuxUseBatchOnlyWrapsBatchDelivery(t *testing.T) {
	var calls []string

	mux := NewMux()

	mux.Use(func(next HandlerFunc) HandlerFunc {
		return func(ctx context.Context, task *Task) error {
			calls = append(calls, "single-mw")

			return next(ctx, task)
		}
	})

	mux.UseBatch(func(next BatchHandlerFunc) BatchHandlerFunc {
		return func(ctx context.Context, batch []*Task) error {
			calls = append(calls, "batch-mw")

			return next(ctx, batch)
		}
	})

	mux.HandleBatch("t", func(context.Context, []*Task) error { return nil })

	batch, ok := mux.batchHandler("t")
	require.True(t, ok)
	require.NoError(t, batch(context.Background(), []*Task{{}, {}}))
	require.Equal(t, []string{"batch-mw"}, calls)

	calls = nil

	single, ok := mux.handler("t")
	require.True(t, ok)
	require.NoError(t, single(context.Background(), &Task{}))
	require.Equal(t, []string{"single-mw"}, calls)
}

// TestMuxBatchHandlerServesSingleDelivery verifies a retried or released group
// member (delivered as a single task) runs through the batch handler as a batch
// of one.
func TestMuxBatchHandlerServesSingleDelivery(t *testing.T) {
	var got []*Task

	mux := NewMux()
	mux.HandleBatch("t", func(_ context.Context, batch []*Task) error {
		got = batch

		return nil
	})

	handler, ok := mux.handler("t")
	require.True(t, ok)
	require.NoError(t, handler(context.Background(), &Task{id: "x", taskType: "t"}))
	require.Len(t, got, 1)
	require.Equal(t, "x", got[0].ID())
}

// TestBatchMemberResult covers the per-member outcome mapping for a batch.
func TestBatchMemberResult(t *testing.T) {
	background := context.Background()

	outcome, msg := batchMemberResult(background, "a", nil)
	require.Equal(t, conveyorv1.TaskOutcome_TASK_OUTCOME_SUCCESS, outcome)
	require.Empty(t, msg)

	// A plain error fails every member with RETRY.
	outcome, msg = batchMemberResult(background, "a", errors.New("boom"))
	require.Equal(t, conveyorv1.TaskOutcome_TASK_OUTCOME_RETRY, outcome)
	require.Equal(t, "boom", msg)

	// A BatchError marks only its listed members; the rest succeed.
	batchErr := &BatchError{Errs: map[string]error{
		"a": errors.New("a-fail"),
		"b": SkipRetry(errors.New("b-bad")),
	}}

	outcome, _ = batchMemberResult(background, "a", batchErr)
	require.Equal(t, conveyorv1.TaskOutcome_TASK_OUTCOME_RETRY, outcome)
	outcome, _ = batchMemberResult(background, "b", batchErr)
	require.Equal(t, conveyorv1.TaskOutcome_TASK_OUTCOME_SKIP_RETRY, outcome)
	outcome, _ = batchMemberResult(background, "c", batchErr)
	require.Equal(t, conveyorv1.TaskOutcome_TASK_OUTCOME_SUCCESS, outcome)

	// A drain releases every member without penalty.
	drainCtx, cancel := context.WithCancelCause(context.Background())
	cancel(errDraining)
	outcome, _ = batchMemberResult(drainCtx, "a", errors.New("interrupted"))
	require.Equal(t, conveyorv1.TaskOutcome_TASK_OUTCOME_RELEASED, outcome)
}

// TestEnqueueGroupRejectsSchedule verifies the server rejects a grouped task
// that also asks to be scheduled.
func TestEnqueueGroupRejectsSchedule(t *testing.T) {
	baseURL := startTestServer(t, nil)

	client, err := NewClient(baseURL)
	require.NoError(t, err)

	_, err = client.Enqueue(context.Background(),
		NewTask("digest:send", JSON("x")), Group("g"), ProcessIn(time.Minute))
	require.Error(t, err, "group and process_in are mutually exclusive")
}

// TestWorkerProcessesBatchEndToEnd proves grouping end to end through a real
// server: grouped tasks enqueued via the client accumulate, fire as one group,
// and are delivered to a HandleBatch worker in a single call.
func TestWorkerProcessesBatchEndToEnd(t *testing.T) {
	baseURL := startTestServer(t, nil)

	client, err := NewClient(baseURL)
	require.NoError(t, err)

	worker, err := NewWorker(baseURL, WithQueues(map[string]int{"default": 1}), WithConcurrency(4))
	require.NoError(t, err)

	var (
		mutex      sync.Mutex
		batchSizes []int
		handled    = make(map[string]bool)
	)

	mux := NewMux()
	mux.HandleBatch("digest:send", func(_ context.Context, batch []*Task) error {
		mutex.Lock()
		defer mutex.Unlock()

		batchSizes = append(batchSizes, len(batch))
		for _, task := range batch {
			handled[task.ID()] = true
		}

		return nil
	})

	runCtx, stopWorker := context.WithCancel(context.Background())
	defer stopWorker()

	runDone := make(chan error, 1)
	go func() { runDone <- worker.Run(runCtx, mux) }()

	ctx := context.Background()

	var ids []string

	for range 5 {
		info, err := client.Enqueue(ctx, NewTask("digest:send", JSON("x")), Group("nightly"), Retention(time.Hour))
		require.NoError(t, err)
		require.Equal(t, TaskStateAggregating, info.State, "a grouped task is reported aggregating")

		ids = append(ids, info.ID)
	}

	for _, id := range ids {
		awaitTaskState(t, client, id, TaskStateCompleted)
	}

	mutex.Lock()
	defer mutex.Unlock()

	require.Len(t, handled, 5, "every member ran")
	require.Contains(t, batchSizes, 5, "the group was delivered as one batch")

	stopWorker()
	require.NoError(t, <-runDone)
}
