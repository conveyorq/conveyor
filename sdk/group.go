// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package conveyor

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"

	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
)

// BatchHandlerFunc processes a fired aggregation group — every member in one
// call. Returning nil acknowledges all members; returning a *BatchError marks
// specific members for retry or skip-retry; any other error retries every
// member. Handlers must be idempotent and should honor ctx cancellation.
type BatchHandlerFunc func(ctx context.Context, batch []*Task) error

// BatchMiddlewareFunc decorates a BatchHandlerFunc, e.g. with logging or
// metrics over a whole fired group. The returned handler must call next to
// keep the batch flowing. It is the batch counterpart of MiddlewareFunc:
// UseBatch wires it onto multi-member deliveries, whereas a group member
// redelivered as a batch of one travels the single-task path and so runs the
// MiddlewareFunc chain instead.
type BatchMiddlewareFunc func(next BatchHandlerFunc) BatchHandlerFunc

// HandleBatch registers a batch handler for one task type: a fired aggregation
// group of that type is delivered to it as one call. Registering a nil handler,
// an empty type, or a type already registered (as either a batch or single
// handler) panics, mirroring HandleFunc. A batch handler also serves single
// deliveries of its type — a retried or released group member redelivers as a
// batch of one — so a batch type needs no separate HandleFunc.
func (m *Mux) HandleBatch(taskType string, handler BatchHandlerFunc) {
	if taskType == "" {
		panic("conveyor: HandleBatch with empty task type")
	}

	if handler == nil {
		panic("conveyor: HandleBatch with nil handler")
	}

	m.requireUnregistered(taskType)

	m.batchHandlers[taskType] = handler
}

// UseBatch appends middleware applied to every batch handler of this Mux,
// regardless of registration order relative to HandleBatch. The first
// middleware registered runs outermost. Registering a nil middleware panics.
// It is the batch counterpart of Use.
func (m *Mux) UseBatch(middleware ...BatchMiddlewareFunc) {
	for _, wrap := range middleware {
		if wrap == nil {
			panic("conveyor: UseBatch with nil middleware")
		}

		m.batchMiddleware = append(m.batchMiddleware, wrap)
	}
}

// batchHandler returns the batch handler registered for a task type, wrapped in
// the registered batch middleware (outermost first).
func (m *Mux) batchHandler(taskType string) (BatchHandlerFunc, bool) {
	handler, ok := m.batchHandlers[taskType]

	if !ok {
		return nil, false
	}

	for i := len(m.batchMiddleware) - 1; i >= 0; i-- {
		handler = m.batchMiddleware[i](handler)
	}

	return handler, true
}

// batchTypes lists the task types registered with HandleBatch, advertised to
// the server in Hello so it batch-dispatches only types this worker can handle.
func (m *Mux) batchTypes() []string {
	types := make([]string, 0, len(m.batchHandlers))
	for taskType := range m.batchHandlers {
		types = append(types, taskType)
	}

	return types
}

// BatchError reports per-member outcomes from a batch handler (Mux.HandleBatch).
// A member whose id appears in Errs failed — it is retried, or archived if its
// error is wrapped with SkipRetry; members not listed succeed. Return it for
// partial failure; return a plain error to fail the whole batch, or nil for
// all-success.
type BatchError struct {
	// Errs maps a batch member's task id to its failure.
	Errs map[string]error
}

// Error implements the error interface.
func (e *BatchError) Error() string {
	return fmt.Sprintf("conveyor: batch failed for %d of its members", len(e.Errs))
}

// Group makes the task a member of the named aggregation group within its
// queue: grouped tasks accumulate and are delivered to a worker as one batch
// (via Mux.HandleBatch) when the group fires. A group is single-type — all
// members share the task's type. Mutually exclusive with ProcessAt/ProcessIn.
func Group(name string) EnqueueOption {
	return func(o *enqueueOptions) { o.group = name }
}

// dispatchBatch registers a fired aggregation group and starts its execution.
// All members share one execution context (and one slot): a Cancel for any
// member, or a drain, cancels the whole batch, and heartbeats extend every
// member's lease while it runs.
func (s *workerSession) dispatchBatch(batch *conveyorv1.BatchDispatch) {
	tasks := batch.GetTasks()
	if len(tasks) == 0 {
		return
	}

	executionCtx, cancel := context.WithCancelCause(context.Background())
	release := func() { cancel(nil) }

	if deadline := batch.GetDeadline(); deadline.IsValid() {
		var stopDeadline context.CancelFunc

		executionCtx, stopDeadline = context.WithDeadline(executionCtx, deadline.AsTime())
		release = func() { stopDeadline(); cancel(nil) }
	}

	s.stateMutex.Lock()
	for _, task := range tasks {
		s.cancels[task.GetId()] = cancel
	}
	s.stateMutex.Unlock()

	go s.executeBatch(executionCtx, release, batch)
}

// executeBatch waits for a slot, runs the batch handler over all members in one
// call, and reports a BatchResult.
func (s *workerSession) executeBatch(ctx context.Context, release func(), batch *conveyorv1.BatchDispatch) {
	defer release()

	select {
	case s.slots <- struct{}{}:
		defer func() { <-s.slots }()

	case <-ctx.Done():
		s.finishBatch(ctx, batch, ctx.Err())

		return
	}

	taskType := batch.GetTasks()[0].GetType()

	handler, ok := s.mux.batchHandler(taskType)
	if !ok {
		s.finishBatch(ctx, batch, fmt.Errorf("no batch handler registered for task type %q", taskType))

		return
	}

	tasks := make([]*Task, 0, len(batch.GetTasks()))
	for _, envelope := range batch.GetTasks() {
		tasks = append(tasks, newTaskFromEnvelope(envelope))
	}

	s.finishBatch(ctx, batch, invokeBatch(ctx, handler, tasks))
}

// invokeBatch runs one batch handler, converting a panic into a retryable error
// for the whole batch.
func invokeBatch(ctx context.Context, handler BatchHandlerFunc, tasks []*Task) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("batch handler panic: %v\n%s", recovered, debug.Stack())
		}
	}()

	return handler(ctx, tasks)
}

// finishBatch reports the per-member outcomes of a finished batch and forgets
// its members.
func (s *workerSession) finishBatch(ctx context.Context, batch *conveyorv1.BatchDispatch, handlerErr error) {
	results := make([]*conveyorv1.Result, 0, len(batch.GetTasks()))

	for _, task := range batch.GetTasks() {
		outcome, errMsg := batchMemberResult(ctx, task.GetId(), handlerErr)
		results = append(results, &conveyorv1.Result{TaskId: task.GetId(), Outcome: outcome, ErrorMsg: errMsg})
	}

	s.stateMutex.Lock()
	for _, task := range batch.GetTasks() {
		delete(s.cancels, task.GetId())
	}
	s.stateMutex.Unlock()

	frame := &conveyorv1.WorkerMessage{
		Frame: &conveyorv1.WorkerMessage_BatchResult{BatchResult: &conveyorv1.BatchResult{Results: results}},
	}

	_ = s.send(frame)
}

// batchMemberResult maps a batch handler result to one member's outcome: a
// drain releases every member; a *BatchError marks only its listed members
// (skip-retry or retry), the rest success; any other error retries every
// member; nil succeeds every member.
func batchMemberResult(ctx context.Context, taskID string, handlerErr error) (conveyorv1.TaskOutcome, string) {
	if handlerErr == nil {
		return conveyorv1.TaskOutcome_TASK_OUTCOME_SUCCESS, ""
	}

	if errors.Is(context.Cause(ctx), errDraining) {
		return conveyorv1.TaskOutcome_TASK_OUTCOME_RELEASED, ""
	}

	var batchErr *BatchError
	if errors.As(handlerErr, &batchErr) {
		memberErr, failed := batchErr.Errs[taskID]
		if !failed {
			return conveyorv1.TaskOutcome_TASK_OUTCOME_SUCCESS, ""
		}

		return errorOutcome(memberErr)
	}

	return errorOutcome(handlerErr)
}

// errorOutcome maps a non-nil member error to its retryable or skip-retry
// outcome.
func errorOutcome(err error) (conveyorv1.TaskOutcome, string) {
	message := ""
	if err != nil {
		message = err.Error()
	}

	if IsSkipRetry(err) {
		return conveyorv1.TaskOutcome_TASK_OUTCOME_SKIP_RETRY, message
	}

	return conveyorv1.TaskOutcome_TASK_OUTCOME_RETRY, message
}
