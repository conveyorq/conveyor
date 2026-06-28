// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package conveyor

import (
	"context"
	"errors"

	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
)

// TxTask pairs a task with its per-task enqueue options. Each task in an
// EnqueueTx call keeps its own options, so a single atomic enqueue may span
// queues, priorities, and schedules.
type TxTask struct {
	// Task is the task to commit.
	Task *Task
	// Options are the per-task enqueue options applied to Task.
	Options []EnqueueOption
}

// Tx bundles a task and its enqueue options into a TxTask, for the EnqueueTx
// task list:
//
//	client.EnqueueTx(ctx, []conveyor.TxTask{
//	    conveyor.Tx(chargeTask, conveyor.Queue("billing")),
//	    conveyor.Tx(receiptTask, conveyor.Queue("mail")),
//	})
func Tx(task *Task, opts ...EnqueueOption) TxTask {
	return TxTask{Task: task, Options: opts}
}

// EnqueueTx commits every task atomically: either all are enqueued or none are.
// If any task fails (a duplicate unique key, a unique-key collision between two
// tasks in the call, or an invalid task), no task is committed and the error
// identifies the offending task. On success it returns the committed tasks in
// the order given. Re-committing an existing task id is a no-op success that does
// not abort the call.
//
// Unlike Enqueue, EnqueueTx does not run the client enqueue middleware: the
// middleware decorates a single-task commit, which the all-or-nothing path does
// not model.
func (c *Client) EnqueueTx(ctx context.Context, tasks []TxTask) ([]*TaskInfo, error) {
	if len(tasks) == 0 {
		return nil, errors.New("conveyor: at least one task is required")
	}

	requests := make([]*conveyorv1.EnqueueRequest, len(tasks))

	for i, item := range tasks {
		settings, uniqueKey, err := resolveEnqueueOptions(item.Task, item.Options...)
		if err != nil {
			return nil, err
		}

		request, err := c.buildEnqueueRequest(ctx, item.Task, settings, uniqueKey)
		if err != nil {
			return nil, err
		}

		requests[i] = request
	}

	infos, err := c.wire.EnqueueTx(ctx, requests)
	if err != nil {
		return nil, wireError(err)
	}

	result := make([]*TaskInfo, len(infos))

	for i, info := range infos {
		result[i] = taskInfoFromProto(info)
	}

	return result, nil
}
