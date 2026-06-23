// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package conveyor

import (
	"context"
	"sync"

	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
)

// maxProgressPercent is the upper bound of a reported progress percent.
const maxProgressPercent = 100

// progressReporter sends a running task's progress to the server over the
// worker session. It coalesces consecutive identical updates so a chatty
// handler cannot flood the stream, bounding sends to one per distinct value.
type progressReporter struct {
	// send writes a frame down the worker session; the session serializes it.
	send func(*conveyorv1.WorkerMessage) error
	// taskID is the dispatched task the progress applies to.
	taskID string

	mutex    sync.Mutex
	reported bool
	lastPct  uint32
	lastMsg  string
}

// report sends one progress update unless it duplicates the last one sent.
// percent above 100 is clamped.
func (r *progressReporter) report(percent uint32, message string) error {
	if percent > maxProgressPercent {
		percent = maxProgressPercent
	}

	r.mutex.Lock()

	if r.reported && percent == r.lastPct && message == r.lastMsg {
		r.mutex.Unlock()

		return nil
	}

	r.reported = true
	r.lastPct = percent
	r.lastMsg = message
	r.mutex.Unlock()

	return r.send(&conveyorv1.WorkerMessage{
		Frame: &conveyorv1.WorkerMessage_Progress{
			Progress: &conveyorv1.Progress{TaskId: r.taskID, Percent: percent, Message: message},
		},
	})
}

// ReportProgress records how far the executing task has advanced, as a percent
// (0 to 100, clamped) plus an optional human-readable message such as
// "dumped 11/54 tables". It is advisory: the value surfaces on the task's
// status for inspection (GetTask, the dashboard) and never affects execution.
// Consecutive identical reports are coalesced into a single send.
//
// ReportProgress is a no-op when ctx is not a running task's handler context,
// for example a handler invoked directly in a test.
func ReportProgress(ctx context.Context, percent uint32, message string) error {
	reporter, ok := progressReporterFrom(ctx)
	if !ok {
		return nil
	}

	return reporter.report(percent, message)
}
