// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package conveyor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
)

func TestReportProgressCoalescesAndClamps(t *testing.T) {
	var sent []*conveyorv1.Progress

	reporter := &progressReporter{
		taskID: "task-1",
		send: func(message *conveyorv1.WorkerMessage) error {
			sent = append(sent, message.GetProgress())

			return nil
		},
	}
	ctx := withProgressReporter(context.Background(), reporter)

	require.NoError(t, ReportProgress(ctx, 10, "starting"))
	require.NoError(t, ReportProgress(ctx, 10, "starting")) // identical: coalesced
	require.NoError(t, ReportProgress(ctx, 60, "halfway"))
	require.NoError(t, ReportProgress(ctx, 150, "done")) // out of range: clamped

	require.Len(t, sent, 3)
	require.Equal(t, "task-1", sent[0].GetTaskId())
	require.Equal(t, uint32(10), sent[0].GetPercent())
	require.Equal(t, uint32(60), sent[1].GetPercent())
	require.Equal(t, uint32(100), sent[2].GetPercent())
	require.Equal(t, "done", sent[2].GetMessage())
}

func TestReportProgressWithoutReporterIsNoop(t *testing.T) {
	require.NoError(t, ReportProgress(context.Background(), 50, "ignored"))
}
