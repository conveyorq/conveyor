// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/conveyorq/conveyor/embedded"
	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
)

// startEmbeddedNode boots an embedded system and returns its base URL.
func startEmbeddedNode(t *testing.T) string {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	system, err := embedded.Start(ctx, embedded.Config{})
	require.NoError(t, err)

	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), testTimeout)
		defer stopCancel()

		require.NoError(t, system.Stop(stopCtx))
	})

	return "http://" + system.Addr()
}

// enqueueOne enqueues a task through the CLI and returns its id.
func enqueueOne(t *testing.T, addr, taskType string, extra ...string) string {
	t.Helper()

	var out bytes.Buffer

	args := append([]string{"--addr", addr, "enqueue", taskType}, extra...)
	require.NoError(t, run(args, &out))

	fields := strings.Fields(out.String())
	require.GreaterOrEqual(t, len(fields), 2)

	return fields[1]
}

func TestQueuesUsageErrors(t *testing.T) {
	err := run([]string{"queues"}, &bytes.Buffer{})
	require.ErrorContains(t, err, "usage: conveyor queues")

	err = run([]string{"queues", "drop", "default"}, &bytes.Buffer{})
	require.ErrorContains(t, err, `unknown subcommand "drop"`)
}

func TestRateLimitUsageErrors(t *testing.T) {
	err := run([]string{"ratelimit"}, &bytes.Buffer{})
	require.ErrorContains(t, err, "usage: conveyor ratelimit")

	err = run([]string{"ratelimit", "explode"}, &bytes.Buffer{})
	require.ErrorContains(t, err, `unknown subcommand "explode"`)

	err = run([]string{"ratelimit", "set"}, &bytes.Buffer{})
	require.ErrorContains(t, err, "exactly one queue name is required")
}

func TestRateLimitAgainstEmbeddedServer(t *testing.T) {
	addr := startEmbeddedNode(t)

	var setOut bytes.Buffer

	require.NoError(t, run([]string{"--addr", addr, "ratelimit", "set", "email", "--rate", "50", "--burst", "10"}, &setOut))
	require.Contains(t, setOut.String(), "queue email limited to 50/s (burst 10)")

	var lsOut bytes.Buffer

	require.NoError(t, run([]string{"--addr", addr, "ratelimit", "ls"}, &lsOut))
	require.Contains(t, lsOut.String(), "email")
	require.Contains(t, lsOut.String(), "50")

	var rmOut bytes.Buffer

	require.NoError(t, run([]string{"--addr", addr, "ratelimit", "rm", "email"}, &rmOut))
	require.Contains(t, rmOut.String(), "queue email rate limit cleared")

	var lsAfter bytes.Buffer

	require.NoError(t, run([]string{"--addr", addr, "ratelimit", "ls"}, &lsAfter))
	require.NotContains(t, lsAfter.String(), "email")
}

func TestCronUsageErrors(t *testing.T) {
	err := run([]string{"cron"}, &bytes.Buffer{})
	require.ErrorContains(t, err, "usage: conveyor cron")

	err = run([]string{"cron", "explode"}, &bytes.Buffer{})
	require.ErrorContains(t, err, `unknown subcommand "explode"`)

	err = run([]string{"cron", "pause"}, &bytes.Buffer{})
	require.ErrorContains(t, err, "exactly one entry id is required")
}

func TestClusterUsageErrors(t *testing.T) {
	err := run([]string{"cluster"}, &bytes.Buffer{})
	require.ErrorContains(t, err, "usage: conveyor cluster info")
}

func TestTaskOperationUsageErrors(t *testing.T) {
	for _, subcommand := range []string{"run", "cancel", "delete"} {
		err := run([]string{"tasks", subcommand}, &bytes.Buffer{})
		require.ErrorContains(t, err, "exactly one task id is required", "subcommand %s", subcommand)
	}
}

func TestTasksListRejectsUnknownState(t *testing.T) {
	err := run([]string{"tasks", "list", "--state", "limbo"}, &bytes.Buffer{})
	require.ErrorContains(t, err, `unknown state "limbo"`)
}

func TestEventsRejectsUnknownType(t *testing.T) {
	err := run([]string{"events", "--type", "exploded"}, &bytes.Buffer{})
	require.ErrorContains(t, err, `unknown event type "exploded"`)
}

func TestStatsAndQueuePauseAgainstEmbeddedServer(t *testing.T) {
	addr := startEmbeddedNode(t)

	enqueueOne(t, addr, "email:welcome", "--queue", "critical", "--in", "1h")

	var pauseOut bytes.Buffer

	require.NoError(t, run([]string{"--addr", addr, "queues", "pause", "critical"}, &pauseOut))
	require.Contains(t, pauseOut.String(), "queue critical paused")

	var statsOut bytes.Buffer

	require.NoError(t, run([]string{"--addr", addr, "stats"}, &statsOut))
	require.Contains(t, statsOut.String(), "QUEUE")
	require.Contains(t, statsOut.String(), "critical")
	require.Contains(t, statsOut.String(), "true")

	var resumeOut bytes.Buffer

	require.NoError(t, run([]string{"--addr", addr, "queues", "resume", "critical"}, &resumeOut))
	require.Contains(t, resumeOut.String(), "queue critical resumed")
}

func TestTaskLifecycleAgainstEmbeddedServer(t *testing.T) {
	addr := startEmbeddedNode(t)

	scheduledID := enqueueOne(t, addr, "report:daily", "--in", "1h")

	var listOut bytes.Buffer

	require.NoError(t, run([]string{"--addr", addr, "tasks", "list", "--state", "scheduled"}, &listOut))
	require.Contains(t, listOut.String(), scheduledID)

	var runOut bytes.Buffer

	require.NoError(t, run([]string{"--addr", addr, "tasks", "run", scheduledID}, &runOut))
	require.Contains(t, runOut.String(), "run requested")

	// Reschedule a waiting task to a new future time and confirm it stays scheduled.
	rescheduleID := enqueueOne(t, addr, "report:monthly", "--in", "1h")

	var rescheduleOut bytes.Buffer

	require.NoError(t, run([]string{"--addr", addr, "tasks", "reschedule", rescheduleID, "--in", "2h"}, &rescheduleOut))
	require.Contains(t, rescheduleOut.String(), "reschedule requested")

	var rescheduledList bytes.Buffer

	require.NoError(t, run([]string{"--addr", addr, "tasks", "list", "--state", "scheduled"}, &rescheduledList))
	require.Contains(t, rescheduledList.String(), rescheduleID)

	// An absolute --at instant is accepted; a malformed one is rejected.
	var rescheduleAtOut bytes.Buffer

	require.NoError(t, run([]string{"--addr", addr, "tasks", "reschedule", rescheduleID, "--at", "2999-01-01T00:00:00Z"}, &rescheduleAtOut))
	require.Contains(t, rescheduleAtOut.String(), "reschedule requested")

	require.ErrorContains(t, run([]string{"--addr", addr, "tasks", "reschedule", rescheduleID, "--at", "not-a-time"}, &bytes.Buffer{}),
		"parsing --at")

	// Setting neither flag, or both, is rejected before any call.
	require.ErrorContains(t, run([]string{"--addr", addr, "tasks", "reschedule", rescheduleID}, &bytes.Buffer{}),
		"set one of --in or --at")
	require.ErrorContains(t, run([]string{"--addr", addr, "tasks", "reschedule", rescheduleID, "--in", "1h", "--at", "2999-01-01T00:00:00Z"}, &bytes.Buffer{}),
		"set only one of --in or --at")

	canceledID := enqueueOne(t, addr, "report:weekly", "--in", "1h")

	var cancelOut bytes.Buffer

	require.NoError(t, run([]string{"--addr", addr, "tasks", "cancel", canceledID}, &cancelOut))
	require.Contains(t, cancelOut.String(), "cancel requested")

	var deleteOut bytes.Buffer

	require.NoError(t, run([]string{"--addr", addr, "tasks", "delete", canceledID}, &deleteOut))
	require.Contains(t, deleteOut.String(), "delete requested")

	err := run([]string{"--addr", addr, "tasks", "get", canceledID}, &bytes.Buffer{})
	require.ErrorContains(t, err, "not found")
}

func TestCronAndClusterAgainstEmbeddedServer(t *testing.T) {
	addr := startEmbeddedNode(t)

	// Create a cron entry, then confirm it round-trips through list.
	var addOut bytes.Buffer

	require.NoError(t, run([]string{"--addr", addr, "cron", "add", "nightly", "0 0 2 * * *", "report:daily", "--queue", "reports"}, &addOut))
	require.Contains(t, addOut.String(), "nightly saved")

	var cronOut bytes.Buffer

	require.NoError(t, run([]string{"--addr", addr, "cron", "list"}, &cronOut))
	require.Contains(t, cronOut.String(), "nightly")
	require.Contains(t, cronOut.String(), "report:daily")

	// A malformed spec is rejected by the server's validation.
	err := run([]string{"--addr", addr, "cron", "add", "bad", "not a spec", "report:daily"}, &bytes.Buffer{})
	require.Error(t, err)

	err = run([]string{"--addr", addr, "cron", "pause", "missing"}, &bytes.Buffer{})
	require.ErrorContains(t, err, "does not exist")

	var clusterOut bytes.Buffer

	require.NoError(t, run([]string{"--addr", addr, "cluster", "info"}, &clusterOut))
	require.Contains(t, clusterOut.String(), "ADDRESS")
}

func TestParseTaskState(t *testing.T) {
	state, err := parseTaskState("")
	require.NoError(t, err)
	require.Equal(t, conveyorv1.TaskState_TASK_STATE_UNSPECIFIED, state)

	state, err = parseTaskState("archived")
	require.NoError(t, err)
	require.Equal(t, conveyorv1.TaskState_TASK_STATE_ARCHIVED, state)

	_, err = parseTaskState("unspecified")
	require.Error(t, err)
}

func TestStateName(t *testing.T) {
	require.Equal(t, "pending", stateName(conveyorv1.TaskState_TASK_STATE_PENDING))
	require.Equal(t, "archived", stateName(conveyorv1.TaskState_TASK_STATE_ARCHIVED))
}
