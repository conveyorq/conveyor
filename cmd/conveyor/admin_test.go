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

	// The CLI cannot create cron entries yet (Phase 6); list must succeed
	// and render only the header on an empty system.
	var cronOut bytes.Buffer

	require.NoError(t, run([]string{"--addr", addr, "cron", "list"}, &cronOut))
	require.Contains(t, cronOut.String(), "ID")

	err := run([]string{"--addr", addr, "cron", "pause", "missing"}, &bytes.Buffer{})
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
