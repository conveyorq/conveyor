package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/conveyorq/conveyor/embedded"
)

// testTimeout bounds the embedded system boot and shutdown in tests.
const testTimeout = 30 * time.Second

func TestRunRequiresCommand(t *testing.T) {
	err := run(nil, &bytes.Buffer{})
	require.ErrorContains(t, err, "a command is required")
}

func TestRunRejectsUnknownCommand(t *testing.T) {
	err := run([]string{"frobnicate"}, &bytes.Buffer{})
	require.ErrorContains(t, err, `unknown command "frobnicate"`)
}

func TestEnqueueRequiresTaskType(t *testing.T) {
	err := run([]string{"enqueue"}, &bytes.Buffer{})
	require.ErrorContains(t, err, "task type is required")

	err = run([]string{"enqueue", "--queue", "critical"}, &bytes.Buffer{})
	require.ErrorContains(t, err, "task type is required")
}

func TestEnqueueRejectsInvalidJSON(t *testing.T) {
	err := run([]string{"enqueue", "email:welcome", "--json", "{nope"}, &bytes.Buffer{})
	require.ErrorContains(t, err, "not valid JSON")
}

func TestEnqueueRejectsBadAtTimestamp(t *testing.T) {
	err := run([]string{"enqueue", "email:welcome", "--at", "tomorrow"}, &bytes.Buffer{})
	require.ErrorContains(t, err, "parsing --at")
}

func TestTasksRequiresSubcommand(t *testing.T) {
	err := run([]string{"tasks"}, &bytes.Buffer{})
	require.ErrorContains(t, err, "a subcommand is required")

	err = run([]string{"tasks", "purge"}, &bytes.Buffer{})
	require.ErrorContains(t, err, `unknown subcommand "purge"`)
}

func TestTasksGetRequiresOneID(t *testing.T) {
	err := run([]string{"tasks", "get"}, &bytes.Buffer{})
	require.ErrorContains(t, err, "exactly one task id is required")
}

func TestEnqueueAndGetAgainstEmbeddedServer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	system, err := embedded.Start(ctx, embedded.Config{})
	require.NoError(t, err)

	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), testTimeout)
		defer stopCancel()

		require.NoError(t, system.Stop(stopCtx))
	})

	addr := "http://" + system.Addr()

	var enqueueOut bytes.Buffer

	err = run([]string{
		"--addr", addr,
		"enqueue", "email:welcome",
		"--json", `{"user_id":42}`,
		"--queue", "critical",
		"--priority", "7",
		"--retention", "1h",
		"--unique", "24h",
	}, &enqueueOut)
	require.NoError(t, err)
	require.Contains(t, enqueueOut.String(), "enqueued ")
	require.Contains(t, enqueueOut.String(), "queue=critical")

	fields := strings.Fields(enqueueOut.String())
	require.GreaterOrEqual(t, len(fields), 2)
	taskID := fields[1]

	var getOut bytes.Buffer

	err = run([]string{"--addr", addr, "tasks", "get", taskID}, &getOut)
	require.NoError(t, err)
	require.Contains(t, getOut.String(), "id:          "+taskID)
	require.Contains(t, getOut.String(), "queue:       critical")
	require.Contains(t, getOut.String(), "type:        email:welcome")
	require.Contains(t, getOut.String(), "priority:    7")
}

func TestTasksGetUnknownIDFails(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	system, err := embedded.Start(ctx, embedded.Config{})
	require.NoError(t, err)

	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), testTimeout)
		defer stopCancel()

		require.NoError(t, system.Stop(stopCtx))
	})

	err = run([]string{"--addr", "http://" + system.Addr(), "tasks", "get", "no-such-task"}, &bytes.Buffer{})
	require.ErrorContains(t, err, "not found")
}

func TestFirstNonEmpty(t *testing.T) {
	require.Equal(t, "flag", firstNonEmpty("flag", "env", "default"))
	require.Equal(t, "env", firstNonEmpty("", "env", "default"))
	require.Equal(t, "default", firstNonEmpty("", "", "default"))
	require.Empty(t, firstNonEmpty("", ""))
}

func TestFormatTime(t *testing.T) {
	require.Equal(t, "-", formatTime(time.Time{}))

	stamped := time.Date(2026, 6, 12, 12, 0, 0, 0, time.UTC)
	require.NotEqual(t, "-", formatTime(stamped))
}

func TestOrDash(t *testing.T) {
	require.Equal(t, "-", orDash(""))
	require.Equal(t, "boom", orDash("boom"))
}
