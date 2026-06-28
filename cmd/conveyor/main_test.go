// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/conveyorq/conveyor/embedded"
	conveyor "github.com/conveyorq/conveyor/sdks/go"
)

// testSecret is a deterministic 32-byte AES-256 key for the encryption tests,
// base64-encoded as the --encryption-key flag expects.
var testSecret = base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{0x7}, 32))

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

func TestEnqueueRejectsBadExpiresAtTimestamp(t *testing.T) {
	err := run([]string{"enqueue", "email:welcome", "--expires-at", "soon"}, &bytes.Buffer{})
	require.ErrorContains(t, err, "parsing --expires-at")
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

func TestReadTxTasksValidation(t *testing.T) {
	_, err := readTxTasks("")
	require.ErrorContains(t, err, "--file is required")

	dir := t.TempDir()

	badJSON := filepath.Join(dir, "bad.json")
	require.NoError(t, os.WriteFile(badJSON, []byte("{nope"), 0o600))
	_, err = readTxTasks(badJSON)
	require.ErrorContains(t, err, "JSON array")

	empty := filepath.Join(dir, "empty.json")
	require.NoError(t, os.WriteFile(empty, []byte("[]"), 0o600))
	_, err = readTxTasks(empty)
	require.ErrorContains(t, err, "task list is empty")

	noType := filepath.Join(dir, "notype.json")
	require.NoError(t, os.WriteFile(noType, []byte(`[{"json":{"a":1}}]`), 0o600))
	_, err = readTxTasks(noType)
	require.ErrorContains(t, err, "task 0: a type is required")

	badDur := filepath.Join(dir, "baddur.json")
	require.NoError(t, os.WriteFile(badDur, []byte(`[{"type":"t","in":"soon"}]`), 0o600))
	_, err = readTxTasks(badDur)
	require.ErrorContains(t, err, `task 0: parsing "in"`)

	ok := filepath.Join(dir, "ok.json")
	require.NoError(t, os.WriteFile(ok, []byte(`[{"type":"a","queue":"q1"},{"type":"b","priority":7}]`), 0o600))
	tasks, err := readTxTasks(ok)
	require.NoError(t, err)
	require.Len(t, tasks, 2)
}

func TestEnqueueTxAgainstEmbeddedServer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	system, err := embedded.Start(ctx, embedded.Config{})
	require.NoError(t, err)

	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), testTimeout)
		defer stopCancel()

		require.NoError(t, system.Stop(stopCtx))
	})

	file := filepath.Join(t.TempDir(), "tasks.json")
	require.NoError(t, os.WriteFile(file, []byte(`[
		{"type": "order:charge", "queue": "billing", "json": {"id": "order-42"}, "priority": 7},
		{"type": "email:receipt", "queue": "mail", "json": {"id": "order-42"}}
	]`), 0o600))

	var out bytes.Buffer

	err = run([]string{"--addr", "http://" + system.Addr(), "enqueue-tx", "--file", file}, &out)
	require.NoError(t, err)
	require.Equal(t, 2, strings.Count(out.String(), "enqueued "))
	require.Contains(t, out.String(), "queue=billing")
	require.Contains(t, out.String(), "queue=mail")
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

func TestBuildEncryptorWithoutKeyIsNil(t *testing.T) {
	t.Setenv(envEncryptionKey, "")

	encryptor, err := buildEncryptor("")
	require.NoError(t, err)
	require.Nil(t, encryptor)
}

func TestBuildEncryptorFromFlag(t *testing.T) {
	encryptor, err := buildEncryptor("k1:" + testSecret)
	require.NoError(t, err)
	require.NotNil(t, encryptor)
}

func TestBuildEncryptorFromEnv(t *testing.T) {
	t.Setenv(envEncryptionKey, "k1:"+testSecret)

	encryptor, err := buildEncryptor("")
	require.NoError(t, err)
	require.NotNil(t, encryptor)
}

func TestBuildEncryptorFlagBeatsEnv(t *testing.T) {
	t.Setenv(envEncryptionKey, "ignored:"+testSecret)

	encryptor, err := buildEncryptor("k1:" + testSecret)
	require.NoError(t, err)
	require.NotNil(t, encryptor)
}

func TestBuildEncryptorRejectsMalformedKey(t *testing.T) {
	for name, key := range map[string]string{
		"no separator": "justthekey",
		"empty id":     ":" + testSecret,
		"empty secret": "k1:",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := buildEncryptor(key)
			require.ErrorContains(t, err, `"<id>:<base64-secret>"`)
		})
	}
}

func TestBuildEncryptorRejectsBadBase64(t *testing.T) {
	_, err := buildEncryptor("k1:not-base-64!!")
	require.ErrorContains(t, err, "decoding --encryption-key secret")
}

func TestBuildEncryptorRejectsWrongLengthSecret(t *testing.T) {
	short := base64.StdEncoding.EncodeToString([]byte("too-short"))

	_, err := buildEncryptor("k1:" + short)
	require.ErrorContains(t, err, "secret")
}

// TestEnqueueWithEncryptionRoundTripsToWorker proves the CLI knob end to end: a
// task enqueued with --encryption-key is sealed before it leaves the CLI, and a
// worker holding the same key opens it and sees the original plaintext.
func TestEnqueueWithEncryptionRoundTripsToWorker(t *testing.T) {
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

	encryptor, err := buildEncryptor("k1:" + testSecret)
	require.NoError(t, err)

	delivered := make(chan []byte, 1)

	mux := conveyor.NewMux()
	mux.HandleFunc("secret:task", func(_ context.Context, task *conveyor.Task) error {
		delivered <- task.Payload()

		return nil
	})

	worker, err := conveyor.NewWorker(addr,
		conveyor.WithQueues(map[string]int{"default": 1}),
		conveyor.WithConcurrency(1),
		conveyor.WithEncryption(encryptor))
	require.NoError(t, err)

	runCtx, stopWorker := context.WithCancel(ctx)
	workerDone := make(chan error, 1)

	go func() { workerDone <- worker.Run(runCtx, mux) }()

	t.Cleanup(func() {
		stopWorker()
		<-workerDone
	})

	err = run([]string{
		"--addr", addr,
		"enqueue", "secret:task",
		"--json", `{"secret":"launch-code"}`,
		"--encryption-key", "k1:" + testSecret,
	}, &bytes.Buffer{})
	require.NoError(t, err)

	select {
	case payload := <-delivered:
		require.JSONEq(t, `{"secret":"launch-code"}`, string(payload))

	case <-time.After(testTimeout):
		t.Fatal("worker did not receive the task")
	}
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

func TestBuildRetryPolicy(t *testing.T) {
	// No flags set yields no option.
	option, err := buildRetryPolicy("", 0, 0)
	require.NoError(t, err)
	require.Nil(t, option)

	// Every known strategy maps without error.
	for _, strategy := range []string{"default", "exponential", "linear", "fixed"} {
		option, err := buildRetryPolicy(strategy, time.Minute, 5*time.Minute)
		require.NoError(t, err, strategy)
		require.NotNil(t, option, strategy)
	}

	// A timing-only override (no strategy) is accepted.
	option, err = buildRetryPolicy("", time.Minute, 0)
	require.NoError(t, err)
	require.NotNil(t, option)

	// An unknown strategy is rejected.
	_, err = buildRetryPolicy("quadratic", 0, 0)
	require.ErrorContains(t, err, "retry-strategy")
}
