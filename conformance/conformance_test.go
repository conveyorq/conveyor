// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

//go:build conformance

// Package conformance is the cross-SDK protocol suite. It starts a real
// conveyord, then drives each SDK's conformance worker (Go, TypeScript, Python)
// through the same checklist and asserts protocol behavior by observing the
// server. It is the gate docs/protocol.md calls "the real gate": one place that
// proves every SDK honors the wire contract, so a divergence like a batch
// heartbeat that drops member ids cannot pass unnoticed.
//
// It is behind the `conformance` build tag so it never runs in the race suite
// (it spawns Node and Python processes, which that job does not provision). Run
// it with `make conformance`, which provisions those toolchains first.
package conformance

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	conveyor "github.com/conveyorq/conveyor/sdks/go"
)

const (
	// leaseTTL is short so a handler that outlives it forces the SDK to keep the
	// lease by heartbeat; a stopped heartbeat lets the reaper reclaim the task
	// and burn a retry, which the checks detect.
	leaseTTL = 2 * time.Second
	// handlerSleep is well over 2x the lease, so completing at all proves the
	// heartbeat held the lease throughout.
	handlerSleep = 5 * time.Second
	// groupSize is the aggregation-group size the server fires on; the harness
	// enqueues exactly this many grouped members.
	groupSize = 3
	// maxRetry must exceed zero so a lapsed lease reclaims as RETRY (retried++)
	// rather than archiving with the counter untouched — otherwise a dropped
	// heartbeat would leave retried at zero and escape the check.
	maxRetry = 25

	readyTimeout = 30 * time.Second
	bootTimeout  = 20 * time.Second
	checkTimeout = 45 * time.Second
)

// TestConformance runs the checklist against every SDK whose toolchain is
// available on this machine (an SDK whose runtime is missing is skipped, not
// failed, so a partial local run is still useful; CI provisions all three).
func TestConformance(t *testing.T) {
	root := repoRoot(t)
	conveyord := buildBinary(t, root, "./cmd/conveyord", "conveyord")
	server := startServer(t, root, conveyord)

	client, err := conveyor.NewClient(server.baseURL)
	require.NoError(t, err)

	server.waitReady(t, client)

	for _, sdk := range availableSDKs(t, root) {
		t.Run(sdk.name, func(t *testing.T) {
			t.Run("BadHelloRejectedLocally", func(t *testing.T) { checkBadHello(t, sdk, server) })
			t.Run("LongTaskKeepsLease", func(t *testing.T) { checkLongTask(t, sdk, server, client) })
			t.Run("BatchKeepsLeases", func(t *testing.T) { checkBatch(t, sdk, server, client) })
			t.Run("DrainReleasesWithoutRetry", func(t *testing.T) { checkDrain(t, sdk, server, client) })
		})
	}
}

// sdk is one SDK's conformance worker: a name and a factory for its process.
type sdk struct {
	// name identifies the SDK ("go", "typescript", "python").
	name string
	// command builds the (unstarted) worker process for the given scenario env,
	// merged over the base environment.
	command func(env map[string]string) *exec.Cmd
}

// availableSDKs returns a worker launcher for each SDK whose toolchain is
// present. The Go worker is always built; TypeScript needs its example's
// node_modules (a pnpm install), Python needs the SDK virtualenv.
func availableSDKs(t *testing.T, root string) []sdk {
	t.Helper()

	sdks := []sdk{goSDK(t, root)}

	if bin := tsxBinary(root); bin != "" {
		sdks = append(sdks, typescriptSDK(root, bin))
	} else {
		t.Log("typescript conformance worker skipped: examples/typescript/node_modules/.bin/tsx not found (run `pnpm install` there)")
	}

	if python := venvPython(root); python != "" {
		sdks = append(sdks, pythonSDK(root, python))
	} else {
		t.Log("python conformance worker skipped: sdks/python/.venv not found (run `make sdk-py-test` once)")
	}

	return sdks
}

// goSDK builds the Go conformance worker binary and returns its launcher.
func goSDK(t *testing.T, root string) sdk {
	t.Helper()

	binary := buildBinary(t, root, "./conformance/goworker", "goworker")

	return sdk{
		name:    "go",
		command: func(env map[string]string) *exec.Cmd { return command(binary, nil, env) },
	}
}

// typescriptSDK returns the TypeScript worker launcher, run through tsx directly
// (not pnpm) so a SIGTERM reaches the Node process rather than a wrapper.
func typescriptSDK(root, tsx string) sdk {
	dir := filepath.Join(root, "examples", "typescript")

	return sdk{
		name: "typescript",
		command: func(env map[string]string) *exec.Cmd {
			cmd := command(tsx, []string{"src/conformance.ts"}, env)
			cmd.Dir = dir

			return cmd
		},
	}
}

// pythonSDK returns the Python worker launcher, run with the SDK virtualenv's
// interpreter so conveyorq is importable.
func pythonSDK(root, python string) sdk {
	script := filepath.Join(root, "examples", "python", "conformance.py")

	return sdk{
		name: "python",
		command: func(env map[string]string) *exec.Cmd {
			return command(python, []string{script}, env)
		},
	}
}

// tsxBinary returns the example's tsx binary path, or "" if it is not installed.
func tsxBinary(root string) string {
	bin := filepath.Join(root, "examples", "typescript", "node_modules", ".bin", "tsx")
	if _, err := os.Stat(bin); err != nil {
		return ""
	}

	return bin
}

// venvPython returns the Python SDK virtualenv interpreter, or "" if absent.
func venvPython(root string) string {
	python := filepath.Join(root, "sdks", "python", ".venv", "bin", "python")
	if _, err := os.Stat(python); err != nil {
		return ""
	}

	return python
}

// checkBadHello asserts an invalid Hello (a zero queue weight) is rejected
// locally: the worker exits non-zero promptly rather than looping on reconnect.
func checkBadHello(t *testing.T, sdk sdk, server *conformanceServer) {
	cmd := sdk.command(map[string]string{
		"CONVEYOR_ADDR": server.baseURL,
		"SCENARIO":      "bad-hello",
		"QUEUE":         queueName(sdk, "badhello"),
	})

	err := runWithin(cmd, bootTimeout)
	require.Error(t, err, "an invalid Hello must be rejected locally, not retried forever")

	var exitErr *exec.ExitError
	require.ErrorAs(t, err, &exitErr, "the worker must exit with a non-zero status")
}

// checkLongTask asserts a single task whose handler outlives the lease still
// completes exactly once with no retry — proving heartbeats extend its lease.
func checkLongTask(t *testing.T, sdk sdk, server *conformanceServer, client *conveyor.Client) {
	queue := queueName(sdk, "longtask")
	stop := startWorker(t, sdk, server, queue)
	defer stop()

	id := enqueue(t, client, queue, "conformance:task")
	info := waitForState(t, client, id, conveyor.TaskStateCompleted, checkTimeout)
	require.Zero(t, info.Retried, "a task outliving the lease must keep it via heartbeats, not be reaped and retried")
}

// checkBatch asserts every member of a fired aggregation group whose batch
// handler outlives the lease completes with no retry — the regression B1 caused
// (TypeScript batch heartbeats that carried the group key, not the member ids,
// so member leases lapsed and the batch was reaped and retried).
func checkBatch(t *testing.T, sdk sdk, server *conformanceServer, client *conveyor.Client) {
	queue := queueName(sdk, "batch")
	stop := startWorker(t, sdk, server, queue)
	defer stop()

	ids := make([]string, 0, groupSize)
	for range groupSize {
		ids = append(ids, enqueue(t, client, queue, "conformance:batch", conveyor.Group("g")))
	}

	for _, id := range ids {
		info := waitForState(t, client, id, conveyor.TaskStateCompleted, checkTimeout)
		require.Zerof(t, info.Retried, "batch member %s outlived the lease but was reaped and retried: heartbeats dropped its member id", id)
	}
}

// checkDrain asserts a task in flight when the worker is signaled to drain does
// not burn a retry: the SDK keeps heartbeating through the grace window (so the
// lease never lapses), then the task completes or is released, retried at zero.
func checkDrain(t *testing.T, sdk sdk, server *conformanceServer, client *conveyor.Client) {
	queue := queueName(sdk, "drain")
	stop := startWorker(t, sdk, server, queue)

	id := enqueue(t, client, queue, "conformance:task")
	waitForState(t, client, id, conveyor.TaskStateActive, bootTimeout)

	// Signal drain while the handler is mid-flight.
	stop()

	info := waitUntil(t, client, id, checkTimeout, func(info *conveyor.TaskInfo) bool {
		return info.State != conveyor.TaskStateActive
	})
	require.Zerof(t, info.Retried, "a drained task must not burn a retry (ended %s); the heartbeat must continue through the drain", info.State)
}

// startWorker launches the SDK's sleep-scenario worker on the given queue and
// returns a function that signals it to drain and waits for exit.
func startWorker(t *testing.T, sdk sdk, server *conformanceServer, queue string) func() {
	t.Helper()

	cmd := sdk.command(map[string]string{
		"CONVEYOR_ADDR": server.baseURL,
		"SCENARIO":      "sleep",
		"SLEEP_MS":      strconv.Itoa(int(handlerSleep.Milliseconds())),
		"QUEUE":         queue,
	})

	require.NoError(t, cmd.Start())

	var stopped bool

	return func() {
		if stopped {
			return
		}

		stopped = true
		terminate(cmd)
	}
}

// enqueue commits one task of the given type on the queue and returns its id.
func enqueue(t *testing.T, client *conveyor.Client, queue, taskType string, opts ...conveyor.EnqueueOption) string {
	t.Helper()

	options := append([]conveyor.EnqueueOption{
		conveyor.Queue(queue),
		conveyor.MaxRetry(maxRetry),
		conveyor.Retention(time.Hour),
	}, opts...)

	info, err := client.Enqueue(context.Background(), conveyor.NewTask(taskType, conveyor.JSON(map[string]int{"n": 1})), options...)
	require.NoError(t, err)

	return info.ID
}

// waitForState polls until the task reaches want, then returns its info.
func waitForState(t *testing.T, client *conveyor.Client, id string, want conveyor.TaskState, timeout time.Duration) *conveyor.TaskInfo {
	t.Helper()

	return waitUntil(t, client, id, timeout, func(info *conveyor.TaskInfo) bool {
		return info.State == want
	})
}

// waitUntil polls the task until pred holds or the timeout elapses, then returns
// the matching info; it fails the test on timeout.
func waitUntil(t *testing.T, client *conveyor.Client, id string, timeout time.Duration, pred func(*conveyor.TaskInfo) bool) *conveyor.TaskInfo {
	t.Helper()

	deadline := time.Now().Add(timeout)

	var last *conveyor.TaskInfo

	for time.Now().Before(deadline) {
		info, err := client.GetTask(context.Background(), id)
		if err == nil {
			last = info
			if pred(info) {
				return info
			}
		}

		time.Sleep(100 * time.Millisecond)
	}

	state := conveyor.TaskState("<not found>")
	if last != nil {
		state = last.State
	}

	require.FailNowf(t, "task did not reach the expected state", "task %s stuck in %s after %s", id, state, timeout)

	return nil
}

// queueName derives a per-SDK, per-check queue so concurrent checks against the
// shared server never cross-deliver.
func queueName(sdk sdk, check string) string {
	return fmt.Sprintf("conf-%s-%s", sdk.name, check)
}
