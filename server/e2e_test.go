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

package server

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/conveyorq/conveyor/internal/dynaport"
	conveyor "github.com/conveyorq/conveyor/sdk"
)

// e2eTaskCount is the workload size of the worker-kill end-to-end test.
const e2eTaskCount = 60

// freePorts reserves n distinct free loopback ports.
func freePorts(t *testing.T, n int) []int {
	t.Helper()

	ports, err := dynaport.Get(n)
	require.NoError(t, err)

	return ports
}

// startNode boots a dev-mode node on ephemeral ports and returns its base URL.
func startNode(t *testing.T) string {
	t.Helper()

	ports := freePorts(t, 3)

	config := DevConfig()
	config.API.Listen = "127.0.0.1:0"
	config.Metrics.Listen = "127.0.0.1:0"
	config.Cluster.RemotingPort = ports[0]
	config.Cluster.DiscoveryPort = ports[1]
	config.Cluster.PeersPort = ports[2]

	node, err := New(config, NewLogger(LogConfig{Level: LogLevelError, Format: LogFormatText}))
	require.NoError(t, err)
	require.NoError(t, node.Start(context.Background()))

	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = node.Stop(stopCtx)
	})

	return "http://" + node.Addr()
}

// buildExampleWorker compiles the standalone example worker and returns
// the binary path.
func buildExampleWorker(t *testing.T) string {
	t.Helper()

	binary := filepath.Join(t.TempDir(), "example-worker")

	build := exec.Command("go", "build", "-o", binary, "github.com/conveyorq/conveyor/examples/standalone/worker")
	build.Dir = ".."

	output, err := build.CombinedOutput()
	require.NoError(t, err, "building example worker: %s", output)

	return binary
}

// startWorkerProcess launches the example worker against the given server.
func startWorkerProcess(t *testing.T, binary, baseURL string) *exec.Cmd {
	t.Helper()

	command := exec.Command(binary)
	command.Env = append(os.Environ(), "CONVEYOR_ADDR="+baseURL)
	command.Stdout = os.Stderr
	command.Stderr = os.Stderr

	require.NoError(t, command.Start())

	t.Cleanup(func() {
		if command.Process != nil {
			_ = command.Process.Kill()
		}

		_ = command.Wait()
	})

	return command
}

// TestExampleWorkerEndToEndWithSIGKILL is the wire-protocol acceptance
// test: the standalone example app (a separate worker process) processes
// tasks end-to-end, survives a SIGKILL mid-load through release-on-
// disconnect, and no task pays a retry penalty for the crash. With the
// default 60s lease TTL, completion within the test window is only
// possible if the dead session's tasks were released on disconnect rather
// than waiting for lease expiry.
func TestExampleWorkerEndToEndWithSIGKILL(t *testing.T) {
	if testing.Short() {
		t.Skip("subprocess end-to-end test skipped in -short mode")
	}

	baseURL := startNode(t)
	binary := buildExampleWorker(t)

	client, err := conveyor.NewClient(baseURL)
	require.NoError(t, err)

	ctx := context.Background()

	taskIDs := make([]string, 0, e2eTaskCount)

	for userID := range e2eTaskCount {
		payload := conveyor.JSON(map[string]int{"user_id": userID})

		info, err := client.Enqueue(ctx, conveyor.NewTask("email:welcome", payload),
			conveyor.Retention(time.Hour))
		require.NoError(t, err)

		taskIDs = append(taskIDs, info.ID)
	}

	doomed := startWorkerProcess(t, binary, baseURL)

	// Let the worker make partial progress before the kill.
	require.Eventually(t, func() bool {
		return countCompleted(t, client, taskIDs) >= 10
	}, time.Minute, 50*time.Millisecond, "worker should make progress before the kill")

	require.NoError(t, doomed.Process.Signal(syscall.SIGKILL))

	// A replacement worker picks the released work up immediately.
	startWorkerProcess(t, binary, baseURL)

	require.Eventually(t, func() bool {
		return countCompleted(t, client, taskIDs) == e2eTaskCount
	}, time.Minute, 50*time.Millisecond, "all tasks must complete after the kill")

	for _, id := range taskIDs {
		info, err := client.GetTask(ctx, id)
		require.NoError(t, err)
		require.Zerof(t, info.Retried, "task %s must not pay a retry penalty for the crash", id)
	}
}

// countCompleted reports how many of the given tasks are completed.
func countCompleted(t *testing.T, client *conveyor.Client, taskIDs []string) int {
	t.Helper()

	completed := 0

	for _, id := range taskIDs {
		info, err := client.GetTask(context.Background(), id)
		if err != nil {
			continue
		}

		if info.State == conveyor.TaskStateCompleted {
			completed++
		}
	}

	return completed
}
