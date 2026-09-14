// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

//go:build conformance

package conformance

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	conveyor "github.com/conveyorq/conveyor/sdks/go"
)

// errWorkerTimeout marks a worker process that did not exit within its window.
var errWorkerTimeout = errors.New("conformance: worker did not exit in time")

// conformanceServer is a running conveyord subprocess for the suite.
type conformanceServer struct {
	// baseURL is the API endpoint the SDK workers and client connect to.
	baseURL string
	// log captures the server's stdout and stderr for diagnostics on failure.
	log *bytes.Buffer
}

// repoRoot returns the module root, resolved from this test file's location.
func repoRoot(t *testing.T) string {
	t.Helper()

	_, file, _, ok := runtime.Caller(0)
	require.True(t, ok, "resolving the test file path")

	return filepath.Dir(filepath.Dir(file))
}

// buildBinary compiles a package to a temp binary and returns its path.
func buildBinary(t *testing.T, root, pkg, name string) string {
	t.Helper()

	binary := filepath.Join(t.TempDir(), name)

	build := exec.Command("go", "build", "-o", binary, pkg)
	build.Dir = root

	output, err := build.CombinedOutput()
	require.NoErrorf(t, err, "building %s: %s", pkg, output)

	return binary
}

// startServer boots conveyord in dev mode on free ports, tuned for fast leases
// and small aggregation groups so the checks run in seconds, and stops it with
// the test.
func startServer(t *testing.T, root, conveyord string) *conformanceServer {
	t.Helper()

	apiPort := freePort(t)
	metricsPort := freePort(t)

	server := &conformanceServer{
		baseURL: fmt.Sprintf("http://127.0.0.1:%d", apiPort),
		log:     new(bytes.Buffer),
	}

	cmd := command(conveyord, []string{"--dev"}, map[string]string{
		"CONVEYOR_API__LISTEN":                  fmt.Sprintf("127.0.0.1:%d", apiPort),
		"CONVEYOR_METRICS__LISTEN":              fmt.Sprintf("127.0.0.1:%d", metricsPort),
		"CONVEYOR_ENGINE__LEASE_TTL":            leaseTTL.String(),
		"CONVEYOR_ENGINE__REAP_INTERVAL":        "500ms",
		"CONVEYOR_ENGINE__PROMOTE_INTERVAL":     "300ms",
		"CONVEYOR_ENGINE__GROUP_MAX_SIZE":       fmt.Sprintf("%d", groupSize),
		"CONVEYOR_ENGINE__GROUP_GRACE_PERIOD":   "500ms",
		"CONVEYOR_ENGINE__GROUP_SWEEP_INTERVAL": "300ms",
	})
	cmd.Stdout = server.log
	cmd.Stderr = server.log

	require.NoError(t, cmd.Start())
	t.Cleanup(func() { terminate(cmd) })

	// A failed check is only diagnosable with the server's side of the story,
	// so its log is printed then; a passing run stays quiet.
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("server log:\n%s", server.log.String())
		}
	})

	return server
}

// waitReady blocks until the server answers an RPC (a not-found is a healthy
// reply), failing the test with the server log if it never comes up.
func (s *conformanceServer) waitReady(t *testing.T, client *conveyor.Client) {
	t.Helper()

	deadline := time.Now().Add(readyTimeout)
	for time.Now().Before(deadline) {
		_, err := client.GetTask(context.Background(), "readiness-probe")
		if err == nil || errors.Is(err, conveyor.ErrTaskNotFound) {
			return
		}

		time.Sleep(200 * time.Millisecond)
	}

	require.FailNowf(t, "server did not become ready", "server log:\n%s", s.log.String())
}

// command builds an unstarted process with the base environment plus env, in
// its own process group so a drain signal can reach the whole worker tree, with
// output surfaced for diagnostics.
func command(bin string, args []string, env map[string]string) *exec.Cmd {
	cmd := exec.Command(bin, args...)
	cmd.Env = os.Environ()

	for key, value := range env {
		cmd.Env = append(cmd.Env, key+"="+value)
	}

	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	return cmd
}

// runWithin starts the process and waits up to timeout for it to exit, returning
// its exit error, or errWorkerTimeout (after killing it) if it hangs. It owns
// the sole Wait for the process.
func runWithin(cmd *exec.Cmd, timeout time.Duration) error {
	if err := cmd.Start(); err != nil {
		return err
	}

	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()

	select {
	case err := <-done:
		return err

	case <-time.After(timeout):
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)

		select {
		case <-done:
		case <-time.After(5 * time.Second):
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
			<-done
		}

		return errWorkerTimeout
	}
}

// terminate signals the process group to drain (SIGTERM), then hard-kills it if
// it lingers, so no worker outlives the test. It owns the sole Wait for the
// process, so it is used only for processes not waited elsewhere.
func terminate(cmd *exec.Cmd) {
	if cmd.Process == nil {
		return
	}

	// Negative pid signals the whole process group set up in command.
	_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)

	done := make(chan struct{})
	go func() {
		_ = cmd.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		<-done
	}
}

// freePort reserves and releases a loopback TCP port, returning its number.
func freePort(t *testing.T) int {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	port := listener.Addr().(*net.TCPAddr).Port
	require.NoError(t, listener.Close())

	return port
}
