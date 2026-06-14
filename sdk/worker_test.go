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

package conveyor

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
	"github.com/conveyorq/conveyor/internal/proto/conveyor/v1/conveyorv1connect"
)

func TestNewWorkerValidation(t *testing.T) {
	_, err := NewWorker("")
	require.ErrorContains(t, err, "base URL is required")

	_, err = NewWorker("http://127.0.0.1:1", WithConcurrency(4))
	require.ErrorContains(t, err, "at least one queue is required")

	_, err = NewWorker("http://127.0.0.1:1", WithQueues(map[string]int{"": 1}), WithConcurrency(4))
	require.ErrorContains(t, err, "queue names must not be empty")

	_, err = NewWorker("http://127.0.0.1:1", WithQueues(map[string]int{"default": 0}), WithConcurrency(4))
	require.ErrorContains(t, err, "weight must be positive")

	_, err = NewWorker("http://127.0.0.1:1", WithQueues(map[string]int{"default": 1}))
	require.ErrorContains(t, err, "concurrency must be positive")
}

func TestWorkerRunRequiresMux(t *testing.T) {
	worker, err := NewWorker("http://127.0.0.1:1",
		WithQueues(map[string]int{"default": 1}), WithConcurrency(1))
	require.NoError(t, err)

	require.ErrorContains(t, worker.Run(context.Background(), nil), "mux is required")
}

// TestWorkerProcessesTasksEndToEnd drives the full public surface: a
// client enqueues, a worker session executes, and every outcome lands in
// its durable state.
func TestWorkerProcessesTasksEndToEnd(t *testing.T) {
	baseURL := startTestServer(t, nil)

	client, err := NewClient(baseURL)
	require.NoError(t, err)

	worker, err := NewWorker(baseURL,
		WithQueues(map[string]int{"default": 1}), WithConcurrency(4))
	require.NoError(t, err)

	type welcomePayload struct {
		UserID int `json:"user_id"`
	}

	var handledMutex sync.Mutex
	handled := make(map[int]bool)

	mux := NewMux()

	mux.HandleFunc("test:ok", func(_ context.Context, task *Task) error {
		var payload welcomePayload
		if err := task.Bind(&payload); err != nil {
			return SkipRetry(err)
		}

		handledMutex.Lock()
		handled[payload.UserID] = true
		handledMutex.Unlock()

		return nil
	})

	mux.HandleFunc("test:skip", func(context.Context, *Task) error {
		return SkipRetry(errors.New("malformed input"))
	})

	mux.HandleFunc("test:flaky", func(_ context.Context, task *Task) error {
		if task.Retried() == 0 {
			return errors.New("transient failure")
		}

		return nil
	})

	runCtx, stopWorker := context.WithCancel(context.Background())
	defer stopWorker()

	runDone := make(chan error, 1)

	go func() { runDone <- worker.Run(runCtx, mux) }()

	ctx := context.Background()

	var okIDs []string

	for userID := range 5 {
		info, err := client.Enqueue(ctx, NewTask("test:ok", JSON(welcomePayload{UserID: userID})),
			Retention(time.Hour))
		require.NoError(t, err)

		okIDs = append(okIDs, info.ID)
	}

	skipped, err := client.Enqueue(ctx, NewTask("test:skip", JSON("x")), Retention(time.Hour))
	require.NoError(t, err)

	flaky, err := client.Enqueue(ctx, NewTask("test:flaky", JSON("x")), Retention(time.Hour))
	require.NoError(t, err)

	unhandled, err := client.Enqueue(ctx, NewTask("test:unknown", JSON("x")),
		MaxRetry(1), Retention(time.Hour))
	require.NoError(t, err)

	for _, id := range okIDs {
		awaitTaskState(t, client, id, TaskStateCompleted)
	}

	awaitTaskState(t, client, skipped.ID, TaskStateArchived)
	awaitTaskState(t, client, flaky.ID, TaskStateCompleted)
	awaitTaskState(t, client, unhandled.ID, TaskStateArchived)

	handledMutex.Lock()
	require.Len(t, handled, 5)
	handledMutex.Unlock()

	flakyInfo, err := client.GetTask(ctx, flaky.ID)
	require.NoError(t, err)
	require.Equal(t, 1, flakyInfo.Retried, "flaky task should have exactly one retry")

	stopWorker()

	select {
	case err := <-runDone:
		require.NoError(t, err, "context cancellation is a clean shutdown")

	case <-time.After(10 * time.Second):
		t.Fatal("worker did not stop on context cancellation")
	}
}

// TestWorkerStopsOnContextCancel guards the duplex-stream shutdown path:
// canceling Run's context must end an idle session promptly even though
// the transport itself stops watching the context once the stream is up.
func TestWorkerStopsOnContextCancel(t *testing.T) {
	baseURL := startTestServer(t, nil)

	worker, err := NewWorker(baseURL,
		WithQueues(map[string]int{"default": 1}), WithConcurrency(1))
	require.NoError(t, err)

	runCtx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)

	go func() { runDone <- worker.Run(runCtx, NewMux()) }()

	// Let the session establish before canceling.
	time.Sleep(time.Second)
	cancel()

	select {
	case err := <-runDone:
		require.NoError(t, err)

	case <-time.After(10 * time.Second):
		t.Fatal("worker did not stop on context cancellation")
	}
}

// TestWorkerRetriesUnreachableServerUntilCanceled pins the reconnect
// contract: connection failures are retried with backoff instead of ending
// Run, and cancellation is still a clean nil shutdown.
func TestWorkerRetriesUnreachableServerUntilCanceled(t *testing.T) {
	worker, err := NewWorker("http://127.0.0.1:1",
		WithQueues(map[string]int{"default": 1}), WithConcurrency(1))
	require.NoError(t, err)

	runCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	require.NoError(t, worker.Run(runCtx, NewMux()))
}

// TestWorkerRunFailsFastOnBadToken pins the one reconnect exception: a
// rejected token never heals, so Run must return instead of retrying.
func TestWorkerRunFailsFastOnBadToken(t *testing.T) {
	baseURL := startTestServer(t, []string{"secret"})

	worker, err := NewWorker(baseURL,
		WithToken("wrong"),
		WithQueues(map[string]int{"default": 1}), WithConcurrency(1))
	require.NoError(t, err)

	runDone := make(chan error, 1)

	go func() { runDone <- worker.Run(context.Background(), NewMux()) }()

	select {
	case err := <-runDone:
		require.Error(t, err)

	case <-time.After(10 * time.Second):
		t.Fatal("worker kept retrying an unauthenticated session")
	}
}

// rejectingWorkerService refuses every session the way a server gates an
// outdated SDK version: a permanent InvalidArgument rejection.
type rejectingWorkerService struct{}

// Session implements conveyorv1connect.WorkerServiceHandler.
func (rejectingWorkerService) Session(_ context.Context, _ *connect.BidiStream[conveyorv1.WorkerMessage, conveyorv1.ServerMessage]) error {
	return connect.NewError(connect.CodeInvalidArgument, errors.New("sdk version v0.0.1 is no longer supported"))
}

// TestWorkerRunFailsFastOnRejectedSession pins the second reconnect
// exception: a session contract the server permanently refuses (outdated
// SDK version, malformed Hello) never heals with the same binary, so Run
// must surface the error instead of silently retrying forever.
func TestWorkerRunFailsFastOnRejectedSession(t *testing.T) {
	mux := http.NewServeMux()
	mux.Handle(conveyorv1connect.NewWorkerServiceHandler(rejectingWorkerService{}))

	server := httptest.NewUnstartedServer(mux)

	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	protocols.SetHTTP2(true)
	protocols.SetUnencryptedHTTP2(true)

	server.Config.Protocols = protocols
	server.Start()
	t.Cleanup(server.Close)

	worker, err := NewWorker(server.URL,
		WithQueues(map[string]int{"default": 1}), WithConcurrency(1))
	require.NoError(t, err)

	runDone := make(chan error, 1)

	go func() { runDone <- worker.Run(context.Background(), NewMux()) }()

	select {
	case err := <-runDone:
		require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

	case <-time.After(10 * time.Second):
		t.Fatal("worker kept retrying a permanently rejected session")
	}
}

// TestWorkerReconnectsAfterStreamDrop drives the reconnect loop end to
// end: the worker's connection is severed mid-session and the same Run
// call must establish a new session and process new work unattended. A
// TCP proxy stands between worker and server so the test can cut the wire
// without restarting the server.
func TestWorkerReconnectsAfterStreamDrop(t *testing.T) {
	baseURL := startTestServer(t, nil)
	proxy := newDroppingProxy(t, strings.TrimPrefix(baseURL, "http://"))

	worker, err := NewWorker("http://"+proxy.addr(),
		WithQueues(map[string]int{"default": 1}), WithConcurrency(1))
	require.NoError(t, err)

	var processed atomic.Int64

	mux := NewMux()

	mux.HandleFunc("test:ok", func(context.Context, *Task) error {
		processed.Add(1)

		return nil
	})

	runCtx, stopWorker := context.WithCancel(context.Background())
	defer stopWorker()

	runDone := make(chan error, 1)

	go func() { runDone <- worker.Run(runCtx, mux) }()

	ctx := context.Background()

	client, err := NewClient(baseURL)
	require.NoError(t, err)

	info, err := client.Enqueue(ctx, NewTask("test:ok", JSON("x")), Retention(time.Hour))
	require.NoError(t, err)

	awaitTaskState(t, client, info.ID, TaskStateCompleted)

	proxy.dropAll()

	second, err := client.Enqueue(ctx, NewTask("test:ok", JSON("y")), Retention(time.Hour))
	require.NoError(t, err)

	awaitTaskState(t, client, second.ID, TaskStateCompleted)
	require.GreaterOrEqual(t, processed.Load(), int64(2))

	stopWorker()
	require.NoError(t, <-runDone)
}

// TestWorkerSurvivesPanickingHandler pins the panic recovery contract: a
// panicking handler is reported as a retryable failure and the worker
// keeps processing.
func TestWorkerSurvivesPanickingHandler(t *testing.T) {
	baseURL := startTestServer(t, nil)

	client, err := NewClient(baseURL)
	require.NoError(t, err)

	worker, err := NewWorker(baseURL,
		WithQueues(map[string]int{"default": 1}), WithConcurrency(1))
	require.NoError(t, err)

	mux := NewMux()

	mux.HandleFunc("test:panic", func(_ context.Context, task *Task) error {
		if task.Retried() == 0 {
			panic("boom")
		}

		return nil
	})

	runCtx, stopWorker := context.WithCancel(context.Background())
	defer stopWorker()

	runDone := make(chan error, 1)

	go func() { runDone <- worker.Run(runCtx, mux) }()

	info, err := client.Enqueue(context.Background(), NewTask("test:panic", JSON("x")), Retention(time.Hour))
	require.NoError(t, err)

	awaitTaskState(t, client, info.ID, TaskStateCompleted)

	final, err := client.GetTask(context.Background(), info.ID)
	require.NoError(t, err)
	require.Equal(t, 1, final.Retried, "the panic must count as one retryable failure")

	stopWorker()
	require.NoError(t, <-runDone)
}

func TestInvokeRecoversPanicsWithStack(t *testing.T) {
	handler := func(context.Context, *Task) error { panic("kaboom") }

	err := invoke(context.Background(), handler, &Task{})
	require.ErrorContains(t, err, "handler panic: kaboom")
	require.ErrorContains(t, err, "worker_test.go", "the error must carry the stack")
	require.False(t, IsSkipRetry(err), "panics are retryable failures")
}

// TestHandlerContextCarriesTaskValues drives the context helpers through a
// real dispatch.
func TestHandlerContextCarriesTaskValues(t *testing.T) {
	baseURL := startTestServer(t, nil)

	client, err := NewClient(baseURL)
	require.NoError(t, err)

	worker, err := NewWorker(baseURL,
		WithQueues(map[string]int{"default": 1}), WithConcurrency(1))
	require.NoError(t, err)

	type seenValues struct {
		id       string
		retries  int
		budget   int
		idOK     bool
		retryOK  bool
		budgetOK bool
	}

	seen := make(chan seenValues, 1)

	mux := NewMux()

	mux.HandleFunc("test:ctx", func(ctx context.Context, _ *Task) error {
		var values seenValues
		values.id, values.idOK = GetTaskID(ctx)
		values.retries, values.retryOK = GetRetryCount(ctx)
		values.budget, values.budgetOK = GetMaxRetry(ctx)
		seen <- values

		return nil
	})

	runCtx, stopWorker := context.WithCancel(context.Background())
	defer stopWorker()

	runDone := make(chan error, 1)

	go func() { runDone <- worker.Run(runCtx, mux) }()

	info, err := client.Enqueue(context.Background(), NewTask("test:ctx", JSON("x")),
		MaxRetry(7), Retention(time.Hour))
	require.NoError(t, err)

	select {
	case values := <-seen:
		require.True(t, values.idOK)
		require.True(t, values.retryOK)
		require.True(t, values.budgetOK)
		require.Equal(t, info.ID, values.id)
		require.Equal(t, 0, values.retries)
		require.Equal(t, 7, values.budget)

	case <-time.After(30 * time.Second):
		t.Fatal("handler never ran")
	}

	stopWorker()
	require.NoError(t, <-runDone)
}

func TestOutcomeForError(t *testing.T) {
	require.Equal(t, conveyorv1.TaskOutcome_TASK_OUTCOME_SUCCESS, outcomeForError(nil))
	require.Equal(t, conveyorv1.TaskOutcome_TASK_OUTCOME_SKIP_RETRY, outcomeForError(SkipRetry(errors.New("bad"))))
	require.Equal(t, conveyorv1.TaskOutcome_TASK_OUTCOME_RETRY, outcomeForError(errors.New("transient")))
}

func TestSdkVersionIsNeverEmpty(t *testing.T) {
	require.NotEmpty(t, sdkVersion())
}
