package conveyor

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	conveyorv1 "github.com/tochemey/conveyor/internal/proto/conveyor/v1"
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

func TestWorkerRunFailsAgainstUnreachableServer(t *testing.T) {
	worker, err := NewWorker("http://127.0.0.1:1",
		WithQueues(map[string]int{"default": 1}), WithConcurrency(1))
	require.NoError(t, err)

	err = worker.Run(context.Background(), NewMux())
	require.Error(t, err)
}

func TestOutcomeForError(t *testing.T) {
	require.Equal(t, conveyorv1.TaskOutcome_TASK_OUTCOME_SUCCESS, outcomeForError(nil))
	require.Equal(t, conveyorv1.TaskOutcome_TASK_OUTCOME_SKIP_RETRY, outcomeForError(SkipRetry(errors.New("bad"))))
	require.Equal(t, conveyorv1.TaskOutcome_TASK_OUTCOME_RETRY, outcomeForError(errors.New("transient")))
}

func TestSdkVersionIsNeverEmpty(t *testing.T) {
	require.NotEmpty(t, sdkVersion())
}
