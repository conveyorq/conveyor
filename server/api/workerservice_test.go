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

package api

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	"github.com/conveyorq/conveyor/internal/clock"
	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
	"github.com/conveyorq/conveyor/internal/proto/conveyor/v1/conveyorv1connect"
)

// openSession dials a session stream against a fresh engine + API server
// and returns the stream.
func openSession(t *testing.T) (*connect.BidiStreamForClient[conveyorv1.WorkerMessage, conveyorv1.ServerMessage], *testSessionBackend) {
	t.Helper()

	engine, taskLog := startTestEngine(t)
	baseURL := startAPIServer(t, engine, taskLog, nil)
	client := conveyorv1connect.NewWorkerServiceClient(h2cHTTPClient(), baseURL)
	stream := client.Session(context.Background())

	// Close the stream before the server shuts down; an open session would
	// park the handler in Receive and block the server's cleanup forever.
	t.Cleanup(func() {
		_ = stream.CloseRequest()
		_ = stream.CloseResponse()
	})

	return stream, &testSessionBackend{engine: engine, taskLog: taskLog}
}

// testSessionBackend bundles the engine side of one protocol test.
type testSessionBackend struct {
	engine  engineAPI
	taskLog taskLogAPI
}

// engineAPI is the slice of the engine the protocol tests use.
type engineAPI interface {
	Enqueue(ctx context.Context, task *conveyorv1.TaskEnvelope) error
}

// taskLogAPI is the slice of the broker the protocol tests use.
type taskLogAPI interface {
	GetTask(ctx context.Context, id string) (*conveyorv1.TaskEnvelope, conveyorv1.TaskState, error)
}

func TestSessionRejectsHelloLessFirstFrame(t *testing.T) {
	stream, _ := openSession(t)

	require.NoError(t, stream.Send(resultFrame("task-1", conveyorv1.TaskOutcome_TASK_OUTCOME_SUCCESS)))

	_, err := stream.Receive()
	require.Error(t, err)
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
}

func TestSessionRejectsDuplicateHelloFrame(t *testing.T) {
	stream, _ := openSession(t)

	hello := helloFrame(map[string]int32{"default": 1}, 2)
	require.NoError(t, stream.Send(hello))

	first, err := stream.Receive()
	require.NoError(t, err)
	require.NotNil(t, first.GetWelcome())

	require.NoError(t, stream.Send(hello))

	_, err = stream.Receive()
	require.Error(t, err)
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
}

func TestSessionRejectsCreditOverflow(t *testing.T) {
	stream, _ := openSession(t)

	require.NoError(t, stream.Send(helloFrame(map[string]int32{"default": 1}, 2)))

	welcome, err := stream.Receive()
	require.NoError(t, err)
	require.NotNil(t, welcome.GetWelcome())

	require.NoError(t, stream.Send(creditFrame(3)))

	_, err = stream.Receive()
	require.Error(t, err)
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
}

func TestSessionWelcomeCarriesLeaseParameters(t *testing.T) {
	stream, _ := openSession(t)

	require.NoError(t, stream.Send(helloFrame(map[string]int32{"default": 1}, 2)))

	first, err := stream.Receive()
	require.NoError(t, err)

	welcome := first.GetWelcome()
	require.NotNil(t, welcome)
	require.NotEmpty(t, welcome.GetSessionId())
	require.Equal(t, 2*time.Second, welcome.GetLeaseTtl().AsDuration())
	require.Equal(t, 2*time.Second/heartbeatDivisor, welcome.GetHeartbeatInterval().AsDuration())
}

// TestSessionDispatchAndResult drives one task through the raw protocol:
// Hello, Welcome, Dispatch, Result, durable completion.
func TestSessionDispatchAndResult(t *testing.T) {
	stream, backend := openSession(t)

	require.NoError(t, stream.Send(helloFrame(map[string]int32{"default": 1}, 2)))

	first, err := stream.Receive()
	require.NoError(t, err)
	require.NotNil(t, first.GetWelcome())

	ctx := context.Background()

	task := &conveyorv1.TaskEnvelope{
		Id:          "task-protocol-1",
		Queue:       "default",
		Type:        "test:protocol",
		Payload:     []byte(`{}`),
		ContentType: "application/json",
		Options:     &conveyorv1.TaskOptions{MaxRetry: 3, Priority: 4, Retention: nil},
	}
	require.NoError(t, backend.engine.Enqueue(ctx, task))

	dispatched, err := stream.Receive()
	require.NoError(t, err)

	dispatch := dispatched.GetDispatch()
	require.NotNil(t, dispatch)
	require.Equal(t, "task-protocol-1", dispatch.GetTask().GetId())
	require.True(t, dispatch.GetDeadline().IsValid(), "dispatch must carry the effective deadline")

	require.NoError(t, stream.Send(resultFrame("task-protocol-1", conveyorv1.TaskOutcome_TASK_OUTCOME_SUCCESS)))

	require.Eventually(t, func() bool {
		_, state, err := backend.taskLog.GetTask(ctx, "task-protocol-1")

		return err == nil && state == conveyorv1.TaskState_TASK_STATE_COMPLETED
	}, 10*time.Second, 20*time.Millisecond, "result must complete the task durably")

	require.NoError(t, stream.CloseRequest())
	_ = stream.CloseResponse()
}

// TestSessionCloseReleasesInflight verifies release-on-disconnect at the
// protocol level: a worker that vanishes mid-task gets its task released
// for immediate redelivery with no retry penalty.
func TestSessionCloseReleasesInflight(t *testing.T) {
	stream, backend := openSession(t)

	require.NoError(t, stream.Send(helloFrame(map[string]int32{"default": 1}, 1)))

	first, err := stream.Receive()
	require.NoError(t, err)
	require.NotNil(t, first.GetWelcome())

	ctx := context.Background()

	task := &conveyorv1.TaskEnvelope{
		Id:          "task-vanish-1",
		Queue:       "default",
		Type:        "test:vanish",
		Payload:     []byte(`{}`),
		ContentType: "application/json",
		Options:     &conveyorv1.TaskOptions{MaxRetry: 3, Priority: 4},
	}
	require.NoError(t, backend.engine.Enqueue(ctx, task))

	dispatched, err := stream.Receive()
	require.NoError(t, err)
	require.NotNil(t, dispatched.GetDispatch())

	// Vanish without sending a Result.
	require.NoError(t, stream.CloseRequest())
	_ = stream.CloseResponse()

	require.Eventually(t, func() bool {
		envelope, state, err := backend.taskLog.GetTask(ctx, "task-vanish-1")

		return err == nil &&
			state == conveyorv1.TaskState_TASK_STATE_PENDING &&
			envelope.GetRetried() == 0
	}, 10*time.Second, 20*time.Millisecond, "in-flight task must be released with no retry penalty")
}

// TestSessionDrainOnShutdown verifies the server-side graceful drain: a
// drain ends live sessions, releases their in-flight tasks with no retry
// penalty, and rejects new sessions while draining.
func TestSessionDrainOnShutdown(t *testing.T) {
	engine, taskLog := startTestEngine(t)
	workerService := NewWorkerService(engine, slog.New(slog.DiscardHandler), clock.System())

	mux := http.NewServeMux()
	mux.Handle(conveyorv1connect.NewWorkerServiceHandler(workerService))

	server := httptest.NewUnstartedServer(mux)

	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	protocols.SetHTTP2(true)
	protocols.SetUnencryptedHTTP2(true)

	server.Config.Protocols = protocols
	server.Start()
	t.Cleanup(server.Close)

	client := conveyorv1connect.NewWorkerServiceClient(h2cHTTPClient(), server.URL)
	stream := client.Session(context.Background())

	t.Cleanup(func() {
		_ = stream.CloseRequest()
		_ = stream.CloseResponse()
	})

	require.NoError(t, stream.Send(helloFrame(map[string]int32{"default": 1}, 1)))

	first, err := stream.Receive()
	require.NoError(t, err)
	require.NotNil(t, first.GetWelcome())

	// The connected session is reported with its declared queues and concurrency.
	require.Eventually(t, func() bool {
		sessions := workerService.Sessions()

		return len(sessions) == 1 &&
			len(sessions[0].Queues) == 1 && sessions[0].Queues[0] == "default" &&
			sessions[0].Concurrency == 1
	}, 5*time.Second, 20*time.Millisecond, "the connected session must appear in Sessions()")

	ctx := context.Background()

	task := &conveyorv1.TaskEnvelope{
		Id:          "task-drain-1",
		Queue:       "default",
		Type:        "test:drain",
		Payload:     []byte(`{}`),
		ContentType: "application/json",
		Options:     &conveyorv1.TaskOptions{MaxRetry: 3, Priority: 4},
	}
	require.NoError(t, engine.Enqueue(ctx, task))

	dispatched, err := stream.Receive()
	require.NoError(t, err)
	require.NotNil(t, dispatched.GetDispatch())

	// Drain with the task still executing: the session must end and the
	// task must be released with no retry penalty before Drain returns.
	drainCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	require.NoError(t, workerService.DrainSessions(drainCtx))

	envelope, state, err := taskLog.GetTask(ctx, "task-drain-1")
	require.NoError(t, err)
	require.Equal(t, conveyorv1.TaskState_TASK_STATE_PENDING, state)
	require.Zero(t, envelope.GetRetried(), "drain release must not count as a retry")

	// The worker sees its stream end.
	_, err = stream.Receive()
	require.Error(t, err, "drained session stream must be closed")

	// New sessions are turned away while draining.
	second := client.Session(context.Background())

	t.Cleanup(func() {
		_ = second.CloseRequest()
		_ = second.CloseResponse()
	})

	require.NoError(t, second.Send(helloFrame(map[string]int32{"default": 1}, 1)))

	_, err = second.Receive()
	require.Error(t, err)
	require.Equal(t, connect.CodeUnavailable, connect.CodeOf(err))
}
