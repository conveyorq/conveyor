package api

import (
	"context"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	conveyorv1 "github.com/tochemey/conveyor/internal/proto/conveyor/v1"
	"github.com/tochemey/conveyor/internal/proto/conveyor/v1/conveyorv1connect"
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
