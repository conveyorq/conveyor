package transport

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
	"github.com/conveyorq/conveyor/internal/proto/conveyor/v1/conveyorv1connect"
)

// headerRecorder captures the Authorization header of every request.
type headerRecorder struct {
	// mutex guards seen.
	mutex sync.Mutex
	// seen are the captured Authorization values in arrival order.
	seen []string
}

// record stores one Authorization value.
func (r *headerRecorder) record(value string) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	r.seen = append(r.seen, value)
}

// all returns the captured values.
func (r *headerRecorder) all() []string {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	return append([]string(nil), r.seen...)
}

// stubTaskService answers every task RPC with fixed data.
type stubTaskService struct {
	conveyorv1connect.UnimplementedTaskServiceHandler
}

// Enqueue echoes a fixed task.
func (stubTaskService) Enqueue(context.Context, *connect.Request[conveyorv1.EnqueueRequest]) (*connect.Response[conveyorv1.EnqueueResponse], error) {
	return connect.NewResponse(&conveyorv1.EnqueueResponse{
		Task: &conveyorv1.TaskInfo{Id: "task-stub"},
	}), nil
}

// GetTask answers not-found for one well-known id and a task otherwise.
func (stubTaskService) GetTask(_ context.Context, request *connect.Request[conveyorv1.GetTaskRequest]) (*connect.Response[conveyorv1.GetTaskResponse], error) {
	if request.Msg.GetId() == "missing" {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("missing"))
	}

	return connect.NewResponse(&conveyorv1.GetTaskResponse{
		Task: &conveyorv1.TaskInfo{Id: request.Msg.GetId()},
	}), nil
}

// stubWorkerService answers Hello with Welcome and closes.
type stubWorkerService struct{}

// Session implements the worker stream for the stub.
func (stubWorkerService) Session(_ context.Context, stream *connect.BidiStream[conveyorv1.WorkerMessage, conveyorv1.ServerMessage]) error {
	if _, err := stream.Receive(); err != nil {
		return err
	}

	return stream.Send(&conveyorv1.ServerMessage{
		Frame: &conveyorv1.ServerMessage_Welcome{
			Welcome: &conveyorv1.Welcome{SessionId: "session-stub"},
		},
	})
}

// startStubServer serves the stubs over unencrypted HTTP/2, recording the
// Authorization header of every request.
func startStubServer(t *testing.T) (string, *headerRecorder) {
	t.Helper()

	recorder := &headerRecorder{}

	mux := http.NewServeMux()
	mux.Handle(conveyorv1connect.NewTaskServiceHandler(stubTaskService{}))
	mux.Handle(conveyorv1connect.NewWorkerServiceHandler(stubWorkerService{}))

	captured := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		recorder.record(request.Header.Get("Authorization"))
		mux.ServeHTTP(w, request)
	})

	server := httptest.NewUnstartedServer(captured)

	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	protocols.SetHTTP2(true)
	protocols.SetUnencryptedHTTP2(true)
	server.Config.Protocols = protocols

	server.Start()
	t.Cleanup(server.Close)

	return server.URL, recorder
}

func TestClientInjectsBearerToken(t *testing.T) {
	baseURL, recorder := startStubServer(t)
	client := New(baseURL, "top-secret")

	info, err := client.Enqueue(context.Background(), &conveyorv1.EnqueueRequest{Type: "test:ok"})
	require.NoError(t, err)
	require.Equal(t, "task-stub", info.GetId())

	stream := client.Session(context.Background())
	require.NoError(t, stream.Send(&conveyorv1.WorkerMessage{}))

	welcome, err := stream.Receive()
	require.NoError(t, err)
	require.Equal(t, "session-stub", welcome.GetWelcome().GetSessionId())

	require.NoError(t, stream.CloseRequest())
	_ = stream.CloseResponse()

	for _, header := range recorder.all() {
		require.Equal(t, "Bearer top-secret", header, "every call must carry the token")
	}

	require.Len(t, recorder.all(), 2, "one unary call and one stream")
}

func TestClientWithoutTokenSendsNoHeader(t *testing.T) {
	baseURL, recorder := startStubServer(t)
	client := New(baseURL, "")

	_, err := client.Enqueue(context.Background(), &conveyorv1.EnqueueRequest{Type: "test:ok"})
	require.NoError(t, err)

	headers := recorder.all()
	require.Len(t, headers, 1)
	require.Empty(t, headers[0])
}

func TestClientGetTaskPassesThroughErrors(t *testing.T) {
	baseURL, _ := startStubServer(t)
	client := New(baseURL, "")

	ctx := context.Background()

	info, err := client.GetTask(ctx, "task-42")
	require.NoError(t, err)
	require.Equal(t, "task-42", info.GetId())

	_, err = client.GetTask(ctx, "missing")
	require.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
}
