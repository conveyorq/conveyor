package api

import (
	"context"
	"net/http"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	conveyorv1 "github.com/tochemey/conveyor/internal/proto/conveyor/v1"
	"github.com/tochemey/conveyor/internal/proto/conveyor/v1/conveyorv1connect"
)

func TestAuthorizeHeaderHandling(t *testing.T) {
	interceptor, ok := NewAuthInterceptor([]string{"alpha", "beta"}).(*authInterceptor)
	require.True(t, ok)

	cases := map[string]struct {
		header string
		valid  bool
	}{
		"missing header":   {header: "", valid: false},
		"wrong scheme":     {header: "Basic alpha", valid: false},
		"empty token":      {header: "Bearer ", valid: false},
		"unknown token":    {header: "Bearer gamma", valid: false},
		"first token":      {header: "Bearer alpha", valid: true},
		"second token":     {header: "Bearer beta", valid: true},
		"token with extra": {header: "Bearer alpha2", valid: false},
	}

	for name, testCase := range cases {
		header := http.Header{}

		if testCase.header != "" {
			header.Set(authorizationHeader, testCase.header)
		}

		err := interceptor.authorize(header)

		if testCase.valid {
			require.NoError(t, err, "case %s", name)
		} else {
			require.Error(t, err, "case %s", name)
			require.Equal(t, connect.CodeUnauthenticated, connect.CodeOf(err), "case %s", name)
		}
	}
}

func TestUnaryCallsRejectBadTokens(t *testing.T) {
	engine, taskLog := startTestEngine(t)
	baseURL := startAPIServer(t, engine, taskLog, []string{"top-secret"})

	ctx := context.Background()
	request := &conveyorv1.EnqueueRequest{Type: "test:auth"}

	for name, token := range map[string]string{"missing": "", "wrong": "nope"} {
		client := newTaskClient(baseURL, token)

		_, err := client.Enqueue(ctx, connect.NewRequest(request))
		require.Error(t, err, "case %s", name)
		require.Equal(t, connect.CodeUnauthenticated, connect.CodeOf(err), "case %s", name)

		_, err = client.GetTask(ctx, connect.NewRequest(&conveyorv1.GetTaskRequest{Id: "x"}))
		require.Equal(t, connect.CodeUnauthenticated, connect.CodeOf(err), "case %s", name)

		_, err = client.EnqueueBatch(ctx, connect.NewRequest(&conveyorv1.EnqueueBatchRequest{
			Tasks: []*conveyorv1.EnqueueRequest{request},
		}))
		require.Equal(t, connect.CodeUnauthenticated, connect.CodeOf(err), "case %s", name)
	}

	allowed := newTaskClient(baseURL, "top-secret")

	_, err := allowed.Enqueue(ctx, connect.NewRequest(request))
	require.NoError(t, err)
}

func TestSessionStreamRejectsBadTokens(t *testing.T) {
	engine, taskLog := startTestEngine(t)
	baseURL := startAPIServer(t, engine, taskLog, []string{"top-secret"})

	client := conveyorv1connect.NewWorkerServiceClient(h2cHTTPClient(), baseURL)

	stream := client.Session(context.Background())

	t.Cleanup(func() {
		_ = stream.CloseRequest()
		_ = stream.CloseResponse()
	})

	require.NoError(t, stream.Send(&conveyorv1.WorkerMessage{
		Frame: &conveyorv1.WorkerMessage_Hello{
			Hello: &conveyorv1.Hello{Queues: map[string]int32{"default": 1}, Concurrency: 1},
		},
	}))

	_, err := stream.Receive()
	require.Error(t, err)
	require.Equal(t, connect.CodeUnauthenticated, connect.CodeOf(err))
}

// newTaskClient builds a TaskService client, optionally authenticated.
func newTaskClient(baseURL, token string) conveyorv1connect.TaskServiceClient {
	var options []connect.ClientOption

	if token != "" {
		options = append(options, connect.WithInterceptors(
			connect.UnaryInterceptorFunc(func(next connect.UnaryFunc) connect.UnaryFunc {
				return func(ctx context.Context, request connect.AnyRequest) (connect.AnyResponse, error) {
					request.Header().Set(authorizationHeader, bearerPrefix+token)

					return next(ctx, request)
				}
			})))
	}

	return conveyorv1connect.NewTaskServiceClient(h2cHTTPClient(), baseURL, options...)
}
