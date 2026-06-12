// Package transport owns the ConnectRPC plumbing of the SDK: client
// construction, bearer-token injection, and the worker session stream. No
// type of this package appears in the public SDK surface.
package transport

import (
	"context"

	"connectrpc.com/connect"

	conveyorv1 "github.com/tochemey/conveyor/internal/proto/conveyor/v1"
	"github.com/tochemey/conveyor/internal/proto/conveyor/v1/conveyorv1connect"
	"github.com/tochemey/conveyor/internal/wire"
)

// Client is the SDK's wire client: task RPCs and worker sessions over one
// HTTP/2 connection pool.
type Client struct {
	// tasks calls the enqueue-side API.
	tasks conveyorv1connect.TaskServiceClient
	// workers opens session streams.
	workers conveyorv1connect.WorkerServiceClient
}

// New builds a wire client for the given base URL. An empty token sends
// unauthenticated requests (development servers only). Plain http URLs use
// unencrypted HTTP/2, which the worker stream requires; https negotiates
// HTTP/2 via ALPN.
func New(baseURL, token string) *Client {
	httpClient := wire.NewH2CClient()

	var options []connect.ClientOption
	if token != "" {
		options = append(options, connect.WithInterceptors(wire.NewBearerInterceptor(token)))
	}

	return &Client{
		tasks:   conveyorv1connect.NewTaskServiceClient(httpClient, baseURL, options...),
		workers: conveyorv1connect.NewWorkerServiceClient(httpClient, baseURL, options...),
	}
}

// Enqueue commits one task and returns its initial server-side view.
func (c *Client) Enqueue(ctx context.Context, request *conveyorv1.EnqueueRequest) (*conveyorv1.TaskInfo, error) {
	response, err := c.tasks.Enqueue(ctx, connect.NewRequest(request))
	if err != nil {
		return nil, err
	}

	return response.Msg.GetTask(), nil
}

// GetTask fetches the current server-side view of one task.
func (c *Client) GetTask(ctx context.Context, id string) (*conveyorv1.TaskInfo, error) {
	response, err := c.tasks.GetTask(ctx, connect.NewRequest(&conveyorv1.GetTaskRequest{Id: id}))
	if err != nil {
		return nil, err
	}

	return response.Msg.GetTask(), nil
}

// Session opens one worker session stream.
func (c *Client) Session(ctx context.Context) *connect.BidiStreamForClient[conveyorv1.WorkerMessage, conveyorv1.ServerMessage] {
	return c.workers.Session(ctx)
}
