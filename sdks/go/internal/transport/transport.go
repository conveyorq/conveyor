// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

// Package transport owns the ConnectRPC plumbing of the SDK: client
// construction, bearer-token injection, and the worker session stream. No
// type of this package appears in the public SDK surface.
package transport

import (
	"context"
	"errors"
	"io"
	"iter"

	"connectrpc.com/connect"

	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
	"github.com/conveyorq/conveyor/internal/proto/conveyor/v1/conveyorv1connect"
	"github.com/conveyorq/conveyor/internal/wire"
)

// maxReadBytes bounds the size of one message the client decodes from the
// server, so a malformed or hostile server cannot exhaust client memory. It
// mirrors the server's request ceiling (server/api.MaxRequestBytes): the
// largest legitimate frame is a full batch of 1000 tasks at the 1 MiB inline
// payload limit plus per-task field overhead. Larger payloads arrive in
// chunks of at most 2 MiB. It lives here rather than importing the server,
// which SDK consumers must not pull in.
const maxReadBytes = 1000 * (1<<20 + 64<<10)

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

	options := []connect.ClientOption{connect.WithReadMaxBytes(maxReadBytes)}
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

// EnqueueStream uploads one task header followed by each chunk of chunks and a
// commit stating their total size, and returns the committed task. The server
// commits only on a matching commit, so a failing chunk source, which cancels
// the upload, commits nothing even if the transport ends the stream cleanly.
// When the server ends the upload early (an idempotent retry, or a rejection),
// the remaining chunks are not read and its answer is returned.
func (c *Client) EnqueueStream(ctx context.Context, header *conveyorv1.EnqueueRequest, chunks iter.Seq2[[]byte, error]) (*conveyorv1.TaskInfo, error) {
	uploadCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	stream := c.tasks.EnqueueStream(uploadCtx)

	err := stream.Send(&conveyorv1.EnqueueStreamRequest{Frame: &conveyorv1.EnqueueStreamRequest_Header{Header: header}})
	if err == nil {
		err = sendChunks(stream, chunks)
	}

	if err != nil && !errors.Is(err, io.EOF) {
		cancel()
		_, _ = stream.CloseAndReceive()

		return nil, err
	}

	response, err := stream.CloseAndReceive()
	if err != nil {
		return nil, err
	}

	return response.Msg.GetTask(), nil
}

// sendChunks sends every chunk on the upload stream, then the commit stating
// their total size, stopping at the first source or send error.
func sendChunks(stream *connect.ClientStreamForClient[conveyorv1.EnqueueStreamRequest, conveyorv1.EnqueueResponse], chunks iter.Seq2[[]byte, error]) error {
	var size uint64

	for data, err := range chunks {
		if err != nil {
			return err
		}

		if err = stream.Send(&conveyorv1.EnqueueStreamRequest{Frame: &conveyorv1.EnqueueStreamRequest_Chunk{Chunk: data}}); err != nil {
			return err
		}

		size += uint64(len(data))
	}

	return stream.Send(&conveyorv1.EnqueueStreamRequest{Frame: &conveyorv1.EnqueueStreamRequest_Commit{Commit: &conveyorv1.PayloadCommit{Size: size}}})
}

// EnqueueTx commits every task atomically and returns the committed tasks in
// request order.
func (c *Client) EnqueueTx(ctx context.Context, requests []*conveyorv1.EnqueueRequest) ([]*conveyorv1.TaskInfo, error) {
	response, err := c.tasks.EnqueueTx(ctx, connect.NewRequest(&conveyorv1.EnqueueTxRequest{Tasks: requests}))
	if err != nil {
		return nil, err
	}

	return response.Msg.GetTasks(), nil
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
