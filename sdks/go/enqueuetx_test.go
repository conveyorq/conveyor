// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package conveyor

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
	"github.com/conveyorq/conveyor/internal/proto/conveyor/v1/conveyorv1connect"
)

// recordingTaskService captures the EnqueueTx request it receives and echoes one
// task per item.
type recordingTaskService struct {
	conveyorv1connect.UnimplementedTaskServiceHandler

	mu     sync.Mutex
	lastTx *conveyorv1.EnqueueTxRequest
}

func (s *recordingTaskService) EnqueueTx(_ context.Context, request *connect.Request[conveyorv1.EnqueueTxRequest]) (*connect.Response[conveyorv1.EnqueueTxResponse], error) {
	s.mu.Lock()
	s.lastTx = request.Msg
	s.mu.Unlock()

	infos := make([]*conveyorv1.TaskInfo, len(request.Msg.GetTasks()))

	for i, task := range request.Msg.GetTasks() {
		if task.GetType() == "boom" {
			return nil, connect.NewError(connect.CodeAlreadyExists, errors.New("duplicate"))
		}

		infos[i] = &conveyorv1.TaskInfo{Id: task.GetTaskId(), Type: task.GetType(), Queue: task.GetQueue()}
	}

	return connect.NewResponse(&conveyorv1.EnqueueTxResponse{Tasks: infos}), nil
}

// startRecordingServer starts a stub TaskService over unencrypted HTTP/2 and
// returns its URL plus the recorder.
func startRecordingServer(t *testing.T) (string, *recordingTaskService) {
	t.Helper()

	service := &recordingTaskService{}

	mux := http.NewServeMux()
	mux.Handle(conveyorv1connect.NewTaskServiceHandler(service))

	server := httptest.NewUnstartedServer(mux)

	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	protocols.SetHTTP2(true)
	protocols.SetUnencryptedHTTP2(true)
	server.Config.Protocols = protocols

	server.Start()
	t.Cleanup(server.Close)

	return server.URL, service
}

func TestClientEnqueueTxValidation(t *testing.T) {
	client, err := NewClient("http://127.0.0.1:1")
	require.NoError(t, err)

	ctx := context.Background()

	_, err = client.EnqueueTx(ctx, nil)
	require.ErrorContains(t, err, "at least one task is required")

	_, err = client.EnqueueTx(ctx, []TxTask{Tx(NewTask("", JSON("x")))})
	require.ErrorContains(t, err, "task type is required")
}

func TestClientEnqueueTxCommitsAllWithPerTaskOptions(t *testing.T) {
	baseURL, service := startRecordingServer(t)

	client, err := NewClient(baseURL)
	require.NoError(t, err)

	infos, err := client.EnqueueTx(context.Background(), []TxTask{
		Tx(NewTask("test:a", JSON("one")), TaskID("tx-1"), Queue("billing")),
		Tx(NewTask("test:b", JSON("two")), TaskID("tx-2"), Queue("mail")),
	})
	require.NoError(t, err)
	require.Len(t, infos, 2)
	require.Equal(t, "tx-1", infos[0].ID)
	require.Equal(t, "tx-2", infos[1].ID)

	service.mu.Lock()
	defer service.mu.Unlock()

	require.Len(t, service.lastTx.GetTasks(), 2)
	require.Equal(t, "billing", service.lastTx.GetTasks()[0].GetQueue())
	require.Equal(t, "mail", service.lastTx.GetTasks()[1].GetQueue())
	require.Equal(t, "test:a", service.lastTx.GetTasks()[0].GetType())
}

func TestClientEnqueueTxMapsEveryOption(t *testing.T) {
	baseURL, service := startRecordingServer(t)

	client, err := NewClient(baseURL)
	require.NoError(t, err)

	_, err = client.EnqueueTx(context.Background(), []TxTask{
		Tx(NewTask("test:a", JSON("one")),
			TaskID("tx-1"),
			Queue("billing"),
			Priority(7),
			MaxRetry(5),
			ProcessIn(time.Minute),
			ExpiresIn(time.Hour),
			Retention(24*time.Hour),
			Unique(time.Hour),
			Timeout(30*time.Second),
			Deadline(time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)),
			ConcurrencyKey("customer:42"),
			RetryPolicy(RetryFixed, time.Second, time.Minute),
		),
		Tx(NewTask("test:b", JSON("two")), DependsOn("tx-1")),
	})
	require.NoError(t, err)

	service.mu.Lock()
	defer service.mu.Unlock()

	request := service.lastTx.GetTasks()[0]
	require.Equal(t, int32(7), request.GetPriority())
	require.Equal(t, int32(5), request.GetMaxRetry())
	require.Equal(t, "customer:42", request.GetConcurrencyKey())
	require.NotNil(t, request.GetProcessIn())
	require.NotNil(t, request.GetExpiresIn())
	require.NotNil(t, request.GetRetention())
	require.NotNil(t, request.GetUniqueTtl())
	require.NotNil(t, request.GetTimeout())
	require.NotNil(t, request.GetDeadline())
	require.NotNil(t, request.GetRetryPolicy())
}

func TestClientEnqueueTxMapsServerError(t *testing.T) {
	baseURL, _ := startRecordingServer(t)

	client, err := NewClient(baseURL)
	require.NoError(t, err)

	_, err = client.EnqueueTx(context.Background(), []TxTask{Tx(NewTask("boom", JSON("x")))})
	require.ErrorIs(t, err, ErrDuplicateTask)
}
