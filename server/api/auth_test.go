// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"context"
	"net/http"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
	"github.com/conveyorq/conveyor/internal/proto/conveyor/v1/conveyorv1connect"
)

func TestAuthorizeHeaderHandling(t *testing.T) {
	interceptor, ok := NewAuthInterceptor([]ScopedToken{
		{Token: "alpha", Scopes: AllScopes()},
		{Token: "beta", Scopes: AllScopes()},
	}).(*authInterceptor)
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

		err := interceptor.authorize(header, conveyorv1connect.TaskServiceEnqueueProcedure)

		if testCase.valid {
			require.NoError(t, err, "case %s", name)
		} else {
			require.Error(t, err, "case %s", name)
			require.Equal(t, connect.CodeUnauthenticated, connect.CodeOf(err), "case %s", name)
		}
	}
}

func TestAuthorizeEnforcesScopes(t *testing.T) {
	interceptor, ok := NewAuthInterceptor([]ScopedToken{
		{Token: "producer", Scopes: []Scope{ScopeProduce}},
		{Token: "worker", Scopes: []Scope{ScopeConsume}},
		{Token: "operator", Scopes: []Scope{ScopeAdmin}},
		{Token: "root", Scopes: AllScopes()},
	}).(*authInterceptor)
	require.True(t, ok)

	// A zero connect.Code marks a case that must be allowed: the enum starts at
	// CodeCanceled (1), so 0 is never a real code.
	const allowed connect.Code = 0

	cases := map[string]struct {
		token     string
		procedure string
		want      connect.Code
	}{
		"produce token enqueues":            {"producer", conveyorv1connect.TaskServiceEnqueueProcedure, allowed},
		"produce token denied on worker":    {"producer", conveyorv1connect.WorkerServiceSessionProcedure, connect.CodePermissionDenied},
		"produce token denied on admin":     {"producer", conveyorv1connect.AdminServiceListQueuesProcedure, connect.CodePermissionDenied},
		"consume token runs worker stream":  {"worker", conveyorv1connect.WorkerServiceSessionProcedure, allowed},
		"consume token denied on enqueue":   {"worker", conveyorv1connect.TaskServiceEnqueueProcedure, connect.CodePermissionDenied},
		"admin token reaches admin":         {"operator", conveyorv1connect.AdminServiceListQueuesProcedure, allowed},
		"admin token denied on enqueue":     {"operator", conveyorv1connect.TaskServiceEnqueueProcedure, connect.CodePermissionDenied},
		"admin token inspects a task":       {"operator", conveyorv1connect.TaskServiceGetTaskProcedure, allowed},
		"produce token inspects a task":     {"producer", conveyorv1connect.TaskServiceGetTaskProcedure, allowed},
		"consume token denied inspection":   {"worker", conveyorv1connect.TaskServiceGetTaskProcedure, connect.CodePermissionDenied},
		"full token enqueues":               {"root", conveyorv1connect.TaskServiceEnqueueProcedure, allowed},
		"full token runs worker stream":     {"root", conveyorv1connect.WorkerServiceSessionProcedure, allowed},
		"full token reaches admin":          {"root", conveyorv1connect.AdminServiceListQueuesProcedure, allowed},
		"full token denied on unmapped":     {"root", conveyorv1connect.WebhookServiceHeartbeatProcedure, connect.CodePermissionDenied},
		"unknown token stays unauthentic'd": {"ghost", conveyorv1connect.TaskServiceEnqueueProcedure, connect.CodeUnauthenticated},
	}

	for name, testCase := range cases {
		header := http.Header{}
		header.Set(authorizationHeader, bearerPrefix+testCase.token)

		err := interceptor.authorize(header, testCase.procedure)

		if testCase.want == allowed {
			require.NoError(t, err, "case %s", name)

			continue
		}

		require.Equal(t, testCase.want, connect.CodeOf(err), "case %s", name)
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
