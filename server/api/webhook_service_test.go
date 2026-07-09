// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	"github.com/conveyorq/conveyor/internal/broker"
	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
	"github.com/conveyorq/conveyor/internal/proto/conveyor/v1/conveyorv1connect"
	"github.com/conveyorq/conveyor/internal/webhook"
)

// acceptingEndpoint is a webhook endpoint answering every delivery with
// "accepted" and capturing each delivery's lease token.
type acceptingEndpoint struct {
	// server is the backing test server.
	server *httptest.Server

	// mutex guards tokens.
	mutex sync.Mutex
	// tokens are the captured lease tokens in arrival order.
	tokens []string
}

// newAcceptingEndpoint starts the endpoint and closes it with the test.
func newAcceptingEndpoint(t *testing.T) *acceptingEndpoint {
	t.Helper()

	endpoint := &acceptingEndpoint{}

	endpoint.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request webhook.Request
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			w.WriteHeader(http.StatusBadRequest)

			return
		}

		encoded, _ := json.Marshal(request.Params)

		var params webhook.TaskParams
		_ = json.Unmarshal(encoded, &params)

		endpoint.mutex.Lock()
		endpoint.tokens = append(endpoint.tokens, params.Lease.Token)
		endpoint.mutex.Unlock()

		response := webhook.Response{JSONRPC: webhook.Version, ID: request.ID, Result: &webhook.ResultBody{Status: webhook.StatusAccepted}}
		_ = json.NewEncoder(w).Encode(&response)
	}))

	t.Cleanup(endpoint.server.Close)

	return endpoint
}

// firstToken returns the first captured lease token, when one arrived.
func (e *acceptingEndpoint) firstToken() (string, bool) {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	if len(e.tokens) == 0 {
		return "", false
	}

	return e.tokens[0], true
}

// TestWebhookServiceAsyncCompletionEndToEnd drives the full asynchronous
// contract through the public API: the endpoint accepts a delivery, then
// heartbeats and reports success over WebhookService with nothing but its
// lease token, while every bearer-token service stays locked.
func TestWebhookServiceAsyncCompletionEndToEnd(t *testing.T) {
	const queue = "cb-async"

	ctx := context.Background()
	engine, taskLog := startTestEngine(t)
	endpoint := newAcceptingEndpoint(t)

	registration := &broker.WebhookWorker{
		Name:        "cb-hooks",
		URL:         endpoint.server.URL,
		Queues:      map[string]int32{queue: 1},
		Concurrency: 4,
		Secrets:     []string{"cb-secret"},
	}
	require.NoError(t, taskLog.UpsertWebhookWorker(ctx, registration))
	require.NoError(t, engine.ReconcileWebhookWorkers(ctx))

	// Bearer tokens are configured, so the exemption is what proves the
	// lease-token surface works without credentials.
	baseURL := startAPIServer(t, engine, taskLog, []string{"api-token"})
	callbacks := conveyorv1connect.NewWebhookServiceClient(http.DefaultClient, baseURL)

	require.NoError(t, taskLog.Enqueue(ctx, &conveyorv1.TaskEnvelope{
		Id: "cb-task-1", Queue: queue, Type: "email:send",
		Payload: []byte(`{}`), ContentType: "application/json",
		Options: &conveyorv1.TaskOptions{MaxRetry: 3, Priority: 4},
	}))

	var token string

	require.Eventually(t, func() bool {
		captured, arrived := endpoint.firstToken()
		token = captured

		return arrived
	}, 10*time.Second, 50*time.Millisecond, "the accepted delivery carries a lease token")

	heartbeat := connect.NewRequest(&conveyorv1.WebhookHeartbeatRequest{LeaseToken: token})
	_, err := callbacks.Heartbeat(ctx, heartbeat)
	require.NoError(t, err)

	report := connect.NewRequest(&conveyorv1.WebhookReportResultRequest{
		LeaseToken: token,
		Outcome:    conveyorv1.TaskOutcome_TASK_OUTCOME_SUCCESS,
	})
	_, err = callbacks.ReportResult(ctx, report)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		_, state, taskErr := taskLog.GetTask(ctx, "cb-task-1")

		return taskErr == nil && state == conveyorv1.TaskState_TASK_STATE_COMPLETED
	}, 10*time.Second, 50*time.Millisecond)

	// A spent token still parses and verifies, but its delivery is gone:
	// the report lands on an unknown delivery and changes nothing.
	_, err = callbacks.ReportResult(ctx, connect.NewRequest(&conveyorv1.WebhookReportResultRequest{
		LeaseToken: token,
		Outcome:    conveyorv1.TaskOutcome_TASK_OUTCOME_RETRY,
	}))
	require.NoError(t, err)

	_, state, err := taskLog.GetTask(ctx, "cb-task-1")
	require.NoError(t, err)
	require.Equal(t, conveyorv1.TaskState_TASK_STATE_COMPLETED, state, "a spent token cannot rewrite the outcome")
}

func TestWebhookServiceRejectsBadTokens(t *testing.T) {
	ctx := context.Background()
	engine, taskLog := startTestEngine(t)

	require.NoError(t, taskLog.UpsertWebhookWorker(ctx, &broker.WebhookWorker{
		Name:        "guard-hooks",
		URL:         "https://example.com/tasks",
		Queues:      map[string]int32{"default": 1},
		Concurrency: 1,
		Secrets:     []string{"right-secret"},
	}))

	baseURL := startAPIServer(t, engine, taskLog, nil)
	callbacks := conveyorv1connect.NewWebhookServiceClient(http.DefaultClient, baseURL)

	cases := []struct {
		name  string
		token string
	}{
		{"malformed", "not-a-token"},
		{"unknown registration", webhook.MintLeaseToken("any", "ghost-hooks", "t", "l")},
		{"wrong secret", webhook.MintLeaseToken("wrong-secret", "guard-hooks", "t", "l")},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := callbacks.Heartbeat(ctx, connect.NewRequest(&conveyorv1.WebhookHeartbeatRequest{LeaseToken: tc.token}))
			require.Equal(t, connect.CodeUnauthenticated, connect.CodeOf(err))

			_, err = callbacks.ReportResult(ctx, connect.NewRequest(&conveyorv1.WebhookReportResultRequest{
				LeaseToken: tc.token,
				Outcome:    conveyorv1.TaskOutcome_TASK_OUTCOME_SUCCESS,
			}))
			require.Equal(t, connect.CodeUnauthenticated, connect.CodeOf(err))
		})
	}
}

// TestWebhookServiceReportsUntrackedDelivery proves that a token whose
// registration exists (so it verifies) but whose gateway is not running maps
// to NotFound on both callbacks: the delivery is no longer tracked.
func TestWebhookServiceReportsUntrackedDelivery(t *testing.T) {
	ctx := context.Background()
	engine, taskLog := startTestEngine(t)

	// The registration exists so its token verifies, but it is paused: the
	// manager's reconcile skips paused registrations, so no gateway is ever
	// spawned and the callback has nowhere to land. Pausing makes this
	// deterministic — an unpaused registration would race the reconcile that
	// spawns its gateway.
	require.NoError(t, taskLog.UpsertWebhookWorker(ctx, &broker.WebhookWorker{
		Name:        "untracked-hooks",
		URL:         "https://example.com/tasks",
		Queues:      map[string]int32{"default": 1},
		Concurrency: 1,
		Secrets:     []string{"untracked-secret"},
		Paused:      true,
	}))

	service := NewWebhookService(engine, taskLog)
	token := webhook.MintLeaseToken("untracked-secret", "untracked-hooks", "t", "l")

	_, err := service.Heartbeat(ctx, connect.NewRequest(&conveyorv1.WebhookHeartbeatRequest{LeaseToken: token}))
	require.Equal(t, connect.CodeNotFound, connect.CodeOf(err))

	_, err = service.ReportResult(ctx, connect.NewRequest(&conveyorv1.WebhookReportResultRequest{
		LeaseToken: token,
		Outcome:    conveyorv1.TaskOutcome_TASK_OUTCOME_SUCCESS,
	}))
	require.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
}

// TestWebhookServiceReturnsInternalOnBrokerFault proves that a broker failure
// while loading a registration for verification surfaces as Internal, not as
// an unauthenticated rejection that would hide the fault.
func TestWebhookServiceReturnsInternalOnBrokerFault(t *testing.T) {
	ctx := context.Background()
	engine, taskLog := startTestEngine(t)

	service := NewWebhookService(engine, &faultBroker{Broker: taskLog, failOn: "GetWebhookWorker"})
	token := webhook.MintLeaseToken("s", "hooks", "t", "l")

	_, err := service.Heartbeat(ctx, connect.NewRequest(&conveyorv1.WebhookHeartbeatRequest{LeaseToken: token}))
	require.Equal(t, connect.CodeInternal, connect.CodeOf(err))

	_, err = service.ReportResult(ctx, connect.NewRequest(&conveyorv1.WebhookReportResultRequest{
		LeaseToken: token,
		Outcome:    conveyorv1.TaskOutcome_TASK_OUTCOME_SUCCESS,
	}))
	require.Equal(t, connect.CodeInternal, connect.CodeOf(err))
}

func TestWebhookServiceRejectsInvalidOutcomes(t *testing.T) {
	ctx := context.Background()
	engine, taskLog := startTestEngine(t)
	baseURL := startAPIServer(t, engine, taskLog, nil)
	callbacks := conveyorv1connect.NewWebhookServiceClient(http.DefaultClient, baseURL)

	for _, outcome := range []conveyorv1.TaskOutcome{
		conveyorv1.TaskOutcome_TASK_OUTCOME_UNSPECIFIED,
		conveyorv1.TaskOutcome_TASK_OUTCOME_RELEASED,
	} {
		_, err := callbacks.ReportResult(ctx, connect.NewRequest(&conveyorv1.WebhookReportResultRequest{
			LeaseToken: webhook.MintLeaseToken("s", "hooks", "t", "l"),
			Outcome:    outcome,
		}))
		require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err), "outcome %s must be rejected before token checks", outcome)
	}
}
