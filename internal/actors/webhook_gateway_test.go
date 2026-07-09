// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package actors

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	goakt "github.com/tochemey/goakt/v4/actor"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/conveyorq/conveyor/internal/broker"
	"github.com/conveyorq/conveyor/internal/broker/memory"
	"github.com/conveyorq/conveyor/internal/clock"
	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
	"github.com/conveyorq/conveyor/internal/webhook"
)

// rpcEndpoint is a scripted webhook worker endpoint: it decodes each
// JSON-RPC delivery (single or batch) and answers with whatever the
// per-request script returns, recording every request it saw.
type rpcEndpoint struct {
	// server is the backing HTTP test server.
	server *httptest.Server
	// script builds the response for one decoded request.
	script func(request *webhook.Request) *webhook.Response

	// mutex guards requests, lastHeader, and lastBody.
	mutex sync.Mutex
	// requests are all decoded execute requests in arrival order.
	requests []*webhook.Request
	// lastHeader is the most recent delivery's HTTP header.
	lastHeader http.Header
	// lastBody is the most recent delivery's raw body.
	lastBody []byte
}

// newRPCEndpoint starts a scripted endpoint and closes it with the test.
func newRPCEndpoint(t *testing.T, script func(request *webhook.Request) *webhook.Response) *rpcEndpoint {
	t.Helper()

	endpoint := &rpcEndpoint{script: script}

	endpoint.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)

			return
		}

		endpoint.mutex.Lock()
		endpoint.lastHeader = r.Header.Clone()
		endpoint.lastBody = body
		endpoint.mutex.Unlock()

		var single webhook.Request
		if err := json.Unmarshal(body, &single); err == nil {
			endpoint.record(&single)
			_ = json.NewEncoder(w).Encode(endpoint.script(&single))

			return
		}

		var batch []*webhook.Request
		if err := json.Unmarshal(body, &batch); err == nil {
			responses := make([]*webhook.Response, 0, len(batch))

			for _, request := range batch {
				endpoint.record(request)
				responses = append(responses, endpoint.script(request))
			}

			_ = json.NewEncoder(w).Encode(responses)
		}
	}))

	t.Cleanup(endpoint.server.Close)

	return endpoint
}

// record captures one decoded request.
func (e *rpcEndpoint) record(request *webhook.Request) {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	e.requests = append(e.requests, request)
}

// seen returns a copy of the captured requests.
func (e *rpcEndpoint) seen() []*webhook.Request {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	return append([]*webhook.Request(nil), e.requests...)
}

// paramsOf re-decodes one captured request's params into TaskParams (the
// endpoint decodes params as generic JSON).
func paramsOf(t *testing.T, request *webhook.Request) *webhook.TaskParams {
	t.Helper()

	encoded, err := json.Marshal(request.Params)
	require.NoError(t, err)

	var params webhook.TaskParams
	require.NoError(t, json.Unmarshal(encoded, &params))

	return &params
}

// completedFor answers one request with a completed result.
func completedFor(request *webhook.Request) *webhook.Response {
	return &webhook.Response{JSONRPC: webhook.Version, ID: request.ID, Result: &webhook.ResultBody{Status: webhook.StatusCompleted}}
}

// errorFor answers one request with a JSON-RPC error.
func errorFor(request *webhook.Request, code int, message string) *webhook.Response {
	return &webhook.Response{JSONRPC: webhook.Version, ID: request.ID, Error: &webhook.ErrorBody{Code: code, Message: message}}
}

// seedWebhookWorker persists one registration before the engine boots, so
// the manager's first reconcile spawns its gateway.
func seedWebhookWorker(t *testing.T, taskLog broker.Broker, worker *broker.WebhookWorker) {
	t.Helper()
	require.NoError(t, taskLog.UpsertWebhookWorker(context.Background(), worker))
}

// testWebhookWorker builds a registration serving one queue.
func testWebhookWorker(url, queue string) *broker.WebhookWorker {
	return &broker.WebhookWorker{
		Name:        "hooks",
		URL:         url,
		Queues:      map[string]int32{queue: 1},
		Concurrency: 8,
		Secrets:     []string{"secret"},
	}
}

// taskState fetches one task's current state and last error.
func taskState(t *testing.T, taskLog broker.Broker, id string) (conveyorv1.TaskState, string) {
	t.Helper()

	task, state, err := taskLog.GetTask(context.Background(), id)
	require.NoError(t, err)

	return state, task.GetLastError()
}

// reconcileNow forces the webhook manager to reconcile without waiting for
// its production tick cadence.
func reconcileNow(t *testing.T, engine *Engine) {
	t.Helper()
	require.NoError(t, engine.ReconcileWebhookWorkers(context.Background()))
}

func TestWebhookDeliveryCompletesTasks(t *testing.T) {
	const queue = "hooks-complete"

	ctx := context.Background()
	taskLog := memory.New(clock.System())
	endpoint := newRPCEndpoint(t, completedFor)

	seedWebhookWorker(t, taskLog, testWebhookWorker(endpoint.server.URL, queue))
	startEngine(t, taskLog)

	engineTasks := 5
	for sequence := range engineTasks {
		require.NoError(t, taskLog.Enqueue(ctx, newTask(fmt.Sprintf("wh-%03d", sequence), queue, "email:send", 4)))
	}

	// The gateway registers on its own; the reaper's pending sweep wakes the
	// queue, so no explicit wake is needed.
	require.Eventually(t, completedReaches(taskLog, engineTasks), 10*time.Second, 50*time.Millisecond)

	requests := endpoint.seen()
	require.GreaterOrEqual(t, len(requests), engineTasks)

	first := requests[0]
	require.Equal(t, webhook.Version, first.JSONRPC)
	require.Equal(t, webhook.MethodExecute, first.Method)
	require.NotEmpty(t, first.ID, "the request id carries the lease id")

	params := paramsOf(t, first)
	require.Equal(t, queue, params.Queue)
	require.Equal(t, "email:send", params.Type)
	require.Equal(t, int32(1), params.Attempt)
	require.JSONEq(t, `{}`, string(params.Payload))
	require.Equal(t, "application/json", params.ContentType)
	require.NotEmpty(t, params.Deadline)

	// Every delivery is signed with the registration's newest secret; a
	// receiver recomputing the HMAC over "{timestamp}.{body}" matches.
	endpoint.mutex.Lock()
	header, body := endpoint.lastHeader, endpoint.lastBody
	endpoint.mutex.Unlock()

	timestamp := header.Get(webhook.TimestampHeader)
	require.NotEmpty(t, timestamp)
	require.Equal(t, "v1="+webhook.SignBody("secret", timestamp, body), header.Get(webhook.SignatureHeader))
}

// TestWebhookDeliveryOutcomes drives every synchronous outcome row through a
// real engine: each task type is scripted to a different endpoint answer,
// and each task's terminal state proves the mapping. Retry budgets are zero
// so every retryable answer archives deterministically as "retries
// exhausted".
func TestWebhookDeliveryOutcomes(t *testing.T) {
	const queue = "hooks-outcomes"

	ctx := context.Background()
	taskLog := memory.New(clock.System())

	endpoint := newRPCEndpoint(t, func(request *webhook.Request) *webhook.Response {
		var params webhook.TaskParams

		encoded, _ := json.Marshal(request.Params)
		_ = json.Unmarshal(encoded, &params)

		switch params.Type {
		case "case:completed":
			return completedFor(request)

		case "case:skip-retry":
			return errorFor(request, webhook.CodeSkipRetry, "bad payload")

		case "case:retry":
			return errorFor(request, webhook.CodeRetry, "smtp down")

		case "case:retry-then-complete":
			if params.Attempt == 1 {
				return errorFor(request, webhook.CodeRetry, "first attempt fails")
			}

			return completedFor(request)

		default:
			return errorFor(request, webhook.CodeRetry, "unscripted type")
		}
	})

	seedWebhookWorker(t, taskLog, testWebhookWorker(endpoint.server.URL, queue))
	startEngine(t, taskLog)

	enqueue := func(id, taskType string, maxRetry int32) {
		task := newTask(id, queue, taskType, 4)
		task.Options.MaxRetry = maxRetry
		require.NoError(t, taskLog.Enqueue(ctx, task))
	}

	enqueue("out-completed", "case:completed", 0)
	enqueue("out-skip", "case:skip-retry", 0)
	enqueue("out-retry", "case:retry", 0)
	enqueue("out-recovers", "case:retry-then-complete", 3)

	require.Eventually(t, func() bool {
		completed, err := tasksInState(taskLog, conveyorv1.TaskState_TASK_STATE_COMPLETED)
		if err != nil || completed != 2 {
			return false
		}

		archived, err := tasksInState(taskLog, conveyorv1.TaskState_TASK_STATE_ARCHIVED)

		return err == nil && archived == 2
	}, 15*time.Second, 100*time.Millisecond, "2 completions and 2 archivals")

	state, lastError := taskState(t, taskLog, "out-completed")
	require.Equal(t, conveyorv1.TaskState_TASK_STATE_COMPLETED, state)
	require.Empty(t, lastError)

	state, lastError = taskState(t, taskLog, "out-skip")
	require.Equal(t, conveyorv1.TaskState_TASK_STATE_ARCHIVED, state)
	require.Equal(t, "bad payload", lastError)

	state, lastError = taskState(t, taskLog, "out-retry")
	require.Equal(t, conveyorv1.TaskState_TASK_STATE_ARCHIVED, state)
	require.Equal(t, retriesExhaustedMessage+"smtp down", lastError)

	state, _ = taskState(t, taskLog, "out-recovers")
	require.Equal(t, conveyorv1.TaskState_TASK_STATE_COMPLETED, state)
}

// acceptedFor answers one request with an accepted result: the endpoint
// takes the task for asynchronous completion.
func acceptedFor(request *webhook.Request) *webhook.Response {
	return &webhook.Response{JSONRPC: webhook.Version, ID: request.ID, Result: &webhook.ResultBody{Status: webhook.StatusAccepted}}
}

// executeCount is how many execute deliveries the endpoint has seen so far.
func executeCount(endpoint *rpcEndpoint) int {
	count := 0

	for _, request := range endpoint.seen() {
		if request.Method == webhook.MethodExecute {
			count++
		}
	}

	return count
}

// canceled reports whether the endpoint received a cancel notification.
func canceled(endpoint *rpcEndpoint) bool {
	for _, request := range endpoint.seen() {
		if request.Method == webhook.MethodCancel {
			return true
		}
	}

	return false
}

// TestWebhookAsyncCompletion proves the asynchronous mode end to end: the
// endpoint accepts, the task holds its slot, and the lease-token-derived
// Heartbeat and ReportResult callbacks keep the lease and complete it.
func TestWebhookAsyncCompletion(t *testing.T) {
	const queue = "hooks-async"

	ctx := context.Background()
	taskLog := memory.New(clock.System())
	endpoint := newRPCEndpoint(t, acceptedFor)

	seedWebhookWorker(t, taskLog, testWebhookWorker(endpoint.server.URL, queue))
	engine := startEngine(t, taskLog)

	require.NoError(t, taskLog.Enqueue(ctx, newTask("async-1", queue, "email:send", 4)))

	require.Eventually(t, func() bool { return len(endpoint.seen()) >= 1 }, 10*time.Second, 50*time.Millisecond)

	params := paramsOf(t, endpoint.seen()[0])
	require.NotNil(t, params.Lease)
	require.NotEmpty(t, params.Lease.Token)
	// The advertised heartbeat cadence is a third of the lease TTL, so a
	// shorter TTL shortens it too instead of the endpoint outliving its lease.
	require.Equal(t, (testSettings.LeaseTTL / 3).String(), params.Lease.HeartbeatInterval)

	claims, err := webhook.ParseLeaseToken(params.Lease.Token)
	require.NoError(t, err)
	require.Equal(t, "hooks", claims.Registration)
	require.Equal(t, "async-1", claims.TaskID)

	// The accepted task holds its slot: active, not retried, not completed.
	require.Eventually(t, func() bool {
		active, err := tasksInState(taskLog, conveyorv1.TaskState_TASK_STATE_ACTIVE)

		return err == nil && active == 1
	}, 5*time.Second, 50*time.Millisecond)

	heartbeat := &conveyorv1.WebhookLeaseHeartbeat{TaskId: claims.TaskID, LeaseId: claims.LeaseID}
	require.NoError(t, engine.TellWebhookGateway(ctx, "hooks", heartbeat))

	// A stale lease id must not complete the delivery.
	stale := &conveyorv1.WebhookLeaseResult{TaskId: claims.TaskID, LeaseId: "stale-lease", Outcome: conveyorv1.TaskOutcome_TASK_OUTCOME_SUCCESS}
	require.NoError(t, engine.TellWebhookGateway(ctx, "hooks", stale))

	report := &conveyorv1.WebhookLeaseResult{TaskId: claims.TaskID, LeaseId: claims.LeaseID, Outcome: conveyorv1.TaskOutcome_TASK_OUTCOME_SUCCESS}
	require.NoError(t, engine.TellWebhookGateway(ctx, "hooks", report))

	require.Eventually(t, completedReaches(taskLog, 1), 10*time.Second, 50*time.Millisecond)

	task, state, err := taskLog.GetTask(ctx, "async-1")
	require.NoError(t, err)
	require.Equal(t, conveyorv1.TaskState_TASK_STATE_COMPLETED, state)
	require.Zero(t, task.GetRetried(), "an asynchronous completion is not a retry")
}

// TestWebhookAsyncLeaseLossCancelsAndRefills proves the lost-lease path: when
// an accepted delivery's lease is stolen, a heartbeat from the still-live
// endpoint pushes it a cancel notification so it stops wasted work, and the
// delivery's held credit is returned so the redelivery flows even though the
// single-slot registration is otherwise saturated.
func TestWebhookAsyncLeaseLossCancelsAndRefills(t *testing.T) {
	const queue = "hooks-async-cancel"

	ctx := context.Background()
	taskLog := memory.New(clock.System())

	endpoint := newRPCEndpoint(t, func(request *webhook.Request) *webhook.Response {
		if request.Method == webhook.MethodExecute {
			return acceptedFor(request)
		}

		// A cancel is a notification: it carries no id and wants no answer.
		return nil
	})

	// One slot: once the task is accepted the registration is saturated, so a
	// redelivery can only flow if the dropped delivery returned its credit.
	worker := testWebhookWorker(endpoint.server.URL, queue)
	worker.Concurrency = 1
	seedWebhookWorker(t, taskLog, worker)

	engine := startEngine(t, taskLog)

	require.NoError(t, taskLog.Enqueue(ctx, newTask("async-cancel-1", queue, "email:send", 4)))

	require.Eventually(t, func() bool { return executeCount(endpoint) >= 1 }, 10*time.Second, 50*time.Millisecond)

	claims, err := webhook.ParseLeaseToken(paramsOf(t, endpoint.seen()[0]).Lease.Token)
	require.NoError(t, err)

	// Steal the delivery's lease: the gateway still tracks it, but its next
	// lease extension now fails as if another delivery had reclaimed the task.
	require.NoError(t, taskLog.Release(ctx, claims.TaskID, claims.LeaseID))

	heartbeat := &conveyorv1.WebhookLeaseHeartbeat{TaskId: claims.TaskID, LeaseId: claims.LeaseID}
	require.Eventually(t, func() bool {
		_ = engine.TellWebhookGateway(ctx, "hooks", heartbeat)

		return canceled(endpoint)
	}, 10*time.Second, 100*time.Millisecond, "a heartbeat on a lost lease cancels the endpoint")

	// The returned credit lets the redelivery lease again despite the single
	// slot; without the refill the gateway would stay stuck at zero capacity.
	require.Eventually(t, func() bool { return executeCount(endpoint) >= 2 }, 10*time.Second, 100*time.Millisecond,
		"the dropped delivery's credit is returned so the task redelivers")
}

// TestWebhookAsyncLeaseExpiryRetries proves a silent endpoint loses its
// lease: the reaper reclaims the accepted task and the retry delivers again.
func TestWebhookAsyncLeaseExpiryRetries(t *testing.T) {
	const queue = "hooks-async-expiry"

	ctx := context.Background()
	taskLog := memory.New(clock.System())

	endpoint := newRPCEndpoint(t, func(request *webhook.Request) *webhook.Response {
		var params webhook.TaskParams

		encoded, _ := json.Marshal(request.Params)
		_ = json.Unmarshal(encoded, &params)

		// First attempt: accept and never report. Retry: complete.
		if params.Attempt == 1 {
			return acceptedFor(request)
		}

		return completedFor(request)
	})

	seedWebhookWorker(t, taskLog, testWebhookWorker(endpoint.server.URL, queue))

	engine := newNode(taskLog, recoverySettings, freePorts(t, 3), nil)
	require.NoError(t, engine.Start(ctx))

	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = engine.Stop(stopCtx)
	})

	require.NoError(t, taskLog.Enqueue(ctx, newTask("expiry-1", queue, "email:send", 4)))

	require.Eventually(t, completedReaches(taskLog, 1), 20*time.Second, 100*time.Millisecond,
		"a never-reporting endpoint's lease expires and the retry completes")

	task, _, err := taskLog.GetTask(ctx, "expiry-1")
	require.NoError(t, err)
	require.GreaterOrEqual(t, task.GetRetried(), int32(1))
	require.GreaterOrEqual(t, len(endpoint.seen()), 2, "the task was delivered at least twice")
}

// TestWebhookDeliveryTransportFailures proves a non-JSON-RPC answer is a
// retryable failure, never a success or permanent archive of its own.
func TestWebhookDeliveryTransportFailures(t *testing.T) {
	const queue = "hooks-transport"

	ctx := context.Background()
	taskLog := memory.New(clock.System())

	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}))
	t.Cleanup(endpoint.Close)

	seedWebhookWorker(t, taskLog, testWebhookWorker(endpoint.URL, queue))
	startEngine(t, taskLog)

	task := newTask("transport-1", queue, "email:send", 4)
	task.Options.MaxRetry = 0
	require.NoError(t, taskLog.Enqueue(ctx, task))

	require.Eventually(t, func() bool {
		archived, err := tasksInState(taskLog, conveyorv1.TaskState_TASK_STATE_ARCHIVED)

		return err == nil && archived == 1
	}, 10*time.Second, 100*time.Millisecond)

	state, lastError := taskState(t, taskLog, "transport-1")
	require.Equal(t, conveyorv1.TaskState_TASK_STATE_ARCHIVED, state)
	require.Contains(t, lastError, "HTTP 502")
}

// TestWebhookBatchDelivery proves a fired aggregation group arrives as one
// JSON-RPC batch POST and its members complete individually.
func TestWebhookBatchDelivery(t *testing.T) {
	const (
		queue     = "hooks-batch"
		batchType = "report:batch"
	)

	ctx := context.Background()
	taskLog := memory.New(clock.System())
	endpoint := newRPCEndpoint(t, completedFor)

	worker := testWebhookWorker(endpoint.server.URL, queue)
	worker.BatchTypes = []string{batchType}
	seedWebhookWorker(t, taskLog, worker)

	settings := testSettings
	settings.GroupGracePeriod = 50 * time.Millisecond
	settings.GroupSweepInterval = 20 * time.Millisecond

	engine := newNode(taskLog, settings, freePorts(t, 3), nil)
	require.NoError(t, engine.Start(ctx))

	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = engine.Stop(stopCtx)
	})

	for _, id := range []string{"wb-001", "wb-002"} {
		require.NoError(t, taskLog.Enqueue(ctx, groupedTask(id, queue, batchType, "G")))
	}

	require.Eventually(t, completedReaches(taskLog, 2), 10*time.Second, 50*time.Millisecond)

	// Both members arrived, keyed by task id (the batch shares one lease).
	requests := endpoint.seen()
	require.Len(t, requests, 2)
	ids := map[string]bool{requests[0].ID: true, requests[1].ID: true}
	require.True(t, ids["wb-001"] && ids["wb-002"], "batch member calls are keyed by task id, got %v", ids)
}

// TestWebhookManagerPauseAndResume proves reconciliation stops a paused
// registration's gateway (tasks stay pending) and revives it on resume.
func TestWebhookManagerPauseAndResume(t *testing.T) {
	const queue = "hooks-pause"

	ctx := context.Background()
	taskLog := memory.New(clock.System())
	endpoint := newRPCEndpoint(t, completedFor)

	seedWebhookWorker(t, taskLog, testWebhookWorker(endpoint.server.URL, queue))
	engine := startEngine(t, taskLog)

	require.NoError(t, taskLog.Enqueue(ctx, newTask("pause-1", queue, "email:send", 4)))
	require.Eventually(t, completedReaches(taskLog, 1), 10*time.Second, 50*time.Millisecond)

	require.NoError(t, taskLog.SetWebhookWorkerPaused(ctx, "hooks", true))
	reconcileNow(t, engine)

	// The drained gateway announced zero capacity; new work must sit pending.
	require.NoError(t, taskLog.Enqueue(ctx, newTask("pause-2", queue, "email:send", 4)))

	require.Never(t, func() bool {
		count, err := tasksInState(taskLog, conveyorv1.TaskState_TASK_STATE_COMPLETED)

		return err == nil && count > 1
	}, 2*time.Second, 200*time.Millisecond, "a paused registration must not take work")

	require.NoError(t, taskLog.SetWebhookWorkerPaused(ctx, "hooks", false))
	reconcileNow(t, engine)

	require.Eventually(t, completedReaches(taskLog, 2), 10*time.Second, 50*time.Millisecond)
}

func TestDeliveryResultMapping(t *testing.T) {
	cases := []struct {
		name        string
		delivery    webhookDelivery
		wantOutcome conveyorv1.TaskOutcome
		wantError   string
	}{
		{"completed", webhookDelivery{taskID: "t", outcome: webhook.OutcomeCompleted}, conveyorv1.TaskOutcome_TASK_OUTCOME_SUCCESS, ""},
		{"skip retry", webhookDelivery{taskID: "t", outcome: webhook.OutcomeSkipRetry, message: "no"}, conveyorv1.TaskOutcome_TASK_OUTCOME_SKIP_RETRY, "no"},
		{"retry", webhookDelivery{taskID: "t", outcome: webhook.OutcomeRetry, message: "later"}, conveyorv1.TaskOutcome_TASK_OUTCOME_RETRY, "later"},
		{"transport", webhookDelivery{taskID: "t", outcome: webhook.OutcomeTransportFailure, message: "conn refused"}, conveyorv1.TaskOutcome_TASK_OUTCOME_RETRY, "conn refused"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result := deliveryResult(tc.delivery)
			require.Equal(t, tc.wantOutcome, result.GetOutcome())
			require.Equal(t, tc.wantError, result.GetErrorMsg())
			require.Equal(t, "t", result.GetTaskId())
		})
	}
}

func TestBatchMemberResultMapping(t *testing.T) {
	transportFailed := webhookBatchDelivery{leaseID: "L", transportError: "conn refused"}
	require.Equal(t, conveyorv1.TaskOutcome_TASK_OUTCOME_RETRY, batchMemberResult(transportFailed, "a").GetOutcome())
	require.Equal(t, "conn refused", batchMemberResult(transportFailed, "a").GetErrorMsg())

	answered := webhookBatchDelivery{
		leaseID: "L",
		outcomes: map[string]webhookDelivery{
			"a": {taskID: "a", outcome: webhook.OutcomeCompleted},
		},
	}
	require.Equal(t, conveyorv1.TaskOutcome_TASK_OUTCOME_SUCCESS, batchMemberResult(answered, "a").GetOutcome())

	// An omitted member releases without penalty, mirroring stream batches.
	require.Equal(t, conveyorv1.TaskOutcome_TASK_OUTCOME_RELEASED, batchMemberResult(answered, "b").GetOutcome())
}

// flakyEndpoint is a webhook endpoint whose health a test flips at will:
// unhealthy answers HTTP 502 (a transport failure), healthy completes.
type flakyEndpoint struct {
	// server is the backing test server.
	server *httptest.Server
	// healthy selects the behavior.
	healthy atomic.Bool
	// hits counts every delivery attempt.
	hits atomic.Int64
}

// newFlakyEndpoint starts an unhealthy endpoint and closes it with the test.
func newFlakyEndpoint(t *testing.T) *flakyEndpoint {
	t.Helper()

	endpoint := &flakyEndpoint{}

	endpoint.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		endpoint.hits.Add(1)

		if !endpoint.healthy.Load() {
			w.WriteHeader(http.StatusBadGateway)

			return
		}

		var request webhook.Request
		_ = json.NewDecoder(r.Body).Decode(&request)
		_ = json.NewEncoder(w).Encode(completedFor(&request))
	}))

	t.Cleanup(endpoint.server.Close)

	return endpoint
}

// tickGatewayNow fires one registerTick on a webhook gateway without waiting
// for the production cadence, driving breaker-state capacity sync.
func tickGatewayNow(t *testing.T, engine *Engine, registration string) {
	t.Helper()

	pid, err := engine.System().ActorOf(context.Background(), webhookGatewayPrefix+registration)
	require.NoError(t, err)
	require.NoError(t, goakt.Tell(context.Background(), pid, registerTick{}))
}

// TestWebhookBreakerWithholdsAndRecovers proves the endpoint breaker's whole
// arc: transport failures open it, an open breaker withholds capacity so
// tasks stop churning, and after the open timeout a probe restores delivery.
func TestWebhookBreakerWithholdsAndRecovers(t *testing.T) {
	const queue = "hooks-breaker"

	ctx := context.Background()
	taskLog := memory.New(clock.System())
	endpoint := newFlakyEndpoint(t)

	seedWebhookWorker(t, taskLog, testWebhookWorker(endpoint.server.URL, queue))

	settings := testSettings
	settings.WebhookBreakerOpenTimeout = 500 * time.Millisecond

	engine := newNode(taskLog, settings, freePorts(t, 3), nil)
	require.NoError(t, engine.Start(ctx))

	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = engine.Stop(stopCtx)
	})

	// Fast fixed retries keep failing attempts flowing until the breaker
	// trips, without waiting out the default exponential backoff.
	for sequence := range 4 {
		task := newTask(fmt.Sprintf("brk-%03d", sequence), queue, "email:send", 4)
		task.Options.RetryPolicy = &conveyorv1.RetryPolicy{
			Strategy: conveyorv1.RetryStrategy_RETRY_STRATEGY_FIXED,
			Base:     durationpb.New(50 * time.Millisecond),
		}
		require.NoError(t, taskLog.Enqueue(ctx, task))
	}

	// The unhealthy endpoint accumulates transport failures until the
	// breaker opens and deliveries stop.
	require.Eventually(t, func() bool { return endpoint.hits.Load() >= 3 }, 10*time.Second, 50*time.Millisecond)

	var plateau int64

	require.Eventually(t, func() bool {
		before := endpoint.hits.Load()
		time.Sleep(600 * time.Millisecond)
		plateau = endpoint.hits.Load()

		return plateau == before
	}, 15*time.Second, 100*time.Millisecond, "an open breaker stops deliveries")

	// No task was archived by the outage: they wait as pending or retry.
	archived, err := tasksInState(taskLog, conveyorv1.TaskState_TASK_STATE_ARCHIVED)
	require.NoError(t, err)
	require.Zero(t, archived, "an unreachable endpoint must not dead-letter tasks")

	// Heal the endpoint; after the open timeout a tick restores capacity
	// and everything completes.
	endpoint.healthy.Store(true)
	time.Sleep(600 * time.Millisecond)

	require.Eventually(t, func() bool {
		tickGatewayNow(t, engine, "hooks")

		count, stateErr := tasksInState(taskLog, conveyorv1.TaskState_TASK_STATE_COMPLETED)

		return stateErr == nil && count == 4
	}, 20*time.Second, 300*time.Millisecond, "a healed endpoint drains the queue")
}
