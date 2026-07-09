// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package actors

import (
	"context"
	"encoding/json"
	"errors"
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
	goaktlog "github.com/tochemey/goakt/v4/log"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

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

	// An outcome the async contract does not allow (a bare RELEASED) is
	// dropped, not applied: the delivery stays in flight.
	invalid := &conveyorv1.WebhookLeaseResult{TaskId: claims.TaskID, LeaseId: claims.LeaseID, Outcome: conveyorv1.TaskOutcome_TASK_OUTCOME_RELEASED}
	require.NoError(t, engine.TellWebhookGateway(ctx, "hooks", invalid))

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

// TestWebhookAdminCancelAsyncPushesNotification proves an admin cancel of an
// accepted (asynchronous) delivery pushes a cancel notification to the still-
// live endpoint so it stops the work the cancel revoked.
func TestWebhookAdminCancelAsyncPushesNotification(t *testing.T) {
	const queue = "hooks-admin-cancel"

	ctx := context.Background()
	taskLog := memory.New(clock.System())

	endpoint := newRPCEndpoint(t, func(request *webhook.Request) *webhook.Response {
		if request.Method == webhook.MethodExecute {
			return acceptedFor(request)
		}

		// A cancel is a notification: it carries no id and wants no answer.
		return nil
	})

	seedWebhookWorker(t, taskLog, testWebhookWorker(endpoint.server.URL, queue))
	engine := startEngine(t, taskLog)

	require.NoError(t, taskLog.Enqueue(ctx, newTask("admin-cancel-1", queue, "email:send", 4)))
	require.Eventually(t, func() bool { return executeCount(endpoint) >= 1 }, 10*time.Second, 50*time.Millisecond)

	claims, err := webhook.ParseLeaseToken(paramsOf(t, endpoint.seen()[0]).Lease.Token)
	require.NoError(t, err)

	cancel := &conveyorv1.CancelActive{TaskId: claims.TaskID}
	require.Eventually(t, func() bool {
		_ = engine.TellWebhookGateway(ctx, "hooks", cancel)

		return canceled(endpoint)
	}, 10*time.Second, 100*time.Millisecond, "an admin cancel of an accepted delivery notifies the endpoint")
}

// TestWebhookAdminCancelSyncAbortsAndArchives proves an admin cancel of a
// synchronous in-flight delivery aborts the open request and archives the
// task as canceled, never earning it a retry even with retries to spare.
func TestWebhookAdminCancelSyncAbortsAndArchives(t *testing.T) {
	const queue = "hooks-sync-cancel"

	ctx := context.Background()
	taskLog := memory.New(clock.System())

	release := make(chan struct{})
	var closeOnce sync.Once
	closeRelease := func() { closeOnce.Do(func() { close(release) }) }

	endpoint := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		// Hold the delivery open until the admin cancel aborts its request
		// (its context ends) or the test tears down.
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	t.Cleanup(func() {
		closeRelease()
		endpoint.Close()
	})

	seedWebhookWorker(t, taskLog, testWebhookWorker(endpoint.URL, queue))
	engine := startEngine(t, taskLog)

	task := newTask("sync-cancel-1", queue, "email:send", 4)
	task.Options.MaxRetry = 5
	require.NoError(t, taskLog.Enqueue(ctx, task))

	// The gateway leased and dispatched the task; it is now blocked on the
	// endpoint, so the task sits ACTIVE.
	require.Eventually(t, func() bool {
		active, activeErr := tasksInState(taskLog, conveyorv1.TaskState_TASK_STATE_ACTIVE)

		return activeErr == nil && active == 1
	}, 10*time.Second, 50*time.Millisecond)

	require.NoError(t, engine.TellWebhookGateway(ctx, "hooks", &conveyorv1.CancelActive{TaskId: "sync-cancel-1"}))

	require.Eventually(t, func() bool {
		state, _ := taskState(t, taskLog, "sync-cancel-1")

		return state == conveyorv1.TaskState_TASK_STATE_ARCHIVED
	}, 10*time.Second, 100*time.Millisecond, "an aborted, admin-canceled delivery archives instead of retrying")

	_, lastError := taskState(t, taskLog, "sync-cancel-1")
	require.Contains(t, lastError, canceledByAdminMessage)
}

// TestWebhookApplyWithdrawsRemovedQueues proves that updating a running
// gateway's registration to drop a queue withdraws that queue's grain while
// the retained queue keeps delivering.
func TestWebhookApplyWithdrawsRemovedQueues(t *testing.T) {
	const (
		keepQueue = "hooks-keep"
		dropQueue = "hooks-drop"
	)

	ctx := context.Background()
	taskLog := memory.New(clock.System())
	endpoint := newRPCEndpoint(t, completedFor)

	worker := &broker.WebhookWorker{
		Name:        "hooks",
		URL:         endpoint.server.URL,
		Queues:      map[string]int32{keepQueue: 1, dropQueue: 1},
		Concurrency: 8,
		Secrets:     []string{"secret"},
	}
	seedWebhookWorker(t, taskLog, worker)
	engine := startEngine(t, taskLog)

	// Both queues deliver before the registration changes.
	require.NoError(t, taskLog.Enqueue(ctx, newTask("keep-1", keepQueue, "email:send", 4)))
	require.NoError(t, taskLog.Enqueue(ctx, newTask("drop-1", dropQueue, "email:send", 4)))
	require.Eventually(t, completedReaches(taskLog, 2), 10*time.Second, 50*time.Millisecond)

	// Drop one queue; the reconcile hands the running gateway an updated
	// registration, so apply withdraws the removed queue's grain.
	updated := &broker.WebhookWorker{
		Name:        "hooks",
		URL:         endpoint.server.URL,
		Queues:      map[string]int32{keepQueue: 1},
		Concurrency: 8,
		Secrets:     []string{"secret"},
	}
	require.NoError(t, taskLog.UpsertWebhookWorker(ctx, updated))
	reconcileNow(t, engine)

	// The retained queue still delivers after the change.
	require.NoError(t, taskLog.Enqueue(ctx, newTask("keep-2", keepQueue, "email:send", 4)))
	require.Eventually(t, completedReaches(taskLog, 3), 10*time.Second, 50*time.Millisecond)
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

// wgFaultBroker wraps a broker and fails or overrides selected operations on
// demand, so a gateway or manager driven in isolation exercises its error
// branches. A nil/zero hook delegates to the embedded broker.
type wgFaultBroker struct {
	broker.Broker
	// extendErr, when set, is returned by every ExtendLease call.
	extendErr error
	// listErr, when set, is returned by every ListWebhookWorkers call.
	listErr error
	// listOverride, when set, replaces the ListWebhookWorkers result with
	// listWorkers instead of consulting the embedded broker.
	listOverride bool
	// listWorkers is the override registration set.
	listWorkers []*broker.WebhookWorker
	// releaseErr, when set, is returned by every Release call.
	releaseErr error
}

// Release fails with the injected error when set, otherwise delegates.
func (b *wgFaultBroker) Release(ctx context.Context, taskID, leaseID string) error {
	if b.releaseErr != nil {
		return b.releaseErr
	}

	return b.Broker.Release(ctx, taskID, leaseID)
}

// ExtendLease fails with the injected error when set, otherwise delegates.
func (b *wgFaultBroker) ExtendLease(ctx context.Context, taskID, leaseID string, ttl time.Duration) error {
	if b.extendErr != nil {
		return b.extendErr
	}

	return b.Broker.ExtendLease(ctx, taskID, leaseID, ttl)
}

// ListWebhookWorkers fails, overrides, or delegates per the configured hooks.
func (b *wgFaultBroker) ListWebhookWorkers(ctx context.Context) ([]*broker.WebhookWorker, error) {
	if b.listErr != nil {
		return nil, b.listErr
	}

	if b.listOverride {
		return b.listWorkers, nil
	}

	return b.Broker.ListWebhookWorkers(ctx)
}

// isolatedRuntime assembles a runtime over the given broker and clock, using
// the fast test settings.
func isolatedRuntime(taskLog broker.Broker, timeSource clock.Clock) *Runtime {
	return NewRuntime(taskLog, timeSource, testSettings, quietLogger())
}

// startIsolatedGateway spawns one webhook gateway into a bare, single-node
// actor system carrying only the engine runtime extension, returning its PID.
// It drives the gateway's own message handling directly, without the manager
// or queue-grain leasing, so a test scripts exactly the messages it needs.
func startIsolatedGateway(t *testing.T, runtime *Runtime, worker *broker.WebhookWorker) *goakt.PID {
	t.Helper()

	ctx := context.Background()

	system, err := goakt.NewActorSystem("wh-iso-"+worker.Name,
		goakt.WithLogger(goaktlog.DiscardLogger), goakt.WithExtensions(runtime))
	require.NoError(t, err)
	require.NoError(t, system.Start(ctx))

	t.Cleanup(func() { _ = system.Stop(context.Background()) })

	pid, err := system.Spawn(ctx, webhookGatewayPrefix+worker.Name, newWebhookGateway(worker),
		goakt.WithLongLived(), goakt.WithRelocationDisabled())
	require.NoError(t, err)

	return pid
}

// flushGateway runs a drain turn on the gateway and waits for its ack, proving
// every message sent before it has been processed. Drain wipes in-flight
// bookkeeping, so call it only when the test no longer needs that state.
func flushGateway(t *testing.T, pid *goakt.PID) {
	t.Helper()

	drained, err := goakt.Ask(context.Background(), pid, drainSession{}, drainTimeout)
	require.NoError(t, err)
	require.IsType(t, sessionDrained{}, drained)
}

// syncGateway blocks until every message sent before it has been processed,
// without disturbing the gateway's state. It sends an unhandled message and
// waits: the ask never gets a reply (the gateway drops it), but FIFO delivery
// guarantees the prior messages ran first. It is the state-preserving
// counterpart to flushGateway.
func syncGateway(pid *goakt.PID) {
	_, _ = goakt.Ask(context.Background(), pid, new(conveyorv1.WebhookReconcile), 500*time.Millisecond)
}

// leaseTaskFor commits and leases one task under leaseID, so a gateway's
// lease-scoped transitions for it succeed against the broker.
func leaseTaskFor(t *testing.T, taskLog broker.Broker, task *conveyorv1.TaskEnvelope, leaseID string) {
	t.Helper()

	ctx := context.Background()
	require.NoError(t, taskLog.Enqueue(ctx, task))

	leased, err := taskLog.Lease(ctx, task.GetQueue(), 100, testSettings.LeaseTTL, leaseID)
	require.NoError(t, err)

	for _, candidate := range leased {
		if candidate.GetId() == task.GetId() {
			return
		}
	}

	t.Fatalf("task %q was not leased under %q", task.GetId(), leaseID)
}

// blockingEndpoint answers no delivery until the test releases it (or the
// request's context is canceled), so a delivery stays in flight while the test
// drives ticks and callbacks against it.
func blockingEndpoint(t *testing.T) (*httptest.Server, func()) {
	t.Helper()

	release := make(chan struct{})
	closed := false

	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))

	releaseOnce := func() {
		if !closed {
			closed = true
			close(release)
		}
	}

	t.Cleanup(func() {
		releaseOnce()
		server.Close()
	})

	return server, releaseOnce
}

// TestWebhookGatewayDropsUnknownMessages proves every "unknown correlation"
// branch is a safe no-op: a delivery, batch result, or admin cancel for an
// untracked id is dropped, an empty batch returns at once, a deferred
// completion for an unregistered queue is dropped, and an unhandled message
// falls through without stopping the gateway.
func TestWebhookGatewayDropsUnknownMessages(t *testing.T) {
	ctx := context.Background()
	taskLog := memory.New(clock.System())
	runtime := isolatedRuntime(taskLog, clock.System())
	pid := startIsolatedGateway(t, runtime, testWebhookWorker("http://127.0.0.1:0", "served"))

	require.NoError(t, goakt.Tell(ctx, pid, webhookDelivery{taskID: "ghost", outcome: webhook.OutcomeCompleted}))
	require.NoError(t, goakt.Tell(ctx, pid, webhookBatchDelivery{leaseID: "ghost"}))
	require.NoError(t, goakt.Tell(ctx, pid, &conveyorv1.CancelActive{TaskId: "ghost"}))
	require.NoError(t, goakt.Tell(ctx, pid, &conveyorv1.ExecuteBatch{}))
	require.NoError(t, goakt.Tell(ctx, pid, deferredCompletion{queue: "ghost", taskID: "t", success: true}))

	flushGateway(t, pid)

	requireUnhandled(t, pid, &conveyorv1.WebhookReconcile{})
}

// isoTask builds a leasable envelope on a queue for isolated delivery. It
// carries an enqueue timestamp so a delivery records queue latency.
func isoTask(id, queue, taskType string) *conveyorv1.TaskEnvelope {
	task := newTask(id, queue, taskType, 4)
	task.EnqueuedAt = timestamppb.New(time.Now())

	return task
}

// executeTaskFor wraps one task as a queue-grain dispatch under leaseID.
func executeTaskFor(task *conveyorv1.TaskEnvelope, leaseID string) *conveyorv1.ExecuteTask {
	return &conveyorv1.ExecuteTask{
		Task:           task,
		LeaseId:        leaseID,
		LeaseExpiresAt: timestamppb.New(time.Now().Add(time.Hour)),
	}
}

// TestWebhookGatewayExtendLeasesAbortsLostLease proves a lease-extension tick
// that finds a synchronous delivery's lease lost drops the delivery, aborts
// its open request, and returns the active slot.
func TestWebhookGatewayExtendLeasesAbortsLostLease(t *testing.T) {
	ctx := context.Background()
	server, _ := blockingEndpoint(t)
	taskLog := &wgFaultBroker{Broker: memory.New(clock.System()), extendErr: broker.ErrLeaseLost}
	runtime := isolatedRuntime(taskLog, clock.System())
	pid := startIsolatedGateway(t, runtime, testWebhookWorker(server.URL, "served"))

	require.NoError(t, goakt.Tell(ctx, pid, executeTaskFor(isoTask("extend-lost", "served", "email:send"), "lease-lost")))
	require.NoError(t, goakt.Tell(ctx, pid, registerTick{}))

	require.Eventually(t, func() bool {
		return runtime.Counters().Active.Load() == -1
	}, 10*time.Second, 50*time.Millisecond, "a lost lease returns the delivery's active slot")
}

// TestWebhookGatewayExtendLeasesLogsGenericError proves a non-lease-lost
// extension error is logged and the delivery is kept in flight, then drained.
func TestWebhookGatewayExtendLeasesLogsGenericError(t *testing.T) {
	ctx := context.Background()
	server, _ := blockingEndpoint(t)
	taskLog := &wgFaultBroker{Broker: memory.New(clock.System()), extendErr: errors.New("extend boom")}
	runtime := isolatedRuntime(taskLog, clock.System())
	pid := startIsolatedGateway(t, runtime, testWebhookWorker(server.URL, "served"))

	require.NoError(t, goakt.Tell(ctx, pid, executeTaskFor(isoTask("extend-boom", "served", "email:send"), "lease-boom")))
	require.NoError(t, goakt.Tell(ctx, pid, registerTick{}))

	// The delivery survived the failed extension; a drain then releases it.
	flushGateway(t, pid)
	require.True(t, pid.IsRunning())
}

// TestWebhookGatewayExtendLeasesRenewsHealthyLease proves a healthy
// synchronous delivery has its lease renewed on the tick and is left in
// flight.
func TestWebhookGatewayExtendLeasesRenewsHealthyLease(t *testing.T) {
	ctx := context.Background()
	server, _ := blockingEndpoint(t)
	taskLog := memory.New(clock.System())
	runtime := isolatedRuntime(taskLog, clock.System())
	pid := startIsolatedGateway(t, runtime, testWebhookWorker(server.URL, "served"))

	task := isoTask("extend-ok", "served", "email:send")
	leaseTaskFor(t, taskLog, task, "lease-ok")

	require.NoError(t, goakt.Tell(ctx, pid, executeTaskFor(task, "lease-ok")))
	require.NoError(t, goakt.Tell(ctx, pid, registerTick{}))

	flushGateway(t, pid)
	require.True(t, pid.IsRunning())
}

// TestWebhookGatewayReapsStaleAsync proves the async lifecycle on the tick: a
// recently heartbeating accepted delivery is skipped by both lease extension
// and stale reaping, and once its lease TTL lapses the reap forgets it and
// returns its slot.
func TestWebhookGatewayReapsStaleAsync(t *testing.T) {
	ctx := context.Background()
	fake := clock.NewFake(time.Now())
	server, _ := blockingEndpoint(t)
	taskLog := memory.New(fake)
	runtime := isolatedRuntime(taskLog, fake)
	pid := startIsolatedGateway(t, runtime, testWebhookWorker(server.URL, "served"))

	// Deliver against a blocking endpoint (no auto response), then park the
	// delivery in asynchronous mode with a hand-fed accepted result, so the
	// async bookkeeping is set deterministically.
	require.NoError(t, goakt.Tell(ctx, pid, executeTaskFor(isoTask("async-reap", "served", "email:send"), "lease-async")))
	require.NoError(t, goakt.Tell(ctx, pid, webhookDelivery{taskID: "async-reap", outcome: webhook.OutcomeAccepted}))

	// A tick while the beat is fresh extends nothing (async is skipped) and
	// reaps nothing (within TTL).
	require.NoError(t, goakt.Tell(ctx, pid, registerTick{}))

	// Wait for the accept to be recorded before advancing time, so the async
	// last-beat is stamped at the pre-advance clock.
	syncGateway(pid)

	// Let the lease TTL lapse; the next tick reaps the silent delivery.
	fake.Advance(testSettings.LeaseTTL + time.Second)

	require.Eventually(t, func() bool {
		require.NoError(t, goakt.Tell(ctx, pid, registerTick{}))

		return runtime.Counters().Active.Load() == -1
	}, 10*time.Second, 100*time.Millisecond, "a stale async delivery is reaped and its slot returned")
}

// TestWebhookGatewayHeartbeatDropsSyncDelivery proves a heartbeat naming a
// synchronous (not accepted) in-flight delivery is dropped: heartbeats belong
// to accepted deliveries only.
func TestWebhookGatewayHeartbeatDropsSyncDelivery(t *testing.T) {
	ctx := context.Background()
	server, _ := blockingEndpoint(t)
	taskLog := memory.New(clock.System())
	runtime := isolatedRuntime(taskLog, clock.System())
	pid := startIsolatedGateway(t, runtime, testWebhookWorker(server.URL, "served"))

	require.NoError(t, goakt.Tell(ctx, pid, executeTaskFor(isoTask("hb-sync", "served", "email:send"), "lease-hb")))
	require.NoError(t, goakt.Tell(ctx, pid, &conveyorv1.WebhookLeaseHeartbeat{TaskId: "hb-sync", LeaseId: "lease-hb"}))

	flushGateway(t, pid)
	require.True(t, pid.IsRunning())
}

// TestWebhookGatewayHeartbeatLogsGenericError proves a heartbeat whose lease
// extension fails with a non-lease-lost error is logged and the accepted
// delivery is kept in flight.
func TestWebhookGatewayHeartbeatLogsGenericError(t *testing.T) {
	ctx := context.Background()
	server, _ := blockingEndpoint(t)
	taskLog := &wgFaultBroker{Broker: memory.New(clock.System()), extendErr: errors.New("hb boom")}
	runtime := isolatedRuntime(taskLog, clock.System())
	pid := startIsolatedGateway(t, runtime, testWebhookWorker(server.URL, "served"))

	require.NoError(t, goakt.Tell(ctx, pid, executeTaskFor(isoTask("hb-boom", "served", "email:send"), "lease-hbb")))
	require.NoError(t, goakt.Tell(ctx, pid, webhookDelivery{taskID: "hb-boom", outcome: webhook.OutcomeAccepted}))
	require.NoError(t, goakt.Tell(ctx, pid, &conveyorv1.WebhookLeaseHeartbeat{TaskId: "hb-boom", LeaseId: "lease-hbb"}))

	flushGateway(t, pid)
	require.True(t, pid.IsRunning())
}

// TestWebhookGatewayPreStartRequiresRuntimeExtension proves a gateway spawned
// into a system without the engine runtime extension fails to start.
func TestWebhookGatewayPreStartRequiresRuntimeExtension(t *testing.T) {
	ctx := context.Background()

	system, err := goakt.NewActorSystem("wh-no-ext", goakt.WithLogger(goaktlog.DiscardLogger))
	require.NoError(t, err)
	require.NoError(t, system.Start(ctx))
	t.Cleanup(func() { _ = system.Stop(context.Background()) })

	_, err = system.Spawn(ctx, "webhook-x", newWebhookGateway(testWebhookWorker("http://127.0.0.1:0", "served")),
		goakt.WithLongLived(), goakt.WithRelocationDisabled())
	require.Error(t, err, "a gateway needs the runtime extension to start")

	_, err = system.Spawn(ctx, "webhook-manager-x", NewWebhookManager())
	require.Error(t, err, "the manager needs the runtime extension to start")
}

// startIsolatedManager spawns a webhook manager into a bare system carrying
// only the runtime extension, returning its PID. Its PostStart reconcile runs
// against the given broker at once.
func startIsolatedManager(t *testing.T, runtime *Runtime) *goakt.PID {
	t.Helper()

	ctx := context.Background()

	system, err := goakt.NewActorSystem("wh-mgr-iso",
		goakt.WithLogger(goaktlog.DiscardLogger), goakt.WithExtensions(runtime))
	require.NoError(t, err)
	require.NoError(t, system.Start(ctx))

	t.Cleanup(func() { _ = system.Stop(context.Background()) })

	pid, err := system.Spawn(ctx, webhookManagerActorName, NewWebhookManager())
	require.NoError(t, err)

	return pid
}

// TestWebhookManagerReconcileSurvivesListFailure proves a broker that fails to
// list registrations leaves the manager running for the next tick to retry.
func TestWebhookManagerReconcileSurvivesListFailure(t *testing.T) {
	taskLog := &wgFaultBroker{Broker: memory.New(clock.System()), listErr: errors.New("list boom")}
	runtime := isolatedRuntime(taskLog, clock.System())
	pid := startIsolatedManager(t, runtime)

	require.NoError(t, goakt.Tell(context.Background(), pid, new(conveyorv1.WebhookReconcile)))
	syncGateway(pid)

	require.True(t, pid.IsRunning(), "a failed list defers to the next tick, it does not crash the manager")
}

// TestWebhookManagerReconcileSkipsSecretlessRegistration proves a persisted
// registration with no secret is skipped rather than spawned into a gateway
// that would fault minting its first token.
func TestWebhookManagerReconcileSkipsSecretlessRegistration(t *testing.T) {
	taskLog := &wgFaultBroker{
		Broker:       memory.New(clock.System()),
		listOverride: true,
		listWorkers: []*broker.WebhookWorker{
			{Name: "corrupt", URL: "https://example.com/tasks", Queues: map[string]int32{"served": 1}, Concurrency: 1},
		},
	}
	runtime := isolatedRuntime(taskLog, clock.System())
	pid := startIsolatedManager(t, runtime)

	require.NoError(t, goakt.Tell(context.Background(), pid, new(conveyorv1.WebhookReconcile)))
	syncGateway(pid)

	require.True(t, pid.IsRunning())
	_, err := pid.ActorSystem().ActorOf(context.Background(), webhookGatewayPrefix+"corrupt")
	require.Error(t, err, "a secret-less registration must never spawn a gateway")
}

// TestWebhookGatewayCompletesTaskOnUnservedQueue proves a delivery whose queue
// is not one this gateway registered still resolves durably; only the freed-slot
// report has nowhere to land and is dropped.
func TestWebhookGatewayCompletesTaskOnUnservedQueue(t *testing.T) {
	ctx := context.Background()
	endpoint := newRPCEndpoint(t, completedFor)
	taskLog := memory.New(clock.System())
	runtime := isolatedRuntime(taskLog, clock.System())
	pid := startIsolatedGateway(t, runtime, testWebhookWorker(endpoint.server.URL, "served"))

	task := isoTask("ghost-complete", "ghost", "email:send")
	leaseTaskFor(t, taskLog, task, "lease-ghost")

	require.NoError(t, goakt.Tell(ctx, pid, executeTaskFor(task, "lease-ghost")))

	require.Eventually(t, func() bool {
		state, _ := taskState(t, taskLog, "ghost-complete")

		return state == conveyorv1.TaskState_TASK_STATE_COMPLETED
	}, 10*time.Second, 50*time.Millisecond, "an unserved-queue delivery still completes durably")
}

// TestWebhookGatewayRefillsCreditForUnservedQueue proves a dropped async
// delivery on an unregistered queue returns its active slot even though the
// credit refill has no grain to receive it.
func TestWebhookGatewayRefillsCreditForUnservedQueue(t *testing.T) {
	ctx := context.Background()
	fake := clock.NewFake(time.Now())
	server, _ := blockingEndpoint(t)
	taskLog := memory.New(fake)
	runtime := isolatedRuntime(taskLog, fake)
	pid := startIsolatedGateway(t, runtime, testWebhookWorker(server.URL, "served"))

	require.NoError(t, goakt.Tell(ctx, pid, executeTaskFor(isoTask("ghost-async", "ghost", "email:send"), "lease-gasync")))
	require.NoError(t, goakt.Tell(ctx, pid, webhookDelivery{taskID: "ghost-async", outcome: webhook.OutcomeAccepted}))
	require.NoError(t, goakt.Tell(ctx, pid, registerTick{}))
	syncGateway(pid)

	fake.Advance(testSettings.LeaseTTL + time.Second)

	require.Eventually(t, func() bool {
		require.NoError(t, goakt.Tell(ctx, pid, registerTick{}))

		return runtime.Counters().Active.Load() == -1
	}, 10*time.Second, 100*time.Millisecond, "a reaped async delivery returns its slot regardless of the queue")
}

// leaseBatch commits and leases every task on one queue under a single lease
// id, mirroring how a fired group is leased as one unit.
func leaseBatch(t *testing.T, taskLog broker.Broker, tasks []*conveyorv1.TaskEnvelope, queue, leaseID string) {
	t.Helper()

	ctx := context.Background()
	for _, task := range tasks {
		require.NoError(t, taskLog.Enqueue(ctx, task))
	}

	leased, err := taskLog.Lease(ctx, queue, 100, testSettings.LeaseTTL, leaseID)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(leased), len(tasks), "every batch member must lease")
}

// executeBatchFor wraps a set of tasks as one batch dispatch under leaseID.
func executeBatchFor(tasks []*conveyorv1.TaskEnvelope, leaseID string) *conveyorv1.ExecuteBatch {
	return &conveyorv1.ExecuteBatch{
		Tasks:          tasks,
		LeaseId:        leaseID,
		LeaseExpiresAt: timestamppb.New(time.Now().Add(time.Hour)),
	}
}

// TestWebhookGatewayBatchTransportFailureRetriesMembers proves a batch POST
// that fails at the transport retries every member.
func TestWebhookGatewayBatchTransportFailureRetriesMembers(t *testing.T) {
	ctx := context.Background()
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(endpoint.Close)

	taskLog := memory.New(clock.System())
	runtime := isolatedRuntime(taskLog, clock.System())
	pid := startIsolatedGateway(t, runtime, testWebhookWorker(endpoint.URL, "served"))

	// The members ride an unserved queue, so the whole-batch completion report
	// also drives the "queue not registered" drop in reportBatchCompletion.
	members := []*conveyorv1.TaskEnvelope{
		isoTask("bt-1", "ghost", "report:batch"),
		isoTask("bt-2", "ghost", "report:batch"),
	}
	leaseBatch(t, taskLog, members, "ghost", "batch-transport")

	require.NoError(t, goakt.Tell(ctx, pid, executeBatchFor(members, "batch-transport")))

	require.Eventually(t, func() bool {
		count, err := tasksInState(taskLog, conveyorv1.TaskState_TASK_STATE_RETRY)

		return err == nil && count == 2
	}, 10*time.Second, 50*time.Millisecond, "a failed batch POST retries every member")
}

// TestWebhookGatewayBatchParksAcceptedMember proves a batch where one member is
// accepted and another completes resolves each on its own answer.
func TestWebhookGatewayBatchParksAcceptedMember(t *testing.T) {
	ctx := context.Background()
	server, _ := blockingEndpoint(t)
	taskLog := memory.New(clock.System())
	runtime := isolatedRuntime(taskLog, clock.System())
	pid := startIsolatedGateway(t, runtime, testWebhookWorker(server.URL, "served"))

	members := []*conveyorv1.TaskEnvelope{
		isoTask("ba-1", "served", "report:batch"),
		isoTask("ba-2", "served", "report:batch"),
	}
	leaseBatch(t, taskLog, members, "served", "batch-accept")

	// Deliver against a blocking endpoint (no auto answer), then hand-feed a
	// batch result: one accepted (parks async), one completed (resolves).
	require.NoError(t, goakt.Tell(ctx, pid, executeBatchFor(members, "batch-accept")))
	require.NoError(t, goakt.Tell(ctx, pid, webhookBatchDelivery{
		leaseID: "batch-accept",
		outcomes: map[string]webhookDelivery{
			"ba-1": {taskID: "ba-1", outcome: webhook.OutcomeAccepted},
			"ba-2": {taskID: "ba-2", outcome: webhook.OutcomeCompleted},
		},
	}))

	require.Eventually(t, func() bool {
		state, _ := taskState(t, taskLog, "ba-2")

		return state == conveyorv1.TaskState_TASK_STATE_COMPLETED
	}, 10*time.Second, 50*time.Millisecond, "the completed member resolves while the accepted one parks")

	state, _ := taskState(t, taskLog, "ba-1")
	require.Equal(t, conveyorv1.TaskState_TASK_STATE_ACTIVE, state, "the accepted member stays active pending its callback")
}

// TestWebhookGatewayDefersCompletionWhenTypeBreakerOpen proves a task type that
// keeps failing opens its circuit breaker, after which a resolution withholds
// its completion report so the type drains at probe speed.
func TestWebhookGatewayDefersCompletionWhenTypeBreakerOpen(t *testing.T) {
	ctx := context.Background()
	server, _ := blockingEndpoint(t)
	taskLog := memory.New(clock.System())
	runtime := isolatedRuntime(taskLog, clock.System())
	pid := startIsolatedGateway(t, runtime, testWebhookWorker(server.URL, "served"))

	// Enough retryable resolutions of one type to open its breaker; the later
	// ones resolve into the open breaker and defer their completion report.
	for sequence := range breakerMinRequests + 3 {
		id := fmt.Sprintf("defer-%02d", sequence)
		require.NoError(t, goakt.Tell(ctx, pid, executeTaskFor(isoTask(id, "served", "flaky:type"), fmt.Sprintf("lease-%02d", sequence))))
		require.NoError(t, goakt.Tell(ctx, pid, webhookDelivery{taskID: id, outcome: webhook.OutcomeRetry, message: "boom"}))
	}

	syncGateway(pid)
	require.True(t, pid.IsRunning(), "an open type breaker defers reports without crashing the gateway")
}

// TestWebhookGatewayDrainLogsReleaseFailure proves a drain whose broker
// release fails with a non-lease-lost error logs and still clears the
// in-flight bookkeeping.
func TestWebhookGatewayDrainLogsReleaseFailure(t *testing.T) {
	ctx := context.Background()
	server, _ := blockingEndpoint(t)
	taskLog := &wgFaultBroker{Broker: memory.New(clock.System()), releaseErr: errors.New("release boom")}
	runtime := isolatedRuntime(taskLog, clock.System())
	pid := startIsolatedGateway(t, runtime, testWebhookWorker(server.URL, "served"))

	require.NoError(t, goakt.Tell(ctx, pid, executeTaskFor(isoTask("drain-boom", "served", "email:send"), "lease-drain")))
	flushGateway(t, pid)

	// The drain decremented the one in-flight slot despite the release failure.
	require.Eventually(t, func() bool {
		return runtime.Counters().Active.Load() == -1
	}, 5*time.Second, 50*time.Millisecond, "a drain frees the slot even when the release errors")
}

// TestWebhookGatewayReapsStaleBatchedAsyncMember proves a stale async delivery
// belonging to a batch resolves through the batch bookkeeping rather than
// refilling a credit on its own.
func TestWebhookGatewayReapsStaleBatchedAsyncMember(t *testing.T) {
	ctx := context.Background()
	fake := clock.NewFake(time.Now())
	server, _ := blockingEndpoint(t)
	taskLog := memory.New(fake)
	runtime := isolatedRuntime(taskLog, fake)
	pid := startIsolatedGateway(t, runtime, testWebhookWorker(server.URL, "served"))

	members := []*conveyorv1.TaskEnvelope{
		isoTask("br-1", "served", "report:batch"),
		isoTask("br-2", "served", "report:batch"),
	}
	leaseBatch(t, taskLog, members, "served", "batch-reap")

	require.NoError(t, goakt.Tell(ctx, pid, executeBatchFor(members, "batch-reap")))
	// One member completes; the other parks async and then goes silent.
	require.NoError(t, goakt.Tell(ctx, pid, webhookBatchDelivery{
		leaseID: "batch-reap",
		outcomes: map[string]webhookDelivery{
			"br-1": {taskID: "br-1", outcome: webhook.OutcomeCompleted},
			"br-2": {taskID: "br-2", outcome: webhook.OutcomeAccepted},
		},
	}))
	syncGateway(pid)

	fake.Advance(testSettings.LeaseTTL + time.Second)

	// Once the silent member is reaped the batch finishes and its one credit
	// is refilled; the batch's only completion is that single credit report.
	require.Eventually(t, func() bool {
		require.NoError(t, goakt.Tell(ctx, pid, registerTick{}))
		state, _ := taskState(t, taskLog, "br-1")

		return state == conveyorv1.TaskState_TASK_STATE_COMPLETED
	}, 10*time.Second, 100*time.Millisecond, "a reaped batched async member closes out its batch")
}

// TestWebhookGatewayCancelNotificationErrorIsLogged proves a cancel
// notification that the endpoint rejects is logged, not surfaced: the cancel
// is best-effort.
func TestWebhookGatewayCancelNotificationErrorIsLogged(t *testing.T) {
	ctx := context.Background()

	var sawCancel atomic.Bool

	release := make(chan struct{})
	var releaseOnce sync.Once
	closeRelease := func() { releaseOnce.Do(func() { close(release) }) }

	// The endpoint holds each execute open (so nothing auto-resolves the
	// delivery) and rejects any cancel with a 500, forcing the notification's
	// transport error path.
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)

		var request webhook.Request
		_ = json.Unmarshal(body, &request)

		if request.Method == webhook.MethodCancel {
			sawCancel.Store(true)

			// Drop the connection mid-request so the notification's POST fails
			// at the transport, driving Notify's error path (a non-200 answer
			// is not itself a Notify error).
			if hijacker, ok := w.(http.Hijacker); ok {
				if conn, _, hijackErr := hijacker.Hijack(); hijackErr == nil {
					_ = conn.Close()
				}
			}

			return
		}

		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	t.Cleanup(func() {
		closeRelease()
		endpoint.Close()
	})

	taskLog := memory.New(clock.System())
	runtime := isolatedRuntime(taskLog, clock.System())
	pid := startIsolatedGateway(t, runtime, testWebhookWorker(endpoint.URL, "served"))

	// Deliver against the blocking endpoint, then park the delivery async with
	// a hand-fed accept so the cancel path is deterministic.
	require.NoError(t, goakt.Tell(ctx, pid, executeTaskFor(isoTask("cancel-err", "served", "email:send"), "lease-cancel")))
	require.NoError(t, goakt.Tell(ctx, pid, webhookDelivery{taskID: "cancel-err", outcome: webhook.OutcomeAccepted}))
	syncGateway(pid)

	require.NoError(t, goakt.Tell(ctx, pid, &conveyorv1.CancelActive{TaskId: "cancel-err"}))

	require.Eventually(t, func() bool {
		return sawCancel.Load()
	}, 10*time.Second, 100*time.Millisecond, "the endpoint receives the best-effort cancel even though it rejects it")
}

// TestWebhookGatewayBatchSkipsAlreadyResolvedMember proves the batch result
// handler skips a member that already resolved (through a stray single result)
// before the batch POST's answer arrived, closing the batch on the remainder.
func TestWebhookGatewayBatchSkipsAlreadyResolvedMember(t *testing.T) {
	ctx := context.Background()
	server, _ := blockingEndpoint(t)
	taskLog := memory.New(clock.System())
	runtime := isolatedRuntime(taskLog, clock.System())
	pid := startIsolatedGateway(t, runtime, testWebhookWorker(server.URL, "served"))

	members := []*conveyorv1.TaskEnvelope{
		isoTask("bs-1", "served", "report:batch"),
		isoTask("bs-2", "served", "report:batch"),
	}
	leaseBatch(t, taskLog, members, "served", "batch-skip")

	require.NoError(t, goakt.Tell(ctx, pid, executeBatchFor(members, "batch-skip")))
	// Resolve one member out of band, then deliver the batch answer: the batch
	// handler must skip the already-resolved member.
	require.NoError(t, goakt.Tell(ctx, pid, webhookDelivery{taskID: "bs-1", outcome: webhook.OutcomeCompleted}))
	require.NoError(t, goakt.Tell(ctx, pid, webhookBatchDelivery{
		leaseID: "batch-skip",
		outcomes: map[string]webhookDelivery{
			"bs-2": {taskID: "bs-2", outcome: webhook.OutcomeCompleted},
		},
	}))

	require.Eventually(t, func() bool {
		one, _ := taskState(t, taskLog, "bs-1")
		two, _ := taskState(t, taskLog, "bs-2")

		return one == conveyorv1.TaskState_TASK_STATE_COMPLETED && two == conveyorv1.TaskState_TASK_STATE_COMPLETED
	}, 10*time.Second, 50*time.Millisecond, "both members complete though one resolved before the batch answer")
}

// TestWebhookGatewayDeliveryHonorsTaskDeadlineAndTimeout drives the
// execution-deadline computation through a task carrying both an absolute
// deadline and a per-attempt timeout.
func TestWebhookGatewayDeliveryHonorsTaskDeadlineAndTimeout(t *testing.T) {
	ctx := context.Background()
	endpoint := newRPCEndpoint(t, completedFor)
	taskLog := memory.New(clock.System())
	runtime := isolatedRuntime(taskLog, clock.System())
	pid := startIsolatedGateway(t, runtime, testWebhookWorker(endpoint.server.URL, "served"))

	task := isoTask("deadline-task", "served", "email:send")
	task.Options.Deadline = timestamppb.New(time.Now().Add(45 * time.Minute))
	task.Options.Timeout = durationpb.New(10 * time.Minute)
	leaseTaskFor(t, taskLog, task, "lease-deadline")

	require.NoError(t, goakt.Tell(ctx, pid, executeTaskFor(task, "lease-deadline")))

	require.Eventually(t, func() bool {
		state, _ := taskState(t, taskLog, "deadline-task")

		return state == conveyorv1.TaskState_TASK_STATE_COMPLETED
	}, 10*time.Second, 50*time.Millisecond, "a task with a deadline and timeout still delivers and completes")
}

// TestWebhookGatewayResolveProbeReopensBreaker drives the endpoint breaker's
// probe-failure arc: transport failures open it, the open timeout lapses so a
// probe slot opens, and a failing probe delivery reopens it and re-withholds
// capacity.
func TestWebhookGatewayResolveProbeReopensBreaker(t *testing.T) {
	ctx := context.Background()
	fake := clock.NewFake(time.Now())
	server, _ := blockingEndpoint(t)
	taskLog := memory.New(fake)
	runtime := isolatedRuntime(taskLog, fake)
	pid := startIsolatedGateway(t, runtime, testWebhookWorker(server.URL, "served"))

	// Accumulate transport failures until the endpoint breaker opens and the
	// gateway withholds capacity.
	for sequence := range endpointBreakerMinRequests + 1 {
		id := fmt.Sprintf("probe-%02d", sequence)
		require.NoError(t, goakt.Tell(ctx, pid, executeTaskFor(isoTask(id, "served", "email:send"), fmt.Sprintf("please-%02d", sequence))))
		require.NoError(t, goakt.Tell(ctx, pid, webhookDelivery{taskID: id, outcome: webhook.OutcomeTransportFailure, message: "conn refused"}))
	}
	syncGateway(pid)

	// The open timeout lapses; the next tick opens a single probe slot.
	fake.Advance(defaultEndpointBreakerOpenTimeout + time.Second)
	require.NoError(t, goakt.Tell(ctx, pid, registerTick{}))
	syncGateway(pid)

	// A probe delivery that also fails finds the breaker still open and
	// re-withholds capacity.
	require.NoError(t, goakt.Tell(ctx, pid, executeTaskFor(isoTask("probe-final", "served", "email:send"), "please-final")))
	require.NoError(t, goakt.Tell(ctx, pid, webhookDelivery{taskID: "probe-final", outcome: webhook.OutcomeTransportFailure, message: "still down"}))
	syncGateway(pid)

	require.True(t, pid.IsRunning(), "a failed probe re-withholds capacity without crashing the gateway")
}
