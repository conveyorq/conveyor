// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
	"github.com/conveyorq/conveyor/internal/proto/conveyor/v1/conveyorv1connect"
	"github.com/conveyorq/conveyor/internal/webhook"
	"github.com/conveyorq/conveyor/internal/wire"
	conveyor "github.com/conveyorq/conveyor/sdks/go"
)

// webhookE2ETaskCount is the workload size of the webhook end-to-end test.
const webhookE2ETaskCount = 20

// signingEndpoint is a webhook endpoint that verifies each delivery's
// signature and completes it synchronously, recording what it saw.
type signingEndpoint struct {
	// server is the backing test server.
	server *httptest.Server
	// secret verifies the delivery signature.
	secret string

	// mutex guards the recorded fields.
	mutex sync.Mutex
	// completed collects the task ids answered "completed".
	completed map[string]struct{}
	// unsigned counts deliveries whose signature failed verification.
	unsigned int
}

// newSigningEndpoint starts a signature-verifying endpoint and closes it
// with the test.
func newSigningEndpoint(t *testing.T, secret string) *signingEndpoint {
	t.Helper()

	endpoint := &signingEndpoint{secret: secret, completed: map[string]struct{}{}}

	endpoint.server = httptest.NewServer(http.HandlerFunc(endpoint.serve))
	t.Cleanup(endpoint.server.Close)

	return endpoint
}

// serve verifies and completes one delivery.
func (e *signingEndpoint) serve(writer http.ResponseWriter, request *http.Request) {
	body, err := io.ReadAll(request.Body)
	if err != nil {
		writer.WriteHeader(http.StatusBadRequest)

		return
	}

	signed := e.verify(request.Header, body)

	var delivery webhook.Request
	if err := json.Unmarshal(body, &delivery); err != nil {
		writer.WriteHeader(http.StatusBadRequest)

		return
	}

	encoded, _ := json.Marshal(delivery.Params)

	var params webhook.TaskParams
	_ = json.Unmarshal(encoded, &params)

	e.mutex.Lock()
	if signed {
		e.completed[params.TaskID] = struct{}{}
	} else {
		e.unsigned++
	}
	e.mutex.Unlock()

	response := webhook.Response{JSONRPC: webhook.Version, ID: delivery.ID, Result: &webhook.ResultBody{Status: webhook.StatusCompleted}}
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(&response)
}

// verify recomputes the delivery signature over the raw body and checks it
// against the header the server stamped.
func (e *signingEndpoint) verify(header http.Header, body []byte) bool {
	timestamp := header.Get(webhook.TimestampHeader)
	if _, err := strconv.ParseInt(timestamp, 10, 64); err != nil {
		return false
	}

	expected := "v1=" + webhook.SignBody(e.secret, timestamp, body)

	return header.Get(webhook.SignatureHeader) == expected
}

// completedCount reports how many distinct tasks the endpoint completed.
func (e *signingEndpoint) completedCount() int {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	return len(e.completed)
}

// unsignedCount reports how many deliveries failed signature verification.
func (e *signingEndpoint) unsignedCount() int {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	return e.unsigned
}

// TestWebhookWorkerEndToEnd is the webhook acceptance test: a registered
// HTTP endpoint processes pushed tasks over the JSON-RPC protocol on a live
// node, every delivery arrives correctly signed, and each task completes
// without an SDK worker ever connecting.
func TestWebhookWorkerEndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("webhook end-to-end test skipped in -short mode")
	}

	const (
		queue  = "email"
		secret = "e2e-secret"
	)

	baseURL := startNode(t)
	endpoint := newSigningEndpoint(t, secret)

	admin := conveyorv1connect.NewAdminServiceClient(wire.NewH2CClient(), baseURL)

	// Register the endpoint. The upsert reconciles the manager, so the
	// gateway is serving before the first enqueue.
	_, err := admin.UpsertWebhookWorker(context.Background(), connect.NewRequest(&conveyorv1.UpsertWebhookWorkerRequest{
		Worker: &conveyorv1.WebhookWorker{
			Name:        "e2e-hooks",
			Url:         endpoint.server.URL,
			Queues:      map[string]int32{queue: 1},
			Concurrency: 8,
			Secrets:     []string{secret},
		},
	}))
	require.NoError(t, err)

	client, err := conveyor.NewClient(baseURL)
	require.NoError(t, err)

	ctx := context.Background()

	taskIDs := make([]string, 0, webhookE2ETaskCount)

	for userID := range webhookE2ETaskCount {
		payload := conveyor.JSON(map[string]int{"user_id": userID})

		info, err := client.Enqueue(ctx, conveyor.NewTask("email:send", payload),
			conveyor.Queue(queue), conveyor.Retention(time.Hour))
		require.NoError(t, err)

		taskIDs = append(taskIDs, info.ID)
	}

	require.Eventually(t, func() bool {
		return countCompleted(t, client, taskIDs) == webhookE2ETaskCount
	}, time.Minute, 50*time.Millisecond, "every task must complete through webhook delivery")

	require.Equal(t, webhookE2ETaskCount, endpoint.completedCount(), "the endpoint completed every task")
	require.Zero(t, endpoint.unsignedCount(), "every delivery must carry a valid signature")
}
