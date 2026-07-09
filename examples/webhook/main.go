// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

// Command webhook is a minimal Conveyor webhook worker: an HTTP endpoint
// that processes pushed tasks with no SDK, speaking only the JSON-RPC 2.0
// delivery protocol. It verifies the delivery signature and completes each
// task synchronously inside the request.
//
// Usage:
//
//	conveyord --dev                          # terminal 1: the server
//	WEBHOOK_SECRET=s3cret \
//	  go run ./examples/webhook              # terminal 2: this endpoint
//	conveyor webhooks add demo-hooks http://localhost:9090/tasks \
//	  --queue email=1 --secret s3cret        # terminal 3: register it, then enqueue
//
// Configuration comes from WEBHOOK_ADDR (default :9090) and WEBHOOK_SECRET
// (the signing secret the registration was created with). An empty secret
// disables verification, matching a registration created without one.
package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	// defaultAddr is the listen address used when WEBHOOK_ADDR is unset.
	defaultAddr = ":9090"
	// deliveryPath is the route deliveries are POSTed to.
	deliveryPath = "/tasks"
	// signatureScheme prefixes the versioned signature header.
	signatureScheme = "v1="
	// replayWindow bounds how stale a delivery timestamp may be.
	replayWindow = 5 * time.Minute
)

// Delivery-signing headers the server stamps on every request.
const (
	// timestampHeader carries the unix-seconds send time.
	timestampHeader = "X-Conveyor-Timestamp"
	// signatureHeader carries the versioned body signature.
	signatureHeader = "X-Conveyor-Signature"
)

// The JSON-RPC methods the server invokes on the endpoint.
const (
	// methodExecute delivers one task attempt.
	methodExecute = "conveyor.task.execute"
	// methodCancel notifies that a task was canceled; it is a notification,
	// so no response is expected.
	methodCancel = "conveyor.task.cancel"
)

// request is one JSON-RPC delivery. A missing id marks a notification.
type request struct {
	// JSONRPC is always "2.0".
	JSONRPC string `json:"jsonrpc"`
	// ID correlates the response; it is empty on notifications.
	ID string `json:"id"`
	// Method names the invoked operation.
	Method string `json:"method"`
	// Params carries the method arguments, decoded per method.
	Params json.RawMessage `json:"params"`
}

// taskParams are the arguments of one execute call.
type taskParams struct {
	// TaskID is the task ULID; use it as the idempotency key.
	TaskID string `json:"taskId"`
	// Queue is the queue the task was leased from.
	Queue string `json:"queue"`
	// Type is the handler routing key.
	Type string `json:"type"`
	// Attempt is 1 on first delivery and grows with each retry.
	Attempt int `json:"attempt"`
	// Payload is the task payload; base64 in JSON, decoded to bytes here.
	Payload []byte `json:"payload"`
}

// response is one JSON-RPC delivery response.
type response struct {
	// JSONRPC is always "2.0".
	JSONRPC string `json:"jsonrpc"`
	// ID echoes the request id.
	ID string `json:"id"`
	// Result reports a successful outcome; nil on failure.
	Result *result `json:"result,omitempty"`
	// Error reports a failed outcome; nil on success.
	Error *rpcError `json:"error,omitempty"`
}

// result carries a successful delivery outcome.
type result struct {
	// Status is "completed" for synchronous completion.
	Status string `json:"status"`
}

// rpcError carries a failed delivery outcome; code -32000 retries, code
// -32001 skips retry, any other code is treated as an endpoint fault and
// retries.
type rpcError struct {
	// Code selects the retry behavior.
	Code int `json:"code"`
	// Message lands in the task's last error.
	Message string `json:"message"`
}

func main() {
	addr := os.Getenv("WEBHOOK_ADDR")
	if addr == "" {
		addr = defaultAddr
	}

	handler := &endpoint{secret: os.Getenv("WEBHOOK_SECRET")}

	mux := http.NewServeMux()
	mux.HandleFunc(deliveryPath, handler.serve)

	log.Printf("webhook worker listening on %q, delivering %q", addr, deliveryPath)

	if err := http.ListenAndServe(addr, mux); err != nil { //nolint:gosec // demo endpoint; no read timeout needed
		log.Fatalf("webhook: %v", err)
	}
}

// endpoint processes signed deliveries with a single shared secret.
type endpoint struct {
	// secret verifies delivery signatures; empty disables verification.
	secret string
}

// serve verifies and dispatches one delivery.
func (e *endpoint) serve(writer http.ResponseWriter, httpRequest *http.Request) {
	body, err := io.ReadAll(httpRequest.Body)
	if err != nil {
		writer.WriteHeader(http.StatusBadRequest)

		return
	}

	if !e.verify(httpRequest.Header, body) {
		log.Print("rejected delivery: bad signature")
		writer.WriteHeader(http.StatusUnauthorized)

		return
	}

	var delivery request
	if err := json.Unmarshal(body, &delivery); err != nil {
		writer.WriteHeader(http.StatusBadRequest)

		return
	}

	// A notification (no id) expects no response body.
	if delivery.Method == methodCancel {
		var params struct {
			TaskID string `json:"taskId"`
		}

		_ = json.Unmarshal(delivery.Params, &params)
		log.Printf("cancel requested for task %s", params.TaskID)
		writer.WriteHeader(http.StatusOK)

		return
	}

	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(e.execute(&delivery))
}

// execute processes one task attempt and returns its synchronous outcome.
func (e *endpoint) execute(delivery *request) *response {
	var params taskParams
	if err := json.Unmarshal(delivery.Params, &params); err != nil {
		// A payload that cannot decode now never will: skip the retry.
		return &response{
			JSONRPC: "2.0",
			ID:      delivery.ID,
			Error:   &rpcError{Code: -32001, Message: fmt.Sprintf("undecodable params: %v", err)},
		}
	}

	log.Printf("processing task %s (%s, attempt %d): %s", params.TaskID, params.Type, params.Attempt, params.Payload)

	return &response{JSONRPC: "2.0", ID: delivery.ID, Result: &result{Status: "completed"}}
}

// verify checks the delivery signature and replay window. An empty secret
// accepts every delivery, matching an unsigned registration.
func (e *endpoint) verify(header http.Header, body []byte) bool {
	if e.secret == "" {
		return true
	}

	timestamp := header.Get(timestampHeader)

	seconds, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil {
		return false
	}

	if age := time.Since(time.Unix(seconds, 0)); age < -replayWindow || age > replayWindow {
		return false
	}

	signature := strings.TrimPrefix(header.Get(signatureHeader), signatureScheme)

	expected := signBody(e.secret, timestamp, body)

	return hmac.Equal([]byte(signature), []byte(expected))
}

// signBody recomputes the hex HMAC-SHA256 of "{timestamp}.{body}" the
// server signed with, keyed by the shared secret.
func signBody(secret, timestamp string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(timestamp))
	mac.Write([]byte("."))
	mac.Write(body)

	return hex.EncodeToString(mac.Sum(nil))
}
