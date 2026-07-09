// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

// Package webhook implements the JSON-RPC 2.0 delivery protocol the server
// speaks to webhook workers: envelope types, response classification, and the
// HTTP client that carries them.
package webhook

import (
	"encoding/json"
	"fmt"
)

// Version is the JSON-RPC protocol version stamped on every envelope.
const Version = "2.0"

// Methods the server invokes on a webhook endpoint.
const (
	// MethodExecute delivers one task attempt.
	MethodExecute = "conveyor.task.execute"
	// MethodCancel notifies the endpoint that a task was canceled; it is a
	// notification, so no response is expected.
	MethodCancel = "conveyor.task.cancel"
)

// Error codes an endpoint uses to select retry behavior; any other code is
// an endpoint fault and retries like a crashed handler.
const (
	// CodeRetry marks a retryable failure.
	CodeRetry = -32000
	// CodeSkipRetry marks a permanent failure that must not retry.
	CodeSkipRetry = -32001
)

// Result statuses an endpoint answers a delivery with.
const (
	// StatusCompleted reports the task finished successfully.
	StatusCompleted = "completed"
	// StatusAccepted reports the endpoint took the task for asynchronous
	// completion: it will heartbeat and report the outcome via callbacks.
	StatusAccepted = "accepted"
)

// Request is one JSON-RPC call to a webhook endpoint.
type Request struct {
	// JSONRPC is always Version.
	JSONRPC string `json:"jsonrpc"`
	// ID correlates the response; it carries the delivery's lease id and is
	// absent on notifications.
	ID string `json:"id,omitempty"`
	// Method names the invoked operation.
	Method string `json:"method"`
	// Params carries the method arguments.
	Params any `json:"params"`
}

// TaskParams are the arguments of one MethodExecute call: everything a
// stream worker receives in a Dispatch frame.
type TaskParams struct {
	// TaskID is the task ULID.
	TaskID string `json:"taskId"`
	// Queue is the queue the task was leased from.
	Queue string `json:"queue"`
	// Type is the handler routing key.
	Type string `json:"type"`
	// Attempt is 1 on first delivery and grows with each retry.
	Attempt int32 `json:"attempt"`
	// MaxRetry is the task's retry budget.
	MaxRetry int32 `json:"maxRetry"`
	// Deadline is the RFC 3339 execution deadline, empty when unbounded.
	Deadline string `json:"deadline,omitempty"`
	// ContentType names the payload codec.
	ContentType string `json:"contentType,omitempty"`
	// Payload is the opaque task payload; base64 in JSON.
	Payload []byte `json:"payload,omitempty"`
	// Metadata carries the task's user tags.
	Metadata map[string]string `json:"metadata,omitempty"`
	// Lease carries the delivery lease an asynchronous completion needs.
	Lease *LeaseParams `json:"lease,omitempty"`
}

// LeaseParams describe the delivery lease of one task.
type LeaseParams struct {
	// Token authenticates Heartbeat and ReportResult callbacks for this
	// delivery only.
	Token string `json:"token,omitempty"`
	// HeartbeatInterval is how often an asynchronously completing endpoint
	// must heartbeat, e.g. "30s".
	HeartbeatInterval string `json:"heartbeatInterval,omitempty"`
}

// CancelParams are the arguments of one MethodCancel notification.
type CancelParams struct {
	// TaskID is the canceled task.
	TaskID string `json:"taskId"`
}

// Response is one JSON-RPC response from a webhook endpoint.
type Response struct {
	// JSONRPC echoes the protocol version.
	JSONRPC string `json:"jsonrpc"`
	// ID echoes the request id.
	ID string `json:"id"`
	// Result is set on success.
	Result *ResultBody `json:"result"`
	// Error is set on failure.
	Error *ErrorBody `json:"error"`
}

// ResultBody is a successful delivery response.
type ResultBody struct {
	// Status is StatusCompleted or StatusAccepted.
	Status string `json:"status"`
}

// ErrorBody is a failed delivery response.
type ErrorBody struct {
	// Code selects the retry behavior (CodeRetry, CodeSkipRetry).
	Code int `json:"code"`
	// Message describes the failure; it lands in the task's last error.
	Message string `json:"message"`
}

// Outcome classifies one delivery response.
type Outcome int

const (
	// OutcomeTransportFailure means no usable response arrived: a non-200
	// status, a malformed envelope, a connection error, or a timeout. The
	// task retries and the failure feeds the endpoint's circuit breaker.
	OutcomeTransportFailure Outcome = iota
	// OutcomeCompleted means the task finished successfully.
	OutcomeCompleted
	// OutcomeAccepted means the endpoint completes the task asynchronously.
	OutcomeAccepted
	// OutcomeRetry means the attempt failed and the task should retry.
	OutcomeRetry
	// OutcomeSkipRetry means the failure is permanent; the task archives.
	OutcomeSkipRetry
)

// NewExecuteRequest builds the delivery call for one task attempt, keyed by
// its lease id.
func NewExecuteRequest(leaseID string, params *TaskParams) *Request {
	return &Request{JSONRPC: Version, ID: leaseID, Method: MethodExecute, Params: params}
}

// NewCancelNotification builds the best-effort cancel notification for one
// task. Notifications carry no id and expect no response.
func NewCancelNotification(taskID string) *Request {
	return &Request{JSONRPC: Version, Method: MethodCancel, Params: &CancelParams{TaskID: taskID}}
}

// Classify maps one response to its outcome and failure message. A response
// that fits neither the result nor the error shape is a transport failure:
// the endpoint did not speak the protocol.
func (r *Response) Classify() (Outcome, string) {
	switch {
	case r == nil:
		return OutcomeTransportFailure, "empty response"

	case r.Error != nil:
		switch r.Error.Code {
		case CodeSkipRetry:
			return OutcomeSkipRetry, r.Error.Message

		default:
			// CodeRetry and every other code, including the JSON-RPC
			// standard errors: a broken handler retries like a crashed one.
			return OutcomeRetry, r.Error.Message
		}

	case r.Result == nil:
		return OutcomeTransportFailure, "response carries neither result nor error"

	case r.Result.Status == StatusCompleted:
		return OutcomeCompleted, ""

	case r.Result.Status == StatusAccepted:
		return OutcomeAccepted, ""

	default:
		return OutcomeTransportFailure, fmt.Sprintf("unknown result status %q", r.Result.Status)
	}
}

// decodeResponses parses a response body that may be a single response or a
// batch (array) of responses.
func decodeResponses(body []byte) ([]*Response, error) {
	trimmed := firstNonSpace(body)

	if trimmed == '[' {
		var batch []*Response
		if err := json.Unmarshal(body, &batch); err != nil {
			return nil, fmt.Errorf("webhook: malformed batch response: %w", err)
		}

		return batch, nil
	}

	var single Response
	if err := json.Unmarshal(body, &single); err != nil {
		return nil, fmt.Errorf("webhook: malformed response: %w", err)
	}

	return []*Response{&single}, nil
}

// firstNonSpace returns the first non-whitespace byte of body, or zero when
// none exists.
func firstNonSpace(body []byte) byte {
	for _, c := range body {
		switch c {
		case ' ', '\t', '\r', '\n':
			continue
		}

		return c
	}

	return 0
}
