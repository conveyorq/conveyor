// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package webhook

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClassify(t *testing.T) {
	cases := []struct {
		name        string
		response    *Response
		wantOutcome Outcome
		wantMessage string
	}{
		{"nil response", nil, OutcomeTransportFailure, "empty response"},
		{"completed", &Response{Result: &ResultBody{Status: StatusCompleted}}, OutcomeCompleted, ""},
		{"accepted", &Response{Result: &ResultBody{Status: StatusAccepted}}, OutcomeAccepted, ""},
		{"retry code", &Response{Error: &ErrorBody{Code: CodeRetry, Message: "smtp down"}}, OutcomeRetry, "smtp down"},
		{"skip retry code", &Response{Error: &ErrorBody{Code: CodeSkipRetry, Message: "bad payload"}}, OutcomeSkipRetry, "bad payload"},
		{"standard jsonrpc error retries", &Response{Error: &ErrorBody{Code: -32601, Message: "method not found"}}, OutcomeRetry, "method not found"},
		{"unknown positive code retries", &Response{Error: &ErrorBody{Code: 7, Message: "custom"}}, OutcomeRetry, "custom"},
		{"neither result nor error", &Response{}, OutcomeTransportFailure, "response carries neither result nor error"},
		{"unknown status", &Response{Result: &ResultBody{Status: "done"}}, OutcomeTransportFailure, `unknown result status "done"`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			outcome, message := tc.response.Classify()
			require.Equal(t, tc.wantOutcome, outcome)
			require.Equal(t, tc.wantMessage, message)
		})
	}
}

func TestNewExecuteRequestShape(t *testing.T) {
	request := NewExecuteRequest("lease-1", &TaskParams{
		TaskID:      "task-1",
		Queue:       "email",
		Type:        "email:send",
		Attempt:     1,
		MaxRetry:    25,
		ContentType: "application/json",
		Payload:     []byte(`{"to":"a@b.c"}`),
		Metadata:    map[string]string{"tenant": "acme"},
		Lease:       &LeaseParams{Token: "tok", HeartbeatInterval: "30s"},
	})

	encoded, err := json.Marshal(request)
	require.NoError(t, err)

	text := string(encoded)

	// The payload is bytes, so it must ride as base64, and the envelope must
	// carry the protocol constants.
	for _, want := range []string{
		`"jsonrpc":"2.0"`,
		`"id":"lease-1"`,
		`"method":"conveyor.task.execute"`,
		`"taskId":"task-1"`,
		`"payload":"eyJ0byI6ImFAYi5jIn0="`,
		`"heartbeatInterval":"30s"`,
	} {
		require.Contains(t, text, want)
	}
}

func TestNewCancelNotificationHasNoID(t *testing.T) {
	encoded, err := json.Marshal(NewCancelNotification("task-9"))
	require.NoError(t, err)

	text := string(encoded)
	require.NotContains(t, text, `"id"`, "notifications must not carry an id")
	require.Contains(t, text, `"method":"conveyor.task.cancel"`)
	require.Contains(t, text, `"taskId":"task-9"`)
}

func TestDecodeResponsesSingle(t *testing.T) {
	responses, err := decodeResponses([]byte(`{"jsonrpc":"2.0","id":"a","result":{"status":"completed"}}`))
	require.NoError(t, err)
	require.Len(t, responses, 1)
	require.Equal(t, "a", responses[0].ID)
	require.Equal(t, StatusCompleted, responses[0].Result.Status)
}

func TestDecodeResponsesBatch(t *testing.T) {
	body := ` [{"jsonrpc":"2.0","id":"a","result":{"status":"completed"}},
	           {"jsonrpc":"2.0","id":"b","error":{"code":-32001,"message":"no"}}]`

	responses, err := decodeResponses([]byte(body))
	require.NoError(t, err)
	require.Len(t, responses, 2)
	require.Equal(t, CodeSkipRetry, responses[1].Error.Code)
}

func TestDecodeResponsesMalformed(t *testing.T) {
	for _, body := range []string{"", "not json", "[{]"} {
		_, err := decodeResponses([]byte(body))
		require.Error(t, err, "body %q must not decode", body)
	}
}
