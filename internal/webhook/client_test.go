// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package webhook

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCallReturnsParsedResponse(t *testing.T) {
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request Request
		require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
		require.Equal(t, MethodExecute, request.Method)
		require.Equal(t, "application/json", r.Header.Get("Content-Type"))

		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":"` + request.ID + `","result":{"status":"completed"}}`))
	}))
	defer endpoint.Close()

	response, err := NewClient().Call(context.Background(), endpoint.URL, nil, NewExecuteRequest("lease-1", &TaskParams{TaskID: "t1"}))
	require.NoError(t, err)

	outcome, _ := response.Classify()
	require.Equal(t, OutcomeCompleted, outcome)
}

func TestCallBatchKeysResponsesByID(t *testing.T) {
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`[
			{"jsonrpc":"2.0","id":"lease-a","result":{"status":"completed"}},
			{"jsonrpc":"2.0","id":"lease-b","error":{"code":-32000,"message":"later"}}
		]`))
	}))
	defer endpoint.Close()

	requests := []*Request{
		NewExecuteRequest("lease-a", &TaskParams{TaskID: "a"}),
		NewExecuteRequest("lease-b", &TaskParams{TaskID: "b"}),
	}

	responses, err := NewClient().CallBatch(context.Background(), endpoint.URL, nil, requests)
	require.NoError(t, err)
	require.Len(t, responses, 2)

	outcome, _ := responses["lease-a"].Classify()
	require.Equal(t, OutcomeCompleted, outcome)

	outcome, message := responses["lease-b"].Classify()
	require.Equal(t, OutcomeRetry, outcome)
	require.Equal(t, "later", message)
}

func TestCallTransportFailures(t *testing.T) {
	cases := []struct {
		name    string
		handler http.HandlerFunc
	}{
		{"non-200", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusBadGateway) }},
		{"malformed body", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("nope")) }},
		{"redirect", func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, "https://elsewhere.example", http.StatusFound)
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			endpoint := httptest.NewServer(tc.handler)
			defer endpoint.Close()

			_, err := NewClient().Call(context.Background(), endpoint.URL, nil, NewExecuteRequest("l", &TaskParams{}))
			require.Error(t, err, "expected a transport failure")
		})
	}
}

func TestCallRejectsMultipleResponses(t *testing.T) {
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`[
			{"jsonrpc":"2.0","id":"a","result":{"status":"completed"}},
			{"jsonrpc":"2.0","id":"b","result":{"status":"completed"}}
		]`))
	}))
	defer endpoint.Close()

	_, err := NewClient().Call(context.Background(), endpoint.URL, nil, NewExecuteRequest("l", &TaskParams{}))
	require.Error(t, err, "a single Call expects exactly one response")
}

func TestCallBatchReportsTransportFailure(t *testing.T) {
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer endpoint.Close()

	requests := []*Request{NewExecuteRequest("a", &TaskParams{TaskID: "a"})}

	_, err := NewClient().CallBatch(context.Background(), endpoint.URL, nil, requests)
	require.Error(t, err, "a non-200 answer fails the whole batch POST")
}

func TestCallBatchDropsResponsesWithoutID(t *testing.T) {
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// The second member carries no id: it cannot be correlated and is
		// dropped from the keyed map.
		_, _ = w.Write([]byte(`[
			{"jsonrpc":"2.0","id":"a","result":{"status":"completed"}},
			{"jsonrpc":"2.0","id":"","result":{"status":"completed"}}
		]`))
	}))
	defer endpoint.Close()

	requests := []*Request{
		NewExecuteRequest("a", &TaskParams{TaskID: "a"}),
		NewExecuteRequest("b", &TaskParams{TaskID: "b"}),
	}

	responses, err := NewClient().CallBatch(context.Background(), endpoint.URL, nil, requests)
	require.NoError(t, err)
	require.Len(t, responses, 1, "an id-less response cannot be correlated")
	require.Contains(t, responses, "a")
}

// TestCallReportsConnectionFailure proves a dial failure to a closed endpoint
// is surfaced as a transport error.
func TestCallReportsConnectionFailure(t *testing.T) {
	endpoint := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	url := endpoint.URL
	endpoint.Close() // Nothing listens now; the dial fails.

	_, err := NewClient().Call(context.Background(), url, nil, NewExecuteRequest("l", &TaskParams{}))
	require.Error(t, err, "a dial to a closed endpoint must fail")
}

// TestNotifyReportsTransportFailure proves Notify surfaces a transport error
// rather than swallowing it.
func TestNotifyReportsTransportFailure(t *testing.T) {
	endpoint := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	url := endpoint.URL
	endpoint.Close()

	err := NewClient().Notify(context.Background(), url, nil, NewCancelNotification("t1"))
	require.Error(t, err, "a dial to a closed endpoint must fail the notification")
}

// TestSendRejectsInvalidURL proves a malformed URL fails at request building,
// before any dial.
func TestSendRejectsInvalidURL(t *testing.T) {
	_, err := NewClient().Call(context.Background(), "://not-a-url", nil, NewExecuteRequest("l", &TaskParams{}))
	require.Error(t, err, "a malformed URL must fail request construction")
}

func TestCallHonorsContextDeadline(t *testing.T) {
	release := make(chan struct{})
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		<-release
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":"l","result":{"status":"completed"}}`))
	}))

	defer func() {
		close(release)
		endpoint.Close()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err := NewClient().Call(ctx, endpoint.URL, nil, NewExecuteRequest("l", &TaskParams{}))
	require.Error(t, err, "expected a deadline failure")
}

func TestNotifyIgnoresResponseBody(t *testing.T) {
	var method string

	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request Request
		_ = json.NewDecoder(r.Body).Decode(&request)
		method = request.Method
		_, _ = w.Write([]byte("anything"))
	}))
	defer endpoint.Close()

	require.NoError(t, NewClient().Notify(context.Background(), endpoint.URL, nil, NewCancelNotification("t1")))
	require.Equal(t, MethodCancel, method)
}

// headerSigner stamps a fixed header, standing in for the HMAC signer.
type headerSigner struct {
	// value is the stamped signature value.
	value string
}

// Sign implements Signer.
func (s headerSigner) Sign(header http.Header, _ []byte) { header.Set("X-Test-Signature", s.value) }

func TestCallAppliesSigner(t *testing.T) {
	var signature string

	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		signature = r.Header.Get("X-Test-Signature")
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":"l","result":{"status":"completed"}}`))
	}))
	defer endpoint.Close()

	_, err := NewClient().Call(context.Background(), endpoint.URL, headerSigner{value: "sig"}, NewExecuteRequest("l", &TaskParams{}))
	require.NoError(t, err)
	require.Equal(t, "sig", signature, "signer headers must reach the endpoint")
}
