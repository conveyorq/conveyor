// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

// maxResponseBytes bounds how much of an endpoint's response body is read; a
// response is a small JSON envelope, so anything larger is misbehavior.
const maxResponseBytes = 1 << 20

// defaultDialTimeout bounds connection setup when a call carries no
// deadline of its own (a cancel notification).
const defaultDialTimeout = 10 * time.Second

// errRedirect rejects redirect responses: a registered URL is delivered to
// exactly as configured, never followed elsewhere.
var errRedirect = errors.New("webhook: endpoint redirected; redirects are not followed")

// Signer stamps authentication headers onto one delivery request. The
// signing scheme lives with the registration secrets; a nil signer sends
// unsigned requests.
type Signer interface {
	// Sign adds authentication headers derived from the request body.
	Sign(header http.Header, body []byte)
}

// Client delivers JSON-RPC calls to webhook endpoints. One client is shared
// by all deliveries of one gateway; per-call deadlines come from the caller's
// context.
type Client struct {
	// http performs the requests; it never follows redirects.
	http *http.Client
}

// NewClient returns a delivery client.
func NewClient() *Client {
	return &Client{
		http: &http.Client{
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return errRedirect
			},
		},
	}
}

// Call POSTs one request and returns the parsed response. Every failure
// short of a parsed JSON-RPC response (connection error, deadline, non-200
// status, malformed envelope) is returned as an error: a transport failure,
// never an outcome.
func (c *Client) Call(ctx context.Context, url string, signer Signer, request *Request) (*Response, error) {
	responses, err := c.post(ctx, url, signer, request)
	if err != nil {
		return nil, err
	}

	if len(responses) != 1 {
		return nil, fmt.Errorf("webhook: expected one response, got %d", len(responses))
	}

	return responses[0], nil
}

// CallBatch POSTs a JSON-RPC batch (one array-bodied request) and returns
// the responses keyed by request id. Members the endpoint omitted are simply
// absent from the map.
func (c *Client) CallBatch(ctx context.Context, url string, signer Signer, requests []*Request) (map[string]*Response, error) {
	responses, err := c.post(ctx, url, signer, requests)
	if err != nil {
		return nil, err
	}

	byID := make(map[string]*Response, len(responses))

	for _, response := range responses {
		if response != nil && response.ID != "" {
			byID[response.ID] = response
		}
	}

	return byID, nil
}

// Notify POSTs one notification and discards the response body; only
// transport failures are reported.
func (c *Client) Notify(ctx context.Context, url string, signer Signer, request *Request) error {
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, defaultDialTimeout)
		defer cancel()
	}

	body, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf("webhook: marshal notification: %w", err)
	}

	response, err := c.send(ctx, url, signer, body)
	if err != nil {
		return err
	}

	defer func() { _ = response.Body.Close() }()

	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, maxResponseBytes))

	return nil
}

// post marshals payload (a request or a batch), sends it, and parses the
// response envelope(s).
func (c *Client) post(ctx context.Context, url string, signer Signer, payload any) ([]*Response, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("webhook: marshal request: %w", err)
	}

	response, err := c.send(ctx, url, signer, body)
	if err != nil {
		return nil, err
	}

	defer func() { _ = response.Body.Close() }()

	responseBody, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes))
	if err != nil {
		return nil, fmt.Errorf("webhook: reading response: %w", err)
	}

	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("webhook: endpoint answered HTTP %d", response.StatusCode)
	}

	return decodeResponses(responseBody)
}

// send performs the signed POST.
func (c *Client) send(ctx context.Context, url string, signer Signer, body []byte) (*http.Response, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("webhook: building request: %w", err)
	}

	request.Header.Set("Content-Type", "application/json")

	if signer != nil {
		signer.Sign(request.Header, body)
	}

	response, err := c.http.Do(request)
	if err != nil {
		return nil, fmt.Errorf("webhook: delivery failed: %w", err)
	}

	return response, nil
}
