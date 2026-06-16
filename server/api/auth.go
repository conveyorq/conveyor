// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

// Package api implements the ConnectRPC services of conveyord: the task
// enqueue API, the worker session protocol, and the bearer-token
// authentication shared by every service.
package api

import (
	"context"
	"crypto/subtle"
	"errors"
	"net/http"
	"strings"

	"connectrpc.com/connect"
)

// authorizationHeader is the HTTP header carrying the bearer token.
const authorizationHeader = "Authorization"

// bearerPrefix is the expected authorization scheme prefix.
const bearerPrefix = "Bearer "

// errUnauthenticated is returned for missing or unknown tokens. One message
// for both cases: callers learn nothing about which tokens exist.
var errUnauthenticated = errors.New("missing or invalid bearer token")

// authInterceptor enforces static bearer-token authentication on every
// unary call and every incoming stream.
type authInterceptor struct {
	// tokens are the accepted bearer tokens.
	tokens [][]byte
}

// enforce interface compliance at compile time.
var _ connect.Interceptor = (*authInterceptor)(nil)

// NewAuthInterceptor builds the bearer-token interceptor from the accepted
// tokens. Token comparison is constant-time per candidate.
func NewAuthInterceptor(tokens []string) connect.Interceptor {
	accepted := make([][]byte, 0, len(tokens))

	for _, token := range tokens {
		accepted = append(accepted, []byte(token))
	}

	return &authInterceptor{tokens: accepted}
}

// WrapUnary implements connect.Interceptor.
func (i *authInterceptor) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, request connect.AnyRequest) (connect.AnyResponse, error) {
		if err := i.authorize(request.Header()); err != nil {
			return nil, err
		}

		return next(ctx, request)
	}
}

// WrapStreamingClient implements connect.Interceptor; outgoing client
// streams are not authenticated here.
func (i *authInterceptor) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return next
}

// WrapStreamingHandler implements connect.Interceptor.
func (i *authInterceptor) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return func(ctx context.Context, conn connect.StreamingHandlerConn) error {
		if err := i.authorize(conn.RequestHeader()); err != nil {
			return err
		}

		return next(ctx, conn)
	}
}

// authorize checks the Authorization header against the accepted tokens.
func (i *authInterceptor) authorize(header http.Header) error {
	value := header.Get(authorizationHeader)

	presented, ok := strings.CutPrefix(value, bearerPrefix)
	if !ok || presented == "" {
		return connect.NewError(connect.CodeUnauthenticated, errUnauthenticated)
	}

	candidate := []byte(presented)

	for _, token := range i.tokens {
		if subtle.ConstantTimeCompare(candidate, token) == 1 {
			return nil
		}
	}

	return connect.NewError(connect.CodeUnauthenticated, errUnauthenticated)
}
