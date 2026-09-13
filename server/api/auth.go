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

	"github.com/conveyorq/conveyor/internal/proto/conveyor/v1/conveyorv1connect"
)

// authorizationHeader is the HTTP header carrying the bearer token.
const authorizationHeader = "Authorization"

// bearerPrefix is the expected authorization scheme prefix.
const bearerPrefix = "Bearer "

// Scope identifies a class of operations a bearer token is authorized for. The
// three scopes map one-to-one onto the three authenticated services, so a
// token's scopes decide which services it may call.
type Scope string

const (
	// ScopeProduce authorizes the task-enqueue API (TaskService).
	ScopeProduce Scope = "produce"
	// ScopeConsume authorizes the worker session protocol (WorkerService).
	ScopeConsume Scope = "consume"
	// ScopeAdmin authorizes the administrative API (AdminService).
	ScopeAdmin Scope = "admin"
)

// Service-procedure prefixes used to resolve the scope one procedure requires.
// Every procedure is named "/<service>/<method>", so the service prefix alone
// selects the scope.
const (
	taskServicePrefix   = "/" + conveyorv1connect.TaskServiceName + "/"
	workerServicePrefix = "/" + conveyorv1connect.WorkerServiceName + "/"
	adminServicePrefix  = "/" + conveyorv1connect.AdminServiceName + "/"
)

// errUnauthenticated is returned for missing or unknown tokens. One message
// for both cases: callers learn nothing about which tokens exist.
var errUnauthenticated = errors.New("missing or invalid bearer token")

// errForbidden is returned when a recognized token lacks the scope its target
// procedure requires.
var errForbidden = errors.New("token is not authorized for this operation")

// AllScopes returns every scope, the grant given to a token configured without
// an explicit scope set. It backs the full-access token that keeps a flat
// api.auth_tokens list working unchanged.
func AllScopes() []Scope {
	return []Scope{ScopeProduce, ScopeConsume, ScopeAdmin}
}

// ParseScope resolves a scope name to its Scope, reporting whether the name is
// one of the known scopes.
func ParseScope(name string) (Scope, bool) {
	switch Scope(name) {
	case ScopeProduce, ScopeConsume, ScopeAdmin:
		return Scope(name), true
	default:
		return "", false
	}
}

// ScopedToken is one accepted bearer token paired with the scopes it may
// exercise.
type ScopedToken struct {
	// Token is the bearer token value presented in the Authorization header.
	Token string
	// Scopes are the operations the token is authorized for.
	Scopes []Scope
}

// scopedToken is the interceptor's internal form of one accepted token: the
// token bytes for constant-time comparison and its scope set for lookup.
type scopedToken struct {
	token  []byte
	scopes map[Scope]struct{}
}

// authInterceptor enforces static bearer-token authentication and per-scope
// authorization on every unary call and every incoming stream.
type authInterceptor struct {
	// tokens are the accepted bearer tokens with their granted scopes.
	tokens []scopedToken
}

// enforce interface compliance at compile time.
var _ connect.Interceptor = (*authInterceptor)(nil)

// NewAuthInterceptor builds the bearer-token interceptor from the accepted
// scoped tokens. Token comparison is constant-time per candidate; a
// recognized token is then authorized against the scope its procedure
// requires.
func NewAuthInterceptor(tokens []ScopedToken) connect.Interceptor {
	accepted := make([]scopedToken, 0, len(tokens))

	for _, entry := range tokens {
		scopes := make(map[Scope]struct{}, len(entry.Scopes))

		for _, scope := range entry.Scopes {
			scopes[scope] = struct{}{}
		}

		accepted = append(accepted, scopedToken{token: []byte(entry.Token), scopes: scopes})
	}

	return &authInterceptor{tokens: accepted}
}

// WrapUnary implements connect.Interceptor.
func (i *authInterceptor) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, request connect.AnyRequest) (connect.AnyResponse, error) {
		if err := i.authorize(request.Header(), request.Spec().Procedure); err != nil {
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
		if err := i.authorize(conn.RequestHeader(), conn.Spec().Procedure); err != nil {
			return err
		}

		return next(ctx, conn)
	}
}

// authorize checks the Authorization header against the accepted tokens and,
// on a match, that the token holds the scope the procedure requires.
func (i *authInterceptor) authorize(header http.Header, procedure string) error {
	value := header.Get(authorizationHeader)

	presented, ok := strings.CutPrefix(value, bearerPrefix)
	if !ok || presented == "" {
		return connect.NewError(connect.CodeUnauthenticated, errUnauthenticated)
	}

	candidate := []byte(presented)

	for _, entry := range i.tokens {
		if subtle.ConstantTimeCompare(candidate, entry.token) == 1 {
			return authorizeScope(entry.scopes, procedure)
		}
	}

	return connect.NewError(connect.CodeUnauthenticated, errUnauthenticated)
}

// authorizeScope reports whether a recognized token's scopes admit the given
// procedure. An unmapped procedure fails closed: the interceptor guards only
// the three scoped services, so any other procedure is denied.
func authorizeScope(scopes map[Scope]struct{}, procedure string) error {
	required, ok := requiredScope(procedure)
	if !ok {
		return connect.NewError(connect.CodePermissionDenied, errForbidden)
	}

	if _, granted := scopes[required]; !granted {
		return connect.NewError(connect.CodePermissionDenied, errForbidden)
	}

	return nil
}

// requiredScope resolves the scope a procedure requires from its service
// prefix, reporting false for a procedure outside the three scoped services.
func requiredScope(procedure string) (Scope, bool) {
	switch {
	case strings.HasPrefix(procedure, taskServicePrefix):
		return ScopeProduce, true

	case strings.HasPrefix(procedure, workerServicePrefix):
		return ScopeConsume, true

	case strings.HasPrefix(procedure, adminServicePrefix):
		return ScopeAdmin, true

	default:
		return "", false
	}
}
