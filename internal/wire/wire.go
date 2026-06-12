// Package wire holds the ConnectRPC plumbing shared by every Conveyor
// wire client — the SDK transport and the CLI's admin client — so the
// HTTP protocol setup and bearer-token injection exist exactly once.
package wire

import (
	"context"
	"net/http"

	"connectrpc.com/connect"
)

// authorizationHeader is the HTTP header carrying the bearer token.
const authorizationHeader = "Authorization"

// bearerPrefix is the authorization scheme prefix.
const bearerPrefix = "Bearer "

// NewH2CClient returns an HTTP client for Conveyor servers. Plain http
// URLs use unencrypted HTTP/2, which the worker session stream requires;
// https negotiates HTTP/2 via ALPN.
func NewH2CClient() *http.Client {
	protocols := new(http.Protocols)
	protocols.SetHTTP2(true)
	protocols.SetUnencryptedHTTP2(true)

	return &http.Client{Transport: &http.Transport{Protocols: protocols}}
}

// BearerInterceptor injects a bearer token into every call and stream.
type BearerInterceptor struct {
	// token is the bearer token presented to the server.
	token string
}

// enforce interface compliance at compile time.
var _ connect.Interceptor = (*BearerInterceptor)(nil)

// NewBearerInterceptor builds an interceptor presenting the given token.
func NewBearerInterceptor(token string) *BearerInterceptor {
	return &BearerInterceptor{token: token}
}

// WrapUnary implements connect.Interceptor.
func (i *BearerInterceptor) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, request connect.AnyRequest) (connect.AnyResponse, error) {
		request.Header().Set(authorizationHeader, bearerPrefix+i.token)

		return next(ctx, request)
	}
}

// WrapStreamingClient implements connect.Interceptor.
func (i *BearerInterceptor) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return func(ctx context.Context, spec connect.Spec) connect.StreamingClientConn {
		conn := next(ctx, spec)
		conn.RequestHeader().Set(authorizationHeader, bearerPrefix+i.token)

		return conn
	}
}

// WrapStreamingHandler implements connect.Interceptor; wire clients never
// serve streams.
func (i *BearerInterceptor) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return next
}
