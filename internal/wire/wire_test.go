// MIT License
//
// Copyright (c) 2026 ConveyorQ
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package wire

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
)

func TestNewH2CClientSpeaksUnencryptedHTTP2(t *testing.T) {
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Proto", r.Proto)
	}))

	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	protocols.SetHTTP2(true)
	protocols.SetUnencryptedHTTP2(true)

	server.Config.Protocols = protocols
	server.Start()
	t.Cleanup(server.Close)

	response, err := NewH2CClient().Get(server.URL)
	require.NoError(t, err)

	defer func() { _ = response.Body.Close() }()

	require.Equal(t, "HTTP/2.0", response.Header.Get("X-Proto"))
}

func TestBearerInterceptorSetsHeaderOnUnaryCalls(t *testing.T) {
	interceptor := NewBearerInterceptor("secret")

	var seen string

	next := connect.UnaryFunc(func(_ context.Context, request connect.AnyRequest) (connect.AnyResponse, error) {
		seen = request.Header().Get("Authorization")

		return nil, nil
	})

	request := connect.NewRequest(&struct{}{})

	_, err := interceptor.WrapUnary(next)(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, "Bearer secret", seen)
}

func TestBearerInterceptorSetsHeaderOnStreams(t *testing.T) {
	interceptor := NewBearerInterceptor("secret")

	conn := &recordingStreamConn{header: make(http.Header)}

	next := connect.StreamingClientFunc(func(context.Context, connect.Spec) connect.StreamingClientConn {
		return conn
	})

	wrapped := interceptor.WrapStreamingClient(next)(context.Background(), connect.Spec{})
	require.Equal(t, "Bearer secret", wrapped.RequestHeader().Get("Authorization"))
}

// recordingStreamConn is a minimal StreamingClientConn capturing headers.
type recordingStreamConn struct {
	connect.StreamingClientConn

	// header is the captured request header.
	header http.Header
}

// RequestHeader implements connect.StreamingClientConn.
func (c *recordingStreamConn) RequestHeader() http.Header {
	return c.header
}
