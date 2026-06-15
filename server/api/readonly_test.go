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

package api

import (
	"context"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	"github.com/conveyorq/conveyor/internal/proto/conveyor/v1/conveyorv1connect"
)

// stubRequest is a minimal connect.AnyRequest carrying only a procedure name,
// which is all the read-only interceptor inspects.
type stubRequest struct {
	connect.AnyRequest
	procedure string
}

func (r stubRequest) Spec() connect.Spec {
	return connect.Spec{Procedure: r.procedure}
}

func TestReadOnlyInterceptorBlocksMutations(t *testing.T) {
	interceptor := NewReadOnlyInterceptor()

	called := false
	next := func(context.Context, connect.AnyRequest) (connect.AnyResponse, error) {
		called = true

		return nil, nil
	}

	wrapped := interceptor.WrapUnary(next)

	// A mutating procedure is rejected before reaching the handler.
	_, err := wrapped(context.Background(), stubRequest{procedure: conveyorv1connect.AdminServiceDeleteTaskProcedure})
	require.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))
	require.False(t, called)

	// A read procedure passes through to the handler.
	_, err = wrapped(context.Background(), stubRequest{procedure: conveyorv1connect.AdminServiceListTasksProcedure})
	require.NoError(t, err)
	require.True(t, called)
}
