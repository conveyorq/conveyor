// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

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

	// The rate-limit mutations are blocked too.
	_, err = wrapped(context.Background(), stubRequest{procedure: conveyorv1connect.AdminServiceSetQueueRateLimitProcedure})
	require.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))

	_, err = wrapped(context.Background(), stubRequest{procedure: conveyorv1connect.AdminServiceDeleteQueueRateLimitProcedure})
	require.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))

	// The group-config mutations are blocked too.
	_, err = wrapped(context.Background(), stubRequest{procedure: conveyorv1connect.AdminServiceSetGroupConfigProcedure})
	require.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))

	_, err = wrapped(context.Background(), stubRequest{procedure: conveyorv1connect.AdminServiceDeleteGroupConfigProcedure})
	require.Equal(t, connect.CodePermissionDenied, connect.CodeOf(err))

	// A read procedure passes through to the handler.
	_, err = wrapped(context.Background(), stubRequest{procedure: conveyorv1connect.AdminServiceListTasksProcedure})
	require.NoError(t, err)
	require.True(t, called)

	// Listing rate limits is a read and passes through.
	called = false
	_, err = wrapped(context.Background(), stubRequest{procedure: conveyorv1connect.AdminServiceListRateLimitsProcedure})
	require.NoError(t, err)
	require.True(t, called)
}
