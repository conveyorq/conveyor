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
	"errors"

	"connectrpc.com/connect"

	"github.com/conveyorq/conveyor/internal/proto/conveyor/v1/conveyorv1connect"
)

// errReadOnly is returned when a mutating admin operation is attempted while
// the server runs in read-only mode.
var errReadOnly = errors.New("server is in read-only mode: admin mutations are disabled")

// mutatingProcedures is the set of AdminService procedures that change state.
// Read paths and the enqueue/worker APIs are intentionally excluded: read-only
// mode restricts operator actions, not task ingestion.
var mutatingProcedures = map[string]struct{}{
	conveyorv1connect.AdminServicePauseQueueProcedure:        {},
	conveyorv1connect.AdminServiceResumeQueueProcedure:       {},
	conveyorv1connect.AdminServiceCancelTaskProcedure:        {},
	conveyorv1connect.AdminServiceDeleteTaskProcedure:        {},
	conveyorv1connect.AdminServiceRunTaskProcedure:           {},
	conveyorv1connect.AdminServiceArchiveTaskProcedure:       {},
	conveyorv1connect.AdminServiceBatchDeleteTasksProcedure:  {},
	conveyorv1connect.AdminServiceBatchRunTasksProcedure:     {},
	conveyorv1connect.AdminServiceBatchCancelTasksProcedure:  {},
	conveyorv1connect.AdminServiceBatchArchiveTasksProcedure: {},
	conveyorv1connect.AdminServiceUpsertCronProcedure:        {},
	conveyorv1connect.AdminServicePauseCronProcedure:         {},
	conveyorv1connect.AdminServiceResumeCronProcedure:        {},
	conveyorv1connect.AdminServiceDeleteCronProcedure:        {},
}

// readOnlyInterceptor rejects mutating admin procedures, leaving reads and the
// enqueue path untouched.
type readOnlyInterceptor struct{}

// enforce interface compliance at compile time.
var _ connect.Interceptor = (*readOnlyInterceptor)(nil)

// NewReadOnlyInterceptor builds an interceptor that denies admin mutations.
func NewReadOnlyInterceptor() connect.Interceptor {
	return &readOnlyInterceptor{}
}

// WrapUnary implements connect.Interceptor.
func (i *readOnlyInterceptor) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, request connect.AnyRequest) (connect.AnyResponse, error) {
		if _, mutating := mutatingProcedures[request.Spec().Procedure]; mutating {
			return nil, connect.NewError(connect.CodePermissionDenied, errReadOnly)
		}

		return next(ctx, request)
	}
}

// WrapStreamingClient implements connect.Interceptor.
func (i *readOnlyInterceptor) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return next
}

// WrapStreamingHandler implements connect.Interceptor; the worker stream is not
// a mutating admin procedure, so it passes through.
func (i *readOnlyInterceptor) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return next
}
