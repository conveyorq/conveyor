// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

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
	conveyorv1connect.AdminServicePauseQueueProcedure:                  {},
	conveyorv1connect.AdminServiceResumeQueueProcedure:                 {},
	conveyorv1connect.AdminServiceSetQueueRateLimitProcedure:           {},
	conveyorv1connect.AdminServiceDeleteQueueRateLimitProcedure:        {},
	conveyorv1connect.AdminServiceSetQueueConcurrencyLimitProcedure:    {},
	conveyorv1connect.AdminServiceDeleteQueueConcurrencyLimitProcedure: {},
	conveyorv1connect.AdminServiceSetGroupConfigProcedure:              {},
	conveyorv1connect.AdminServiceDeleteGroupConfigProcedure:           {},
	conveyorv1connect.AdminServiceCancelTaskProcedure:                  {},
	conveyorv1connect.AdminServiceDeleteTaskProcedure:                  {},
	conveyorv1connect.AdminServiceRunTaskProcedure:                     {},
	conveyorv1connect.AdminServiceRescheduleTaskProcedure:              {},
	conveyorv1connect.AdminServiceArchiveTaskProcedure:                 {},
	conveyorv1connect.AdminServiceBatchDeleteTasksProcedure:            {},
	conveyorv1connect.AdminServiceBatchRunTasksProcedure:               {},
	conveyorv1connect.AdminServiceBatchCancelTasksProcedure:            {},
	conveyorv1connect.AdminServiceBatchArchiveTasksProcedure:           {},
	conveyorv1connect.AdminServiceUpsertCronProcedure:                  {},
	conveyorv1connect.AdminServicePauseCronProcedure:                   {},
	conveyorv1connect.AdminServiceResumeCronProcedure:                  {},
	conveyorv1connect.AdminServiceDeleteCronProcedure:                  {},
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
