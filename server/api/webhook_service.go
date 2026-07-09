// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"context"
	"errors"
	"fmt"

	"connectrpc.com/connect"

	"github.com/conveyorq/conveyor/internal/actors"
	"github.com/conveyorq/conveyor/internal/broker"
	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
	"github.com/conveyorq/conveyor/internal/proto/conveyor/v1/conveyorv1connect"
	"github.com/conveyorq/conveyor/internal/webhook"
)

// WebhookService serves the asynchronous-completion callbacks of webhook
// workers. It is authenticated by per-delivery lease tokens, never by API
// bearer tokens, so it mounts without the bearer interceptor: an endpoint
// holding a token may heartbeat and resolve exactly one delivery.
type WebhookService struct {
	// engine routes verified callbacks to the owning webhook gateway.
	engine *actors.Engine
	// taskLog loads registrations for token verification.
	taskLog broker.Broker
}

// enforce interface compliance at compile time.
var _ conveyorv1connect.WebhookServiceHandler = (*WebhookService)(nil)

// NewWebhookService assembles the webhook callback service.
func NewWebhookService(engine *actors.Engine, taskLog broker.Broker) *WebhookService {
	return &WebhookService{engine: engine, taskLog: taskLog}
}

// Heartbeat extends one asynchronously completing delivery's lease.
func (s *WebhookService) Heartbeat(ctx context.Context, request *connect.Request[conveyorv1.WebhookHeartbeatRequest]) (*connect.Response[conveyorv1.WebhookHeartbeatResponse], error) {
	claims, err := s.verify(ctx, request.Msg.GetLeaseToken())
	if err != nil {
		return nil, err
	}

	message := &conveyorv1.WebhookLeaseHeartbeat{TaskId: claims.TaskID, LeaseId: claims.LeaseID}
	if err := s.engine.TellWebhookGateway(ctx, claims.Registration, message); err != nil {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("delivery is no longer tracked"))
	}

	return connect.NewResponse(&conveyorv1.WebhookHeartbeatResponse{}), nil
}

// ReportResult resolves one asynchronously completing delivery.
func (s *WebhookService) ReportResult(ctx context.Context, request *connect.Request[conveyorv1.WebhookReportResultRequest]) (*connect.Response[conveyorv1.WebhookReportResultResponse], error) {
	switch request.Msg.GetOutcome() {
	case conveyorv1.TaskOutcome_TASK_OUTCOME_SUCCESS,
		conveyorv1.TaskOutcome_TASK_OUTCOME_RETRY,
		conveyorv1.TaskOutcome_TASK_OUTCOME_SKIP_RETRY:

	default:
		return nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("outcome must be success, retry, or skip-retry, got %s", request.Msg.GetOutcome()))
	}

	claims, err := s.verify(ctx, request.Msg.GetLeaseToken())
	if err != nil {
		return nil, err
	}

	message := &conveyorv1.WebhookLeaseResult{
		TaskId:   claims.TaskID,
		LeaseId:  claims.LeaseID,
		Outcome:  request.Msg.GetOutcome(),
		ErrorMsg: request.Msg.GetErrorMsg(),
		Result:   request.Msg.GetResult(),
	}

	if err := s.engine.TellWebhookGateway(ctx, claims.Registration, message); err != nil {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("delivery is no longer tracked"))
	}

	return connect.NewResponse(&conveyorv1.WebhookReportResultResponse{}), nil
}

// verify parses and verifies one lease token against its registration's
// secrets. Every failure is the same unauthenticated error: callers learn
// nothing about which registrations or deliveries exist.
func (s *WebhookService) verify(ctx context.Context, token string) (*webhook.LeaseClaims, error) {
	unauthenticated := connect.NewError(connect.CodeUnauthenticated, errors.New("invalid lease token"))

	claims, err := webhook.ParseLeaseToken(token)
	if err != nil {
		return nil, unauthenticated
	}

	worker, err := s.taskLog.GetWebhookWorker(ctx, claims.Registration)
	if err != nil {
		if errors.Is(err, broker.ErrTaskNotFound) {
			return nil, unauthenticated
		}

		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("loading registration: %w", err))
	}

	if err := webhook.VerifyLeaseToken(token, claims, worker.Secrets); err != nil {
		return nil, unauthenticated
	}

	return claims, nil
}
