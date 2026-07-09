// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"slices"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/conveyorq/conveyor/internal/broker"
	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
)

// webhookSecretLimit is the most signing secrets one registration holds:
// its active secret plus, during a rotation, its predecessor.
const webhookSecretLimit = 2

// ListWebhookWorkers returns every registration with its secrets redacted:
// they are write-only credentials, and a dashboard listing must never leak
// them.
func (s *AdminService) ListWebhookWorkers(ctx context.Context, _ *connect.Request[conveyorv1.ListWebhookWorkersRequest]) (*connect.Response[conveyorv1.ListWebhookWorkersResponse], error) {
	workers, err := s.taskLog.ListWebhookWorkers(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	response := &conveyorv1.ListWebhookWorkersResponse{Workers: make([]*conveyorv1.WebhookWorker, 0, len(workers))}

	for _, worker := range workers {
		response.Workers = append(response.Workers, webhookWorkerInfo(worker))
	}

	return connect.NewResponse(response), nil
}

// UpsertWebhookWorker creates or replaces one registration and nudges the
// webhook manager so the change applies immediately.
func (s *AdminService) UpsertWebhookWorker(ctx context.Context, request *connect.Request[conveyorv1.UpsertWebhookWorkerRequest]) (*connect.Response[conveyorv1.UpsertWebhookWorkerResponse], error) {
	worker, err := s.webhookWorkerFromRequest(request.Msg.GetWorker())
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	if err := s.taskLog.UpsertWebhookWorker(ctx, worker); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	s.reconcileWebhookWorkers(ctx)

	return connect.NewResponse(&conveyorv1.UpsertWebhookWorkerResponse{}), nil
}

// PauseWebhookWorker suspends delivery to one registration.
func (s *AdminService) PauseWebhookWorker(ctx context.Context, request *connect.Request[conveyorv1.PauseWebhookWorkerRequest]) (*connect.Response[conveyorv1.PauseWebhookWorkerResponse], error) {
	if err := s.setWebhookWorkerPaused(ctx, request.Msg.GetName(), true); err != nil {
		return nil, err
	}

	return connect.NewResponse(&conveyorv1.PauseWebhookWorkerResponse{}), nil
}

// ResumeWebhookWorker resumes delivery to one paused registration.
func (s *AdminService) ResumeWebhookWorker(ctx context.Context, request *connect.Request[conveyorv1.ResumeWebhookWorkerRequest]) (*connect.Response[conveyorv1.ResumeWebhookWorkerResponse], error) {
	if err := s.setWebhookWorkerPaused(ctx, request.Msg.GetName(), false); err != nil {
		return nil, err
	}

	return connect.NewResponse(&conveyorv1.ResumeWebhookWorkerResponse{}), nil
}

// DeleteWebhookWorker removes one registration; deleting an absent name
// succeeds.
func (s *AdminService) DeleteWebhookWorker(ctx context.Context, request *connect.Request[conveyorv1.DeleteWebhookWorkerRequest]) (*connect.Response[conveyorv1.DeleteWebhookWorkerResponse], error) {
	if request.Msg.GetName() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("registration name is required"))
	}

	if err := s.taskLog.DeleteWebhookWorker(ctx, request.Msg.GetName()); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	s.reconcileWebhookWorkers(ctx)

	return connect.NewResponse(&conveyorv1.DeleteWebhookWorkerResponse{}), nil
}

// setWebhookWorkerPaused flips one registration's pause flag and applies it.
func (s *AdminService) setWebhookWorkerPaused(ctx context.Context, name string, paused bool) error {
	if name == "" {
		return connect.NewError(connect.CodeInvalidArgument, errors.New("registration name is required"))
	}

	err := s.taskLog.SetWebhookWorkerPaused(ctx, name, paused)
	if err != nil {
		if errors.Is(err, broker.ErrTaskNotFound) {
			return connect.NewError(connect.CodeNotFound, fmt.Errorf("webhook worker %q does not exist", name))
		}

		return connect.NewError(connect.CodeInternal, err)
	}

	s.reconcileWebhookWorkers(ctx)

	return nil
}

// reconcileWebhookWorkers nudges the webhook manager after a registration
// change. Best-effort: the manager's own tick converges regardless, so a
// failed nudge only delays the change.
func (s *AdminService) reconcileWebhookWorkers(ctx context.Context) {
	if err := s.engine.ReconcileWebhookWorkers(ctx); err != nil {
		// The tick is the backstop; nothing to surface to the caller.
		_ = err
	}
}

// webhookWorkerFromRequest validates one wire registration and converts it
// to the broker shape, applying the same rules as config-declared entries.
func (s *AdminService) webhookWorkerFromRequest(worker *conveyorv1.WebhookWorker) (*broker.WebhookWorker, error) {
	if worker == nil {
		return nil, errors.New("worker is required")
	}

	if !queueNamePattern.MatchString(worker.GetName()) {
		return nil, fmt.Errorf("invalid registration name %q", worker.GetName())
	}

	if err := s.validateWebhookURL(worker.GetUrl()); err != nil {
		return nil, err
	}

	if len(worker.GetQueues()) == 0 {
		return nil, errors.New("at least one queue is required")
	}

	for queue, weight := range worker.GetQueues() {
		if !queueNamePattern.MatchString(queue) {
			return nil, fmt.Errorf("invalid queue name %q", queue)
		}

		if weight <= 0 {
			return nil, fmt.Errorf("queue %q weight must be positive, got %d", queue, weight)
		}
	}

	if worker.GetConcurrency() < 1 {
		return nil, fmt.Errorf("concurrency must be at least 1, got %d", worker.GetConcurrency())
	}

	if count := len(worker.GetSecrets()); count < 1 || count > webhookSecretLimit {
		return nil, fmt.Errorf("one or two secrets are required, got %d", count)
	}

	if slices.Contains(worker.GetSecrets(), "") {
		return nil, errors.New("secrets must not be empty")
	}

	if worker.GetRequestTimeout().AsDuration() < 0 {
		return nil, fmt.Errorf("request timeout must not be negative, got %s", worker.GetRequestTimeout().AsDuration())
	}

	return &broker.WebhookWorker{
		Name:           worker.GetName(),
		URL:            worker.GetUrl(),
		Queues:         worker.GetQueues(),
		Concurrency:    worker.GetConcurrency(),
		Secrets:        worker.GetSecrets(),
		BatchTypes:     worker.GetBatchTypes(),
		RequestTimeout: worker.GetRequestTimeout().AsDuration(),
		Paused:         worker.GetPaused(),
	}, nil
}

// validateWebhookURL checks a delivery URL: an absolute http(s) URL, with
// plaintext http admitted only on an unauthenticated development server,
// because signed deliveries over cleartext hand the payload to the network.
func (s *AdminService) validateWebhookURL(raw string) error {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return fmt.Errorf("url must be a valid http(s) URL, got %q", raw)
	}

	if parsed.Scheme == "http" && !s.allowInsecureWebhooks {
		return fmt.Errorf("plaintext http requires an unauthenticated development server; use https, got %q", raw)
	}

	return nil
}

// webhookWorkerInfo converts one stored registration to its wire shape with
// the secrets redacted.
func webhookWorkerInfo(worker *broker.WebhookWorker) *conveyorv1.WebhookWorker {
	info := &conveyorv1.WebhookWorker{
		Name:        worker.Name,
		Url:         worker.URL,
		Queues:      worker.Queues,
		Concurrency: worker.Concurrency,
		BatchTypes:  worker.BatchTypes,
		Paused:      worker.Paused,
	}

	if worker.RequestTimeout > 0 {
		info.RequestTimeout = durationpb.New(worker.RequestTimeout)
	}

	return info
}
