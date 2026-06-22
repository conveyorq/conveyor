// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package postmark

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	conveyor "github.com/conveyorq/conveyor/sdks/go"
)

// digestUserCount is how many per-user summaries one weekly-digest run builds
// and sends. It keeps the cron-fired task long enough to observe but bounded.
const digestUserCount = 50

// NewMux builds the task router for a Postmark worker: every send task type is
// served by the same send handler (so each gets its own circuit breaker keyed
// by type), the weekly digest gets its own, and a logging middleware records
// the outcome of every task. The provider is the simulated email backend the
// handlers deliver through.
func NewMux(provider *Provider, logger *slog.Logger) *conveyor.Mux {
	mux := conveyor.NewMux()
	mux.Use(logging(logger))

	send := sendEmail(provider)

	for _, taskType := range sendTaskTypes() {
		mux.HandleFunc(taskType, send)
	}

	mux.HandleFunc(TaskDigest, sendDigest(provider, logger))

	return mux
}

// sendTaskTypes lists the task types that deliver a single email through the
// provider and therefore share the send handler.
func sendTaskTypes() []string {
	return []string{
		TaskWelcome,
		TaskPasswordReset,
		TaskTwoFactor,
		TaskReceipt,
		TaskTrialEnding,
		TaskCampaign,
	}
}

// sendEmail returns the handler for a single-recipient send. It binds the
// payload, delivers it through the provider, and maps the result to a Conveyor
// outcome: a hard bounce is archived immediately (SkipRetry), while a transient
// failure or a full outage returns a plain error so the server retries with
// backoff. A payload that cannot decode is archived: it never will.
func sendEmail(provider *Provider) conveyor.HandlerFunc {
	return func(ctx context.Context, task *conveyor.Task) error {
		var email Email

		if err := task.Bind(&email); err != nil {
			return conveyor.SkipRetry(err)
		}

		if err := provider.Send(ctx, email); err != nil {
			if errors.Is(err, ErrHardBounce) {
				return conveyor.SkipRetry(fmt.Errorf("delivering %s to %s: %w", task.Type(), email.To, err))
			}

			return fmt.Errorf("delivering %s to %s: %w", task.Type(), email.To, err)
		}

		return nil
	}
}

// sendDigest returns the handler for the weekly digest. It builds and delivers
// one summary per user; an individual hard bounce is logged and skipped rather
// than failing the whole run, but a provider outage fails the task so it is
// retried once the provider recovers. The cron-materialized task carries no
// payload, so none is bound.
func sendDigest(provider *Provider, logger *slog.Logger) conveyor.HandlerFunc {
	return func(ctx context.Context, _ *conveyor.Task) error {
		for userID := range digestUserCount {
			email := Email{
				UserID:  userID,
				To:      DeliverableAddress(userID),
				Subject: "Your weekly activity summary",
			}

			err := provider.Send(ctx, email)

			switch {
			case err == nil, errors.Is(err, ErrHardBounce):
				// A bounce on one recipient must not abandon the whole digest.

			default:
				return fmt.Errorf("sending weekly digest: %w", err)
			}
		}

		logger.Info("weekly digest sent", "recipients", digestUserCount)

		return nil
	}
}

// logging records the outcome of every task: delivered, archived (a permanent
// failure the server will not retry), or failed (a transient failure the server
// will retry with backoff).
func logging(logger *slog.Logger) conveyor.MiddlewareFunc {
	return func(next conveyor.HandlerFunc) conveyor.HandlerFunc {
		return func(ctx context.Context, task *conveyor.Task) error {
			err := next(ctx, task)

			switch {
			case err == nil:
				logger.Info("delivered", "type", task.Type(), "id", task.ID(), "attempt", task.Retried())

			case conveyor.IsSkipRetry(err):
				logger.Warn("archived", "type", task.Type(), "id", task.ID(), "error", err.Error())

			case errors.Is(err, context.Canceled):
				// A drain (worker shutdown) or an admin cancel: not a delivery
				// failure, and not retried against the worker's will.
				logger.Info("canceled", "type", task.Type(), "id", task.ID())

			default:
				logger.Warn("failed, will retry", "type", task.Type(), "id", task.ID(), "attempt", task.Retried(), "error", err.Error())
			}

			return err
		}
	}
}
