// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package postmark

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	conveyor "github.com/conveyorq/conveyor/sdks/go"
)

// Producer workload shape. The delays stand in for the product's real "send
// later" windows (a day-2 welcome tip, a trial-ending reminder three days out)
// compressed so a watcher sees the scheduled tasks fire within a demo.
const (
	// welcomeFollowupDelay schedules the second welcome-series mail; a day-2
	// tips email in production.
	welcomeFollowupDelay = 15 * time.Second
	// trialEndingDelay schedules the trial-ending reminder; three days before
	// the trial lapses in production.
	trialEndingDelay = 30 * time.Second
	// twoFactorTimeout bounds a 2FA send: a code that cannot go out fast is
	// worthless, so the attempt is abandoned rather than left hanging.
	twoFactorTimeout = 5 * time.Second
	// receiptRetention keeps a delivered receipt visible for the audit view.
	receiptRetention = 24 * time.Hour
	// passwordResetUnique holds the per-user dedup claim on a reset, so a burst
	// of "resend" clicks collapses to one mail.
	passwordResetUnique = 2 * time.Minute
	// campaignRecipients is how many recipients one campaign blast enqueues.
	campaignRecipients = 25
	// resendStormClicks is how many times an impatient user mashes "resend" in
	// the simulated reset storm; all but the first are deduplicated away.
	resendStormClicks = 4
	// maxUserID bounds the synthetic user-id space the producer draws from.
	maxUserID = 1000
)

// bounceFraction is the fraction of campaign recipients placed at a
// hard-bounce address, so a steady trickle of mail dead-letters into the
// archive for the "show me what failed and why" view.
const bounceFraction = 0.05

// campaignTenants are the customer accounts that send campaigns; each keys its
// own send concurrency so one tenant's blast cannot monopolize the provider.
var campaignTenants = []string{"acme", "globex", "initech"}

// Producer simulates the customer apps hitting the platform's API. Each method
// enqueues the Conveyor tasks one product action generates; Run drives a
// believable continuous mix of them.
type Producer struct {
	// client commits tasks to the server.
	client *conveyor.Client
	// logger records what was enqueued.
	logger *slog.Logger
}

// NewProducer builds a Producer over an enqueueing client.
func NewProducer(client *conveyor.Client, logger *slog.Logger) *Producer {
	return &Producer{client: client, logger: logger}
}

// Run drives a continuous, transactional-heavy workload, enqueuing one product
// action every interval until ctx is canceled. It returns ctx.Err on exit.
func (p *Producer) Run(ctx context.Context, interval time.Duration) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-ticker.C:
			if err := p.act(ctx); err != nil && ctx.Err() == nil {
				p.logger.Warn("enqueue failed", "error", err.Error())
			}
		}
	}
}

// act performs one randomly chosen product action, weighted toward the
// interactive transactional mail that dominates a real platform.
func (p *Producer) act(ctx context.Context) error {
	userID := rand.IntN(maxUserID) //nolint:gosec // non-cryptographic: synthetic user selection

	switch roll := rand.IntN(100); { //nolint:gosec // non-cryptographic: workload mix selection
	case roll < 35:
		return p.TwoFactor(ctx, userID)

	case roll < 55:
		return p.PasswordReset(ctx, userID)

	case roll < 70:
		return p.Receipt(ctx, userID)

	case roll < 85:
		return p.Welcome(ctx, userID)

	case roll < 95:
		return p.Campaign(ctx, campaignTenants[rand.IntN(len(campaignTenants))]) //nolint:gosec // non-cryptographic: tenant selection

	default:
		return p.ResendStorm(ctx, userID)
	}
}

// Welcome enqueues a signup's mail: a welcome email now, a follow-up tips email
// scheduled for later, and a trial-ending reminder scheduled further out. The
// scheduled tasks exercise delayed dispatch.
func (p *Producer) Welcome(ctx context.Context, userID int) error {
	now := conveyor.NewTask(TaskWelcome, conveyor.JSON(Email{
		UserID:  userID,
		To:      DeliverableAddress(userID),
		Subject: "Welcome to Postmark",
	}))

	if err := p.enqueue(ctx, now, conveyor.Queue(QueueDefault)); err != nil {
		return err
	}

	followup := conveyor.NewTask(TaskWelcome, conveyor.JSON(Email{
		UserID:  userID,
		To:      DeliverableAddress(userID),
		Subject: "Getting the most out of Postmark",
	}))

	if err := p.enqueue(ctx, followup, conveyor.Queue(QueueDefault), conveyor.ProcessIn(welcomeFollowupDelay)); err != nil {
		return err
	}

	trial := conveyor.NewTask(TaskTrialEnding, conveyor.JSON(Email{
		UserID:  userID,
		To:      DeliverableAddress(userID),
		Subject: "Your trial ends soon",
	}))

	return p.enqueue(ctx, trial, conveyor.Queue(QueueDefault), conveyor.ProcessIn(trialEndingDelay))
}

// PasswordReset enqueues a password-reset email on the transactional queue,
// deduplicated per user: a second reset for the same user while the first is
// still pending is rejected as a duplicate, not sent twice.
func (p *Producer) PasswordReset(ctx context.Context, userID int) error {
	task := conveyor.NewTask(TaskPasswordReset, conveyor.JSON(Email{
		UserID:  userID,
		To:      DeliverableAddress(userID),
		Subject: "Reset your password",
	}))

	return p.enqueue(ctx, task,
		conveyor.Queue(QueueTransactional),
		conveyor.Priority(PriorityHigh),
		conveyor.UniqueKey(fmt.Sprintf("user:%d:password-reset", userID)),
		conveyor.Unique(passwordResetUnique),
	)
}

// ResendStorm simulates an impatient user mashing "resend" on the reset form.
// Every click after the first collides with the per-user uniqueness key and is
// dropped, so the user gets one mail instead of resendStormClicks.
func (p *Producer) ResendStorm(ctx context.Context, userID int) error {
	for range resendStormClicks {
		if err := p.PasswordReset(ctx, userID); err != nil {
			return err
		}
	}

	return nil
}

// TwoFactor enqueues a 2FA code: the highest priority in the transactional
// queue, so it jumps ahead of welcome and receipt mail, and bounded by a tight
// timeout because a late code is useless.
func (p *Producer) TwoFactor(ctx context.Context, userID int) error {
	task := conveyor.NewTask(TaskTwoFactor, conveyor.JSON(Email{
		UserID:  userID,
		To:      DeliverableAddress(userID),
		Subject: "Your verification code",
	}))

	return p.enqueue(ctx, task,
		conveyor.Queue(QueueTransactional),
		conveyor.Priority(PriorityUrgent),
		conveyor.Timeout(twoFactorTimeout),
	)
}

// Receipt enqueues a purchase receipt on the transactional queue and keeps the
// completed task visible for the audit view via retention.
func (p *Producer) Receipt(ctx context.Context, userID int) error {
	task := conveyor.NewTask(TaskReceipt, conveyor.JSON(Email{
		UserID:  userID,
		To:      DeliverableAddress(userID),
		Subject: "Your receipt",
	}))

	return p.enqueue(ctx, task,
		conveyor.Queue(QueueTransactional),
		conveyor.Retention(receiptRetention),
	)
}

// Campaign enqueues one marketing blast for a tenant: campaignRecipients
// recipients on the lightly weighted marketing queue at bulk priority, all
// sharing the tenant's concurrency key so one campaign cannot stampede the
// provider. A small fraction of recipients are at a hard-bounce address and
// will dead-letter.
func (p *Producer) Campaign(ctx context.Context, tenant string) error {
	concurrencyKey := fmt.Sprintf("tenant:%s", tenant)

	for recipient := range campaignRecipients {
		userID := rand.IntN(maxUserID) //nolint:gosec // non-cryptographic: synthetic recipient selection

		to := DeliverableAddress(userID)
		if float64(recipient)/campaignRecipients < bounceFraction {
			to = BounceAddress(userID)
		}

		task := conveyor.NewTask(TaskCampaign, conveyor.JSON(Email{
			UserID:  userID,
			To:      to,
			Subject: "News from " + tenant,
			Tenant:  tenant,
		}))

		err := p.enqueue(ctx, task,
			conveyor.Queue(QueueMarketing),
			conveyor.Priority(PriorityBulk),
			conveyor.ConcurrencyKey(concurrencyKey),
		)
		if err != nil {
			return err
		}
	}

	return nil
}

// enqueue commits one task and logs the outcome. A duplicate rejection is the
// uniqueness feature working as intended, not an error, so it is logged and
// swallowed.
func (p *Producer) enqueue(ctx context.Context, task *conveyor.Task, opts ...conveyor.EnqueueOption) error {
	info, err := p.client.Enqueue(ctx, task, opts...)
	if err != nil {
		if errors.Is(err, conveyor.ErrDuplicateTask) {
			p.logger.Info("deduplicated", "type", task.Type())

			return nil
		}

		return fmt.Errorf("enqueue %s: %w", task.Type(), err)
	}

	p.logger.Info("enqueued", "type", info.Type, "queue", info.Queue, "id", info.ID, "state", string(info.State))

	return nil
}
