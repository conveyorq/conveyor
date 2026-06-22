// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

// Package postmark is a production-like example application built on Conveyor:
// a miniature transactional email and notification platform. An API (the
// Producer) accepts requests to notify users, and every piece of downstream
// work is a Conveyor task processed by the handlers in this package.
//
// The point of the example is that each Conveyor feature falls out of the
// product naturally rather than being bolted on. Password resets and 2FA codes
// ride a heavily weighted transactional queue; campaign blasts ride a
// lightly weighted marketing queue; a fake email provider with a fixed
// connection limit and an occasional outage exercises concurrency limits,
// retries with backoff, dead-lettering, and the per-task-type circuit breaker.
//
// The worker and producer run as separate processes (cmd/worker and
// cmd/producer) against a Postgres-backed conveyord cluster; the deploy
// directory runs them on Kubernetes, which is how the example showcases
// Conveyor's durability and high-availability story. The send is always
// simulated, never real SMTP, so the example needs no secrets and the flaky and
// outage behaviors stay controllable for the retry, circuit-breaker, and
// dead-letter demos.
package postmark

import (
	"fmt"
	"strings"
)

// The platform's queue tiers. A worker declares a relative weight per queue and
// the server hands out each queue's work in proportion, so a password reset
// never waits behind a million-recipient newsletter.
const (
	// QueueTransactional carries must-send-now mail: password resets, 2FA
	// codes, and receipts. It is weighted the heaviest.
	QueueTransactional = "transactional"
	// QueueDefault carries ordinary notifications: welcome mail and trial
	// reminders. It is weighted in between.
	QueueDefault = "default"
	// QueueMarketing carries campaign blasts. It is weighted the lightest so a
	// large send drains slowly without starving the other tiers.
	QueueMarketing = "marketing"
)

// Task types routed to the handlers registered by NewMux.
const (
	// TaskWelcome is a welcome email sent immediately after signup.
	TaskWelcome = "email:welcome"
	// TaskPasswordReset is a password-reset email, deduplicated per user so a
	// burst of "resend" clicks does not send ten mails.
	TaskPasswordReset = "email:password-reset"
	// TaskTwoFactor is a 2FA code: the most urgent mail, jumping ahead of
	// everything else in the transactional queue and bounded by a tight timeout.
	TaskTwoFactor = "email:2fa"
	// TaskReceipt is a purchase receipt, kept visible after completion for the
	// audit view.
	TaskReceipt = "email:receipt"
	// TaskTrialEnding is a "your trial ends soon" reminder, scheduled for a
	// future time rather than sent now.
	TaskTrialEnding = "email:trial-ending"
	// TaskCampaign is one recipient of a marketing campaign blast.
	TaskCampaign = "email:campaign"
	// TaskDigest builds and sends every user's weekly activity summary; it is
	// materialized by a cron entry, not enqueued by the producer.
	TaskDigest = "digest:weekly"
)

// Dispatch priorities within a queue (1 lowest, 9 highest; the unset default is
// 4). A 2FA code outranks a welcome email sharing the transactional queue.
const (
	// PriorityUrgent puts 2FA codes ahead of everything in their queue.
	PriorityUrgent = 9
	// PriorityHigh puts password resets ahead of ordinary transactional mail.
	PriorityHigh = 7
	// PriorityBulk sinks campaign mail below interactive notifications.
	PriorityBulk = 2
)

// bounceDomain is the recipient domain whose addresses always hard-bounce, so
// the producer can deterministically generate permanently undeliverable mail
// that the handler dead-letters. ".invalid" is reserved by RFC 2606 and never
// resolves, so the address is unmistakably synthetic.
const bounceDomain = "bounce.invalid"

// deliverableDomain is the recipient domain for ordinary, deliverable mail.
const deliverableDomain = "example.com"

// Email is the payload of every send task: who to mail, on whose behalf, and
// what about. A single shape serves every task type because, to the platform,
// they are all "send this user an email".
type Email struct {
	// UserID identifies the recipient account; it keys per-user uniqueness.
	UserID int `json:"user_id"`
	// To is the destination address. An address at the hard-bounce domain is
	// permanently undeliverable; see IsHardBounce.
	To string `json:"to"`
	// Subject is the human-readable subject line, used only for log output.
	Subject string `json:"subject"`
	// Tenant identifies the sending customer; it keys per-tenant send
	// concurrency on campaign blasts. Empty for system mail.
	Tenant string `json:"tenant,omitempty"`
}

// WorkerQueues returns the queue-to-weight map a Postmark worker serves:
// transactional far above default, marketing far below, so dispatch favors the
// mail that must go now. Pass it to conveyor.WithQueues.
func WorkerQueues() map[string]int {
	return map[string]int{
		QueueTransactional: 10,
		QueueDefault:       5,
		QueueMarketing:     1,
	}
}

// DeliverableAddress returns the ordinary, deliverable address for a user.
func DeliverableAddress(userID int) string {
	return fmt.Sprintf("user%d@%s", userID, deliverableDomain)
}

// BounceAddress returns a permanently undeliverable address for a user, used to
// demonstrate hard bounces landing in the archive.
func BounceAddress(userID int) string {
	return fmt.Sprintf("user%d@%s", userID, bounceDomain)
}

// IsHardBounce reports whether addr is permanently undeliverable. A handler that
// gets a hard bounce wraps the failure in conveyor.SkipRetry so the task is
// archived immediately instead of retried against an address that never works.
func IsHardBounce(addr string) bool {
	return strings.HasSuffix(addr, "@"+bounceDomain)
}
