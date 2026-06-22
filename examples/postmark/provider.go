// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package postmark

import (
	"context"
	"errors"
	"math/rand/v2"
	"sync/atomic"
	"time"
)

// Provider defaults. A connection limit well below a worker's concurrency makes
// the provider the bottleneck, so back-pressure is visible: surplus worker slots
// block in Send waiting for a connection, exactly as a real SMTP or API limit
// would gate a fleet.
const (
	// defaultMaxConnections is the largest number of concurrent sends the
	// simulated provider accepts.
	defaultMaxConnections = 8
	// defaultSendLatency is the simulated round-trip time of one send.
	defaultSendLatency = 150 * time.Millisecond
	// defaultFailureRate is the fraction of sends that fail transiently when the
	// provider is healthy, exercising retries with backoff.
	defaultFailureRate = 0.1
)

// Provider errors. A handler maps each to a Conveyor outcome: a hard bounce is
// archived (SkipRetry), while a transient failure or a full outage is retried.
var (
	// ErrTransient is a temporary failure (the provider's flaky "500"): the send
	// should be retried.
	ErrTransient = errors.New("postmark: provider returned a transient error")
	// ErrHardBounce is a permanent failure: the address is undeliverable and the
	// task must be archived rather than retried.
	ErrHardBounce = errors.New("postmark: recipient address hard-bounced")
	// ErrProviderDown is returned by every call while the provider is in an
	// outage. A task type that sees it repeatedly trips its circuit breaker.
	ErrProviderDown = errors.New("postmark: provider is unavailable")
)

// ProviderConfig configures a simulated provider. The zero value is usable: it
// applies the package defaults.
type ProviderConfig struct {
	// MaxConnections is the concurrent-send limit; zero selects the default.
	MaxConnections int
	// Latency is the simulated per-send round trip; zero selects the default.
	Latency time.Duration
	// FailureRate is the transient-failure probability in [0,1] while healthy;
	// the zero value never fails transiently. Use DefaultProviderConfig for the
	// realistic default that exercises retries.
	FailureRate float64
}

// DefaultProviderConfig returns the realistic provider settings the example's
// commands run with: a tight connection limit and a fraction of sends failing
// transiently, so retries with backoff are exercised. Tests build providers
// directly with a zero FailureRate when they need deterministic delivery.
func DefaultProviderConfig() ProviderConfig {
	return ProviderConfig{
		MaxConnections: defaultMaxConnections,
		Latency:        defaultSendLatency,
		FailureRate:    defaultFailureRate,
	}
}

// Provider is a fake email/SMTP provider. It accepts a bounded number of
// concurrent sends, fails a fraction of them transiently, permanently rejects
// hard-bounce addresses, and can be switched into a total outage. It holds no
// real connection and sends no real mail, so the example runs offline and its
// failure modes stay fully controllable.
type Provider struct {
	// connections is a counting semaphore bounding concurrent sends.
	connections chan struct{}
	// latency is the simulated per-send round trip.
	latency time.Duration
	// failureRate is the transient-failure probability while healthy.
	failureRate float64
	// down, when true, makes every send return ErrProviderDown.
	down atomic.Bool
	// sent, transient, and bounced count send outcomes for inspection.
	sent      atomic.Int64
	transient atomic.Int64
	bounced   atomic.Int64
}

// ProviderStats is a point-in-time snapshot of a provider's send outcomes.
type ProviderStats struct {
	// Sent is the number of successful deliveries.
	Sent int64
	// Transient is the number of transient failures (including outage rejections).
	Transient int64
	// Bounced is the number of permanent hard-bounce rejections.
	Bounced int64
}

// NewProvider builds a simulated provider from config, applying defaults for
// any unset field.
func NewProvider(config ProviderConfig) *Provider {
	maxConnections := config.MaxConnections
	if maxConnections <= 0 {
		maxConnections = defaultMaxConnections
	}

	latency := config.Latency
	if latency <= 0 {
		latency = defaultSendLatency
	}

	failureRate := config.FailureRate
	if failureRate < 0 {
		failureRate = 0
	}

	return &Provider{
		connections: make(chan struct{}, maxConnections),
		latency:     latency,
		failureRate: failureRate,
	}
}

// Send simulates delivering one email, honoring ctx cancellation throughout. It
// first claims one of the provider's limited connections — blocking if all are
// in use — then waits out the round-trip latency before deciding the outcome:
// an outage rejects every call, a hard-bounce address fails permanently, and an
// otherwise healthy send fails transiently at the configured rate.
func (p *Provider) Send(ctx context.Context, email Email) error {
	select {
	case p.connections <- struct{}{}:
		defer func() { <-p.connections }()

	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case <-time.After(p.latency):

	case <-ctx.Done():
		return ctx.Err()
	}

	if p.down.Load() {
		p.transient.Add(1)

		return ErrProviderDown
	}

	if IsHardBounce(email.To) {
		p.bounced.Add(1)

		return ErrHardBounce
	}

	if p.failureRate > 0 && rand.Float64() < p.failureRate { //nolint:gosec // non-cryptographic: simulated provider flakiness
		p.transient.Add(1)

		return ErrTransient
	}

	p.sent.Add(1)

	return nil
}

// SetDown switches the provider into (down=true) or out of (down=false) a total
// outage. While down, every Send returns ErrProviderDown, which trips the
// per-task-type circuit breaker; clearing it lets the breaker recover.
func (p *Provider) SetDown(down bool) {
	p.down.Store(down)
}

// Down reports whether the provider is currently in an outage.
func (p *Provider) Down() bool {
	return p.down.Load()
}

// InFlight reports how many sends currently hold a connection. It never exceeds
// the configured connection limit; surplus callers block in Send until a
// connection frees up.
func (p *Provider) InFlight() int {
	return len(p.connections)
}

// Stats snapshots the running send-outcome counters.
func (p *Provider) Stats() ProviderStats {
	return ProviderStats{
		Sent:      p.sent.Load(),
		Transient: p.transient.Load(),
		Bounced:   p.bounced.Load(),
	}
}
