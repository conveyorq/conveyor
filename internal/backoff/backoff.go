// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

// Package backoff computes retry delays for failed task executions. The delay
// grows with the retry count according to the chosen Kind: exponential
// (base*2^retried) and linear (base*(retried+1)) grow a ceiling and draw the
// delay uniformly from [0, ceiling) (full jitter, which spreads retry storms),
// while fixed returns base exactly, for predictable spacing such as a
// rate-limited downstream. Every delay is bounded by the configured maximum.
package backoff

import (
	"math/rand/v2"
	"time"
)

// Server-side retry backoff defaults.
const (
	// DefaultBase is the backoff ceiling of the first retry.
	DefaultBase = 2 * time.Second
	// DefaultCap bounds the backoff ceiling regardless of retry count.
	DefaultCap = 15 * time.Minute
)

// Kind selects how a Strategy's delay ceiling grows with the retry count.
type Kind uint8

const (
	// Exponential doubles the ceiling each retry: base*2^retried.
	Exponential Kind = iota
	// Linear grows the ceiling linearly: base*(retried+1).
	Linear
	// Fixed returns base exactly on every retry (no jitter), for predictable
	// spacing such as a steady interval against a rate-limited downstream.
	Fixed
)

// ParseKind maps a configuration string ("exponential", "linear", "fixed") to a
// Kind. It returns false for an unrecognized value.
func ParseKind(name string) (Kind, bool) {
	switch name {
	case "exponential":
		return Exponential, true

	case "linear":
		return Linear, true

	case "fixed":
		return Fixed, true

	default:
		return 0, false
	}
}

// Strategy computes full-jitter retry delays. The zero value is unusable;
// build one with New or NewWithKind.
type Strategy struct {
	// kind is how the ceiling grows with the retry count.
	kind Kind
	// base is the delay ceiling of the first retry.
	base time.Duration
	// maxDelay bounds the delay ceiling for high retry counts.
	maxDelay time.Duration
}

// New builds a full-jitter exponential Strategy with the given first-retry
// ceiling and overall maximum delay.
func New(base, maxDelay time.Duration) Strategy {
	return Strategy{kind: Exponential, base: base, maxDelay: maxDelay}
}

// NewWithKind builds a Strategy with an explicit growth kind.
func NewWithKind(kind Kind, base, maxDelay time.Duration) Strategy {
	return Strategy{kind: kind, base: base, maxDelay: maxDelay}
}

// Kind returns the strategy's ceiling-growth kind.
func (s Strategy) Kind() Kind { return s.kind }

// Base returns the first-retry delay ceiling.
func (s Strategy) Base() time.Duration { return s.base }

// Cap returns the overall maximum delay ceiling.
func (s Strategy) Cap() time.Duration { return s.maxDelay }

// Delay returns the retry delay for the attempt. For the growing kinds it is a
// uniformly random value in [0, ceiling) (full jitter); for Fixed it is base
// exactly. The ceiling is bounded by maxDelay, and a negative retried counts as
// zero.
func (s Strategy) Delay(retried int32) time.Duration {
	if retried < 0 {
		retried = 0
	}

	ceiling := s.ceiling(retried)
	if ceiling <= 0 {
		return 0
	}

	// Fixed spaces retries at an exact interval for a predictable cadence; the
	// growing kinds jitter to spread retry storms.
	if s.kind == Fixed {
		return ceiling
	}

	// Jitter carries no security weight.
	return rand.N(ceiling) //nolint:gosec // math/rand is fine for jitter
}

// ceiling computes the pre-jitter delay ceiling for the attempt, bounded by
// maxDelay and guarding against overflow.
func (s Strategy) ceiling(retried int32) time.Duration {
	switch s.kind {
	case Linear:
		return s.scaled(int64(retried) + 1)

	case Fixed:
		return min(s.base, s.maxDelay)

	default: // Exponential
		// Shifting by 62+ bits or past the maximum overflows; the cap wins there.
		if retried < 62 {
			if shifted := s.base << retried; shifted > 0 && shifted < s.maxDelay {
				return shifted
			}
		}

		return s.maxDelay
	}
}

// scaled returns base*factor bounded by maxDelay. It tests the factor against
// the cap before multiplying, so a factor large enough to overflow (or a
// degenerate zero base) yields the maximum rather than a wrapped value.
func (s Strategy) scaled(factor int64) time.Duration {
	if s.base <= 0 || factor <= 0 {
		return s.maxDelay
	}

	if factor > int64(s.maxDelay/s.base) {
		return s.maxDelay
	}

	return s.base * time.Duration(factor)
}
