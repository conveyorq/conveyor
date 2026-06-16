// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

// Package backoff computes retry delays for failed task executions using
// full-jitter exponential backoff: each delay is drawn uniformly from
// [0, min(cap, base*2^retried)), which spreads retry storms while keeping
// the expected delay growing exponentially.
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

// Strategy computes full-jitter retry delays. The zero value is unusable;
// build one with New.
type Strategy struct {
	// base is the delay ceiling of the first retry.
	base time.Duration
	// maxDelay bounds the delay ceiling for high retry counts.
	maxDelay time.Duration
}

// New builds a Strategy with the given first-retry ceiling and overall
// maximum delay.
func New(base, maxDelay time.Duration) Strategy {
	return Strategy{base: base, maxDelay: maxDelay}
}

// Delay returns a uniformly random delay in
// [0, min(maxDelay, base*2^retried)). A negative retried counts as zero.
func (s Strategy) Delay(retried int32) time.Duration {
	if retried < 0 {
		retried = 0
	}

	ceiling := s.maxDelay

	// Shifting by 62+ bits or past the maximum overflows; it wins there.
	if retried < 62 {
		if shifted := s.base << retried; shifted > 0 && shifted < s.maxDelay {
			ceiling = shifted
		}
	}

	if ceiling <= 0 {
		return 0
	}

	// Jitter spreads retry storms; it carries no security weight.
	return rand.N(ceiling) //nolint:gosec // math/rand is fine for jitter
}
