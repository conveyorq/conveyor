// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package backoff

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDelayStaysUnderGrowingCeiling(t *testing.T) {
	strategy := New(2*time.Second, 15*time.Minute)

	for retried := range int32(8) {
		ceiling := 2 * time.Second << retried

		for range 200 {
			delay := strategy.Delay(retried)
			require.GreaterOrEqual(t, delay, time.Duration(0))
			require.Less(t, delay, ceiling, "retried=%d", retried)
		}
	}
}

func TestDelayIsCapped(t *testing.T) {
	strategy := New(2*time.Second, 15*time.Minute)

	for _, retried := range []int32{20, 62, 63, 1000} {
		for range 200 {
			delay := strategy.Delay(retried)
			require.GreaterOrEqual(t, delay, time.Duration(0))
			require.Less(t, delay, 15*time.Minute, "retried=%d", retried)
		}
	}
}

func TestDelayNegativeRetriedCountsAsZero(t *testing.T) {
	strategy := New(2*time.Second, 15*time.Minute)

	for range 200 {
		delay := strategy.Delay(-5)
		require.GreaterOrEqual(t, delay, time.Duration(0))
		require.Less(t, delay, 2*time.Second)
	}
}

func TestDelayWithDegenerateStrategyIsZero(t *testing.T) {
	require.Zero(t, New(0, 0).Delay(0))
	require.Zero(t, New(-time.Second, -time.Minute).Delay(3))
}

func TestLinearCeilingGrowsLinearly(t *testing.T) {
	strategy := NewWithKind(Linear, 3*time.Second, time.Hour)

	for retried := range int32(8) {
		ceiling := 3 * time.Second * time.Duration(retried+1)

		for range 200 {
			delay := strategy.Delay(retried)
			require.GreaterOrEqual(t, delay, time.Duration(0))
			require.Less(t, delay, ceiling, "retried=%d", retried)
		}
	}
}

func TestLinearCapsLargeRetriedWithoutOverflow(t *testing.T) {
	strategy := NewWithKind(Linear, time.Minute, 10*time.Minute)

	// A huge retry count must neither overflow nor wrap; the delay stays capped.
	for _, retried := range []int32{100, 1_000_000, 2147483647} {
		for range 50 {
			delay := strategy.Delay(retried)
			require.GreaterOrEqual(t, delay, time.Duration(0))
			require.Less(t, delay, 10*time.Minute, "retried=%d", retried)
		}
	}
}

func TestLinearDegenerateBaseIsZero(t *testing.T) {
	require.Zero(t, NewWithKind(Linear, 0, 0).Delay(3))
}

func TestFixedDelayIsExactAndConstant(t *testing.T) {
	strategy := NewWithKind(Fixed, 5*time.Second, time.Hour)

	// Fixed returns base exactly on every attempt, no jitter and no growth.
	for _, retried := range []int32{0, 1, 5, 100} {
		require.Equal(t, 5*time.Second, strategy.Delay(retried), "retried=%d", retried)
	}
}

func TestFixedDelayIsCappedAtMax(t *testing.T) {
	// A base above the cap is clamped to the cap, still exact.
	strategy := NewWithKind(Fixed, time.Hour, 5*time.Minute)
	require.Equal(t, 5*time.Minute, strategy.Delay(0))
}

func TestKindBaseAndCapAccessors(t *testing.T) {
	strategy := NewWithKind(Fixed, 5*time.Second, time.Hour)
	require.Equal(t, Fixed, strategy.Kind())
	require.Equal(t, 5*time.Second, strategy.Base())
	require.Equal(t, time.Hour, strategy.Cap())

	// New defaults to exponential.
	require.Equal(t, Exponential, New(time.Second, time.Minute).Kind())
}

func TestParseKind(t *testing.T) {
	cases := map[string]Kind{
		"exponential": Exponential,
		"linear":      Linear,
		"fixed":       Fixed,
	}

	for name, want := range cases {
		got, ok := ParseKind(name)
		require.True(t, ok, name)
		require.Equal(t, want, got, name)
	}

	_, ok := ParseKind("quadratic")
	require.False(t, ok)
}
