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
