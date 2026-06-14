// MIT License
//
// Copyright (c) 2026 ConveyorQ
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

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
