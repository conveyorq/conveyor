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

package broker

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSentinelErrorsAreDistinct(t *testing.T) {
	sentinels := []error{ErrDuplicateTask, ErrTaskNotFound, ErrLeaseLost, ErrInvalidState}

	for i, left := range sentinels {
		for j, right := range sentinels {
			if i == j {
				require.ErrorIs(t, left, right)

				continue
			}

			require.NotErrorIs(t, left, right, "%v must not match %v", left, right)
		}
	}
}

func TestSentinelErrorsSurviveWrapping(t *testing.T) {
	wrapped := errors.Join(errors.New("context"), ErrLeaseLost)

	require.ErrorIs(t, wrapped, ErrLeaseLost)
	require.NotErrorIs(t, wrapped, ErrDuplicateTask)
}

func TestListLimitsAreSane(t *testing.T) {
	require.Positive(t, DefaultListLimit)
	require.Greater(t, MaxListLimit, DefaultListLimit)
}

func TestEffectiveListLimit(t *testing.T) {
	require.Equal(t, DefaultListLimit, EffectiveListLimit(0))
	require.Equal(t, DefaultListLimit, EffectiveListLimit(-5))
	require.Equal(t, 25, EffectiveListLimit(25))
	require.Equal(t, MaxListLimit, EffectiveListLimit(MaxListLimit))
	require.Equal(t, MaxListLimit, EffectiveListLimit(MaxListLimit+1))
}
