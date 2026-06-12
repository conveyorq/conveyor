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
