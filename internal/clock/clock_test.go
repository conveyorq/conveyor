// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package clock

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSystemClockTracksWallTime(t *testing.T) {
	source := System()

	before := time.Now() //nolint:forbidigo // comparing against the wall clock
	now := source.Now()
	after := time.Now() //nolint:forbidigo // comparing against the wall clock

	require.False(t, now.Before(before))
	require.False(t, now.After(after))
}

func TestFakeClockIsFrozenUntilAdvanced(t *testing.T) {
	start := time.Date(2026, 6, 12, 12, 0, 0, 0, time.UTC)
	fake := NewFake(start)

	require.Equal(t, start, fake.Now())
	require.Equal(t, start, fake.Now(), "the fake clock must not tick on its own")

	fake.Advance(90 * time.Second)
	require.Equal(t, start.Add(90*time.Second), fake.Now())
}
