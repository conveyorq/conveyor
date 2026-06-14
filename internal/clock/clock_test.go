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
