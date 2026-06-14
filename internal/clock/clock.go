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

// Package clock provides the injectable time source used across the
// codebase. Calling time.Now directly is forbidden outside this package so
// that every time-dependent behavior can be driven deterministically in
// tests.
package clock

import (
	"sync"
	"time"
)

// Clock supplies the current time.
type Clock interface {
	// Now returns the current time.
	Now() time.Time
}

// systemClock reads the wall clock.
type systemClock struct{}

// Now returns the current wall-clock time.
func (systemClock) Now() time.Time {
	return time.Now() //nolint:forbidigo // the one sanctioned call site
}

// System returns the wall-clock Clock used in production.
func System() Clock {
	return systemClock{}
}

// Fake is a manually advanced Clock for tests. It is safe for concurrent
// use.
type Fake struct {
	// mutex guards now.
	mutex sync.Mutex
	// now is the frozen current time.
	now time.Time
}

// NewFake returns a Fake clock frozen at start.
func NewFake(start time.Time) *Fake {
	return &Fake{now: start}
}

// Now returns the frozen current time.
func (x *Fake) Now() time.Time {
	x.mutex.Lock()
	defer x.mutex.Unlock()

	return x.now
}

// Advance moves the clock forward by d.
func (x *Fake) Advance(d time.Duration) {
	x.mutex.Lock()
	defer x.mutex.Unlock()

	x.now = x.now.Add(d)
}
