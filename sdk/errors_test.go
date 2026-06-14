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

package conveyor

import (
	"errors"
	"fmt"
	"testing"
)

func TestSkipRetryDetection(t *testing.T) {
	cause := errors.New("payload decode failed")
	err := SkipRetry(cause)

	if !IsSkipRetry(err) {
		t.Fatal("IsSkipRetry must detect a SkipRetry error")
	}

	if !errors.Is(err, cause) {
		t.Fatal("SkipRetry must unwrap to its cause")
	}
}

func TestSkipRetrySurvivesWrapping(t *testing.T) {
	err := fmt.Errorf("handler: %w", SkipRetry(errors.New("bad input")))

	if !IsSkipRetry(err) {
		t.Fatal("IsSkipRetry must see through fmt.Errorf wrapping")
	}
}

func TestPlainErrorIsNotSkipRetry(t *testing.T) {
	if IsSkipRetry(errors.New("transient")) {
		t.Fatal("plain errors must not be skip-retry")
	}

	if IsSkipRetry(nil) {
		t.Fatal("nil must not be skip-retry")
	}
}
