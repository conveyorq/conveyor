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
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWithTaskValuesExposesHelpers(t *testing.T) {
	task := &Task{id: "01TASK", retried: 3, maxRetry: 25}

	ctx := withTaskValues(context.Background(), task)

	id, ok := GetTaskID(ctx)
	require.True(t, ok)
	require.Equal(t, "01TASK", id)

	retries, ok := GetRetryCount(ctx)
	require.True(t, ok)
	require.Equal(t, 3, retries)

	budget, ok := GetMaxRetry(ctx)
	require.True(t, ok)
	require.Equal(t, 25, budget)
}

func TestHelpersReportMissingValues(t *testing.T) {
	ctx := context.Background()

	_, ok := GetTaskID(ctx)
	require.False(t, ok)

	_, ok = GetRetryCount(ctx)
	require.False(t, ok)

	_, ok = GetMaxRetry(ctx)
	require.False(t, ok)
}
