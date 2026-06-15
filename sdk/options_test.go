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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestOptionsApply(t *testing.T) {
	settings := &options{}

	for _, opt := range []Option{
		WithToken("secret"),
		WithQueues(map[string]int{"critical": 6, "default": 3}),
		WithConcurrency(20),
	} {
		opt(settings)
	}

	require.Equal(t, "secret", settings.token)
	require.Equal(t, map[string]int{"critical": 6, "default": 3}, settings.queues)
	require.Equal(t, 20, settings.concurrency)
}

func TestEnqueueOptionsApply(t *testing.T) {
	processAt := time.Date(2026, 6, 12, 12, 0, 0, 0, time.UTC)
	deadline := time.Date(2026, 6, 12, 13, 0, 0, 0, time.UTC)
	settings := &enqueueOptions{}

	for _, opt := range []EnqueueOption{
		TaskID("01JXAMPLE"),
		Queue("critical"),
		MaxRetry(10),
		Priority(7),
		Timeout(30 * time.Second),
		Deadline(deadline),
		ProcessAt(processAt),
		ProcessIn(5 * time.Minute),
		Retention(48 * time.Hour),
		Unique(24 * time.Hour),
		UniqueKey("user:42:welcome"),
	} {
		opt(settings)
	}

	require.Equal(t, "01JXAMPLE", settings.taskID)
	require.Equal(t, "critical", settings.queue)
	require.Equal(t, 10, settings.maxRetry)
	require.Equal(t, 7, settings.priority)
	require.Equal(t, 30*time.Second, settings.timeout)
	require.Equal(t, deadline, settings.deadline)
	require.Equal(t, processAt, settings.processAt)
	require.Equal(t, 5*time.Minute, settings.processIn)
	require.Equal(t, 48*time.Hour, settings.retention)
	require.Equal(t, 24*time.Hour, settings.uniqueTTL)
	require.Equal(t, "user:42:welcome", settings.uniqueKey)
}
