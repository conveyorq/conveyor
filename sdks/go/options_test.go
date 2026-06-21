// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

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
		WithMinServerVersion("v1.2.0"),
	} {
		opt(settings)
	}

	require.Equal(t, "secret", settings.token)
	require.Equal(t, map[string]int{"critical": 6, "default": 3}, settings.queues)
	require.Equal(t, 20, settings.concurrency)
	require.Equal(t, "v1.2.0", settings.minServerVersion)
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

func TestConcurrencyKeyOption(t *testing.T) {
	settings := &enqueueOptions{}
	ConcurrencyKey("customer:42")(settings)
	require.Equal(t, "customer:42", settings.concurrencyKey)
}
