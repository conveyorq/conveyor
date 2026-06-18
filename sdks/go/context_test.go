// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

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
