// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/conveyorq/conveyor/internal/clock"
	conveyor "github.com/conveyorq/conveyor/sdk"
)

// Latency gate parameters: p99 enqueue-to-handler-start under 50ms at
// 1k tasks/s on one node. The gate is provisional until it is
// recalibrated against measured baselines on pinned CI hardware.
const (
	latencyGateTasks    = 3000
	latencyGateInterval = time.Millisecond
	latencyGateP99      = 50 * time.Millisecond
)

// TestLatencyGateEnqueueToHandlerStart measures the push-based payoff:
// the p99 latency from the Enqueue call returning to the handler starting
// on the worker, under a paced 1k tasks/s load.
func TestLatencyGateEnqueueToHandlerStart(t *testing.T) {
	if testing.Short() {
		t.Skip("latency gate skipped in -short mode")
	}

	baseURL := startNode(t)

	client, err := conveyor.NewClient(baseURL)
	require.NoError(t, err)

	worker, err := conveyor.NewWorker(baseURL,
		conveyor.WithQueues(map[string]int{"default": 1}),
		conveyor.WithConcurrency(64),
	)
	require.NoError(t, err)

	var startMutex sync.Mutex
	started := make(map[string]time.Time, latencyGateTasks)

	mux := conveyor.NewMux()

	mux.HandleFunc("latency:probe", func(_ context.Context, task *conveyor.Task) error {
		now := clock.System().Now()

		startMutex.Lock()
		started[task.ID()] = now
		startMutex.Unlock()

		return nil
	})

	go func() { _ = worker.Run(t.Context(), mux) }()

	// Give the session a moment to register before the measured window.
	time.Sleep(time.Second)

	ctx := context.Background()
	enqueued := make(map[string]time.Time, latencyGateTasks)
	ticker := time.NewTicker(latencyGateInterval)

	defer ticker.Stop()

	for range latencyGateTasks {
		<-ticker.C

		info, err := client.Enqueue(ctx, conveyor.NewTask("latency:probe", conveyor.Bytes(nil)),
			conveyor.Retention(time.Hour))
		require.NoError(t, err)

		enqueued[info.ID] = clock.System().Now()
	}

	require.Eventually(t, func() bool {
		startMutex.Lock()
		defer startMutex.Unlock()

		return len(started) == latencyGateTasks
	}, time.Minute, 20*time.Millisecond, "every probe task must start")

	latencies := make([]time.Duration, 0, latencyGateTasks)

	startMutex.Lock()

	for id, enqueuedAt := range enqueued {
		latencies = append(latencies, started[id].Sub(enqueuedAt))
	}

	startMutex.Unlock()

	slices.Sort(latencies)

	p50 := latencies[len(latencies)/2]
	p99 := latencies[len(latencies)*99/100]

	t.Logf("enqueue->handler-start latency: p50=%s p99=%s (n=%d)", p50, p99, len(latencies))

	require.Lessf(t, p99, latencyGateP99,
		"p99 enqueue->handler-start %s exceeds the %s gate", p99, latencyGateP99)
}
