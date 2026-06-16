// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	conveyor "github.com/conveyorq/conveyor/sdk"
)

// TestMetricsRecordsExecutionTiming is the end-to-end check that the §13 timing
// histograms fire on the real gateway path: a worker processes one task, after
// which the process-duration and queue-latency series appear at /metrics.
func TestMetricsRecordsExecutionTiming(t *testing.T) {
	node := startTestServer(t)
	addr := "http://" + node.Addr()

	worker, err := conveyor.NewWorker(addr, conveyor.WithConcurrency(4), conveyor.WithQueues(map[string]int{"default": 1}))
	require.NoError(t, err)

	mux := conveyor.NewMux()
	mux.HandleFunc("bench:noop", func(context.Context, *conveyor.Task) error { return nil })

	workerCtx, stopWorker := context.WithCancel(context.Background())
	t.Cleanup(stopWorker)

	go func() { _ = worker.Run(workerCtx, mux) }()

	client, err := conveyor.NewClient(addr)
	require.NoError(t, err)

	_, err = client.Enqueue(context.Background(), conveyor.NewTask("bench:noop", conveyor.Bytes(nil)), conveyor.Retention(time.Hour))
	require.NoError(t, err)

	// The histograms are synchronous instruments — they appear only after the
	// task has been dispatched and completed.
	require.Eventually(t, func() bool {
		resp, scrapeErr := http.Get(fmt.Sprintf("http://%s%s", node.MetricsAddr(), metricsPath))
		if scrapeErr != nil {
			return false
		}

		defer func() { _ = resp.Body.Close() }()
		body, _ := io.ReadAll(resp.Body)

		return strings.Contains(string(body), "conveyor_process_duration_seconds")
	}, 15*time.Second, 100*time.Millisecond, "process-duration histogram must appear after a task runs")

	require.Contains(t, scrapeMetrics(t, node), "conveyor_queue_latency_seconds")
}

// scrapeMetrics fetches the Prometheus exposition from the node's /metrics
// endpoint and returns it as text.
func scrapeMetrics(t *testing.T, node *Server) string {
	t.Helper()

	resp, err := http.Get(fmt.Sprintf("http://%s%s", node.MetricsAddr(), metricsPath))
	require.NoError(t, err)

	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return string(body)
}

// TestMetricsEndpointServesEngineCounters verifies that /metrics exposes the
// core engine instruments and that an enqueue moves them: the enqueued
// counter and the per-queue pending gauge both reflect the committed task.
func TestMetricsEndpointServesEngineCounters(t *testing.T) {
	node := startTestServer(t)

	client, err := conveyor.NewClient("http://" + node.Addr())
	require.NoError(t, err)

	// Enqueue before the first scrape so the pending-depth probe (which is
	// cached per scrape window) captures the committed task on its first run.
	_, err = client.Enqueue(context.Background(), conveyor.NewTask("email:welcome", conveyor.Bytes(nil)))
	require.NoError(t, err)

	// No worker is connected, so the task stays pending: the enqueued counter
	// is non-zero, the active gauge is exported, and the queue surfaces in the
	// pending gauge. The exporter decorates each series with OpenTelemetry
	// scope labels, so match the value past the label set.
	body := scrapeMetrics(t, node)
	require.Regexp(t, regexp.MustCompile(`conveyor_enqueued_total\{[^}]*\} 1`), body)
	require.Contains(t, body, "conveyor_active")
	require.Contains(t, body, "conveyor_pending")

	// The full §13 counter set and the session gauge are registered and
	// exported even at zero.
	for _, series := range []string{
		"conveyor_retried_total", "conveyor_archived_total", "conveyor_released_total",
		"conveyor_sessions_active",
	} {
		require.Contains(t, body, series)
	}
}

// TestMetricsEndpointExposesActorMetrics verifies that GoAkt's own metrics,
// enabled through WithMetrics, reach the same Prometheus exporter — proving
// the global meter provider is installed before the actor system starts.
func TestMetricsEndpointExposesActorMetrics(t *testing.T) {
	node := startTestServer(t)

	body := scrapeMetrics(t, node)

	// GoAkt namespaces its instruments under "actor_system"; any such series
	// confirms its metric provider is recording into our exporter.
	require.True(t, strings.Contains(body, "actor_system"),
		"expected GoAkt actor_system metrics in:\n%s", body)
}
