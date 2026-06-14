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

package server

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	conveyor "github.com/conveyorq/conveyor/sdk"
)

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
