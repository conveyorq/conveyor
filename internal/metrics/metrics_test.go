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

package metrics_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/conveyorq/conveyor/internal/metrics"
)

// TestEngineRecordsAllInstruments verifies every record method emits its
// instrument. A synchronous instrument only appears in a collection after it
// has a sample, so a metric's presence proves the record reached the meter.
func TestEngineRecordsAllInstruments(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	otel.SetMeterProvider(provider)

	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

	engine, err := metrics.NewEngine()
	require.NoError(t, err)

	ctx := context.Background()
	engine.RecordProcessDuration(ctx, 0.5, "default")
	engine.RecordQueueLatency(ctx, 0.2, "default")
	engine.LeaseExpired(ctx, 2)
	engine.WakeupsSwept(ctx, 3)
	engine.BreakerOpen(ctx)

	var collected metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &collected))

	recorded := map[string]bool{}

	for _, scope := range collected.ScopeMetrics {
		for _, instrument := range scope.Metrics {
			recorded[instrument.Name] = true
		}
	}

	for _, name := range []string{
		"conveyor.process.duration",
		"conveyor.queue.latency",
		"conveyor.lease.expired",
		"conveyor.wakeups.swept",
		"conveyor.breaker.open",
	} {
		require.True(t, recorded[name], "%s must be recorded", name)
	}
}
