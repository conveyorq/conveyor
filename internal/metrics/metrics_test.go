// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

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
