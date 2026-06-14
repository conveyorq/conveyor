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
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"

	"github.com/conveyorq/conveyor/internal/actors"
	"github.com/conveyorq/conveyor/internal/broker"
	"github.com/conveyorq/conveyor/internal/clock"
)

// metricsScope is the instrumentation scope of conveyor's own meters.
const metricsScope = "github.com/conveyorq/conveyor"

// serviceNameKey is the resource attribute identifying the service in
// exported metrics. It mirrors the OpenTelemetry semantic convention without
// pinning a semconv package version.
const serviceNameKey = "service.name"

// queueAttr labels per-queue metrics with the queue name.
const queueAttr = "queue"

// pendingCacheTTL bounds how stale the per-queue pending depth may be. The
// gauge is read on every Prometheus scrape; without a cache each scrape would
// run a COUNT query against the broker, so frequent scrapes (or several
// Prometheus replicas) are coalesced into at most one query per TTL.
const pendingCacheTTL = 5 * time.Second

// pendingCache memoizes the broker's per-queue pending depth for a short
// window so repeated scrapes share one query.
type pendingCache struct {
	// taskLog is the broker probed for pending depth.
	taskLog broker.Broker
	// timeSource stamps cache entries.
	timeSource clock.Clock
	// mu guards the cached snapshot.
	mu sync.Mutex
	// fetchedAt is when counts was last refreshed.
	fetchedAt time.Time
	// counts is the last per-queue pending snapshot.
	counts map[string]int64
}

// get returns the per-queue pending depth, refreshing from the broker only
// when the cached snapshot is older than pendingCacheTTL.
func (c *pendingCache) get(ctx context.Context) (map[string]int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := c.timeSource.Now()
	if c.counts != nil && now.Sub(c.fetchedAt) < pendingCacheTTL {
		return c.counts, nil
	}

	counts, err := c.taskLog.PendingCount(ctx)
	if err != nil {
		return nil, err
	}

	c.counts = counts
	c.fetchedAt = now

	return counts, nil
}

// telemetry owns the metrics pipeline: an OpenTelemetry meter provider whose
// readings are scraped in Prometheus text format from /metrics. It is also
// installed as the process-global meter provider so GoAkt's own actor and
// cluster metrics record into the same exporter.
type telemetry struct {
	// provider is the OpenTelemetry SDK meter provider.
	provider *sdkmetric.MeterProvider
	// registry backs the Prometheus exporter and the /metrics handler.
	registry *prometheus.Registry
}

// newTelemetry builds the meter provider, wires the Prometheus exporter, and
// installs the provider globally so GoAkt records into it. The provider must
// be shut down to flush and release it.
func newTelemetry(serviceName string) (*telemetry, error) {
	registry := prometheus.NewRegistry()

	exporter, err := otelprom.New(otelprom.WithRegisterer(registry))
	if err != nil {
		return nil, fmt.Errorf("building prometheus exporter: %w", err)
	}

	res, err := resource.Merge(
		resource.Default(),
		resource.NewSchemaless(attribute.String(serviceNameKey, serviceName)),
	)
	if err != nil {
		return nil, fmt.Errorf("building telemetry resource: %w", err)
	}

	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(exporter),
		sdkmetric.WithResource(res),
	)

	otel.SetMeterProvider(provider)

	return &telemetry{provider: provider, registry: registry}, nil
}

// handler serves the current metrics in Prometheus text format.
func (t *telemetry) handler() http.Handler {
	return promhttp.HandlerFor(t.registry, promhttp.HandlerOpts{})
}

// shutdown flushes and stops the meter provider.
func (t *telemetry) shutdown(ctx context.Context) error {
	return t.provider.Shutdown(ctx)
}

// registerEngineMetrics publishes conveyor's core counters and the per-queue
// pending-depth gauge as asynchronous instruments, read on every collection.
// The engine's counters are cumulative atomics, so they map to observable
// counters; active executions and pending depth are point-in-time, so they
// map to gauges. The broker is probed for pending depth inside the callback.
func (t *telemetry) registerEngineMetrics(engine *actors.Engine, taskLog broker.Broker, timeSource clock.Clock) error {
	meter := t.provider.Meter(metricsScope)
	counters := engine.Counters()
	pendingProbe := &pendingCache{taskLog: taskLog, timeSource: timeSource}

	enqueued, err := meter.Int64ObservableCounter("conveyor.enqueued",
		metric.WithDescription("Tasks durably committed through the engine."))
	if err != nil {
		return err
	}

	dispatched, err := meter.Int64ObservableCounter("conveyor.dispatched",
		metric.WithDescription("ExecuteTask messages handed to gateways."))
	if err != nil {
		return err
	}

	completed, err := meter.Int64ObservableCounter("conveyor.completed",
		metric.WithDescription("Successful executions reported back."))
	if err != nil {
		return err
	}

	failed, err := meter.Int64ObservableCounter("conveyor.failed",
		metric.WithDescription("Failed executions reported back."))
	if err != nil {
		return err
	}

	active, err := meter.Int64ObservableGauge("conveyor.active",
		metric.WithDescription("Executions currently in flight."))
	if err != nil {
		return err
	}

	pending, err := meter.Int64ObservableGauge("conveyor.pending",
		metric.WithDescription("Tasks due and waiting to dispatch, per queue."))
	if err != nil {
		return err
	}

	_, err = meter.RegisterCallback(
		func(ctx context.Context, observer metric.Observer) error {
			observer.ObserveInt64(enqueued, counters.Enqueued.Load())
			observer.ObserveInt64(dispatched, counters.Dispatched.Load())
			observer.ObserveInt64(completed, counters.Completed.Load())
			observer.ObserveInt64(failed, counters.Failed.Load())
			observer.ObserveInt64(active, counters.Active.Load())

			counts, err := pendingProbe.get(ctx)
			if err != nil {
				return fmt.Errorf("observing pending depth: %w", err)
			}

			for queue, n := range counts {
				observer.ObserveInt64(pending, n, metric.WithAttributes(attribute.String(queueAttr, queue)))
			}

			return nil
		},
		enqueued, dispatched, completed, failed, active, pending,
	)

	return err
}
