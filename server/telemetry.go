// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

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

// engineCounter binds a cumulative engine counter to its observable
// instrument so the description, the atomic reader, and the instrument stay on
// one record.
type engineCounter struct {
	// name is the instrument name.
	name string
	// description is the instrument help text.
	description string
	// read returns the counter's current value.
	read func() int64
	// instrument is the registered observable counter.
	instrument metric.Int64ObservableCounter
}

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

// telemetry owns conveyord's OpenTelemetry pipeline: a meter provider scraped
// in Prometheus text format from /metrics, and a tracer provider. Both are
// installed as the process-global providers (so GoAkt and the trace
// propagation helpers record into them), and both also push over OTLP when an
// endpoint is configured. The W3C trace-context propagator is installed so the
// enqueue span travels to the worker through the task envelope.
type telemetry struct {
	// meterProvider is the OpenTelemetry SDK meter provider.
	meterProvider *sdkmetric.MeterProvider
	// tracerProvider is the OpenTelemetry SDK tracer provider.
	tracerProvider *sdktrace.TracerProvider
	// registry backs the Prometheus exporter and the /metrics handler.
	registry *prometheus.Registry
}

// newTelemetry builds the meter and tracer providers and installs them as the
// process-global providers. The Prometheus reader is always present; an OTLP
// push exporter is added to both signals when config.Endpoint is set. The
// providers must be shut down to flush and release them.
func newTelemetry(ctx context.Context, config OtelConfig) (*telemetry, error) {
	res, err := resource.Merge(
		resource.Default(),
		resource.NewSchemaless(attribute.String(serviceNameKey, config.ServiceName)),
	)
	if err != nil {
		return nil, fmt.Errorf("building telemetry resource: %w", err)
	}

	registry := prometheus.NewRegistry()

	promExporter, err := otelprom.New(otelprom.WithRegisterer(registry))
	if err != nil {
		return nil, fmt.Errorf("building prometheus exporter: %w", err)
	}

	meterOptions := []sdkmetric.Option{sdkmetric.WithReader(promExporter), sdkmetric.WithResource(res)}
	traceOptions := []sdktrace.TracerProviderOption{sdktrace.WithResource(res)}

	if config.Endpoint != "" {
		metricExporter, err := otlpmetricgrpc.New(ctx,
			otlpmetricgrpc.WithEndpoint(config.Endpoint), otlpmetricgrpc.WithInsecure())
		if err != nil {
			return nil, fmt.Errorf("building OTLP metric exporter: %w", err)
		}

		traceExporter, err := otlptracegrpc.New(ctx,
			otlptracegrpc.WithEndpoint(config.Endpoint), otlptracegrpc.WithInsecure())
		if err != nil {
			return nil, fmt.Errorf("building OTLP trace exporter: %w", err)
		}

		meterOptions = append(meterOptions, sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter)))
		traceOptions = append(traceOptions, sdktrace.WithBatcher(traceExporter))
	}

	meterProvider := sdkmetric.NewMeterProvider(meterOptions...)
	tracerProvider := sdktrace.NewTracerProvider(traceOptions...)

	otel.SetMeterProvider(meterProvider)
	otel.SetTracerProvider(tracerProvider)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	return &telemetry{meterProvider: meterProvider, tracerProvider: tracerProvider, registry: registry}, nil
}

// handler serves the current metrics in Prometheus text format.
func (t *telemetry) handler() http.Handler {
	return promhttp.HandlerFor(t.registry, promhttp.HandlerOpts{})
}

// shutdown flushes and stops the meter and tracer providers.
func (t *telemetry) shutdown(ctx context.Context) error {
	return errors.Join(t.meterProvider.Shutdown(ctx), t.tracerProvider.Shutdown(ctx))
}

// registerEngineMetrics publishes conveyor's core counters and the per-queue
// pending-depth gauge as asynchronous instruments, read on every collection.
// The engine's counters are cumulative atomics, so they map to observable
// counters; active executions and pending depth are point-in-time, so they
// map to gauges. The broker is probed for pending depth inside the callback.
func (t *telemetry) registerEngineMetrics(engine *actors.Engine, taskLog broker.Broker, timeSource clock.Clock, activeSessions func() int64) error {
	meter := t.meterProvider.Meter(metricsScope)
	counters := engine.Counters()
	pendingProbe := &pendingCache{taskLog: taskLog, timeSource: timeSource}

	// One entry per cumulative counter: name, help text, and the atomic to
	// read. Keeping description and reader on one record removes the chance of
	// two parallel maps drifting out of sync.
	counterMetrics := []engineCounter{
		{name: "conveyor.enqueued", description: "Tasks durably committed through the engine.", read: counters.Enqueued.Load},
		{name: "conveyor.dispatched", description: "ExecuteTask messages handed to gateways.", read: counters.Dispatched.Load},
		{name: "conveyor.completed", description: "Successful executions reported back.", read: counters.Completed.Load},
		{name: "conveyor.failed", description: "Failed executions reported back.", read: counters.Failed.Load},
		{name: "conveyor.retried", description: "Executions returned for a later retry.", read: counters.Retried.Load},
		{name: "conveyor.archived", description: "Executions dead-lettered (retries exhausted, skip-retry, or cancel).", read: counters.Archived.Load},
		{name: "conveyor.released", description: "Deliveries returned to the queue for redelivery.", read: counters.Released.Load},
	}

	for i := range counterMetrics {
		instrument, err := meter.Int64ObservableCounter(counterMetrics[i].name,
			metric.WithDescription(counterMetrics[i].description))
		if err != nil {
			return err
		}

		counterMetrics[i].instrument = instrument
	}

	active, err := meter.Int64ObservableGauge("conveyor.active",
		metric.WithDescription("Executions currently in flight."))
	if err != nil {
		return err
	}

	sessions, err := meter.Int64ObservableGauge("conveyor.sessions.active",
		metric.WithDescription("Live worker sessions."))
	if err != nil {
		return err
	}

	pending, err := meter.Int64ObservableGauge("conveyor.pending",
		metric.WithDescription("Tasks due and waiting to dispatch, per queue."))
	if err != nil {
		return err
	}

	instruments := []metric.Observable{active, sessions, pending}
	for _, counterMetric := range counterMetrics {
		instruments = append(instruments, counterMetric.instrument)
	}

	_, err = meter.RegisterCallback(
		func(ctx context.Context, observer metric.Observer) error {
			for _, counterMetric := range counterMetrics {
				observer.ObserveInt64(counterMetric.instrument, counterMetric.read())
			}

			observer.ObserveInt64(active, counters.Active.Load())
			observer.ObserveInt64(sessions, activeSessions())

			counts, err := pendingProbe.get(ctx)
			if err != nil {
				return fmt.Errorf("observing pending depth: %w", err)
			}

			for queue, n := range counts {
				observer.ObserveInt64(pending, n, metric.WithAttributes(attribute.String(queueAttr, queue)))
			}

			return nil
		},
		instruments...,
	)

	return err
}
