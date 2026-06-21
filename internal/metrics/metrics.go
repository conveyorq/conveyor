// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

// Package metrics holds the engine's synchronous OpenTelemetry instruments —
// the per-task timing histograms and the maintenance canaries — behind a small
// record API. The engine actors call the record methods; this package owns the
// instrument definitions so observability concerns stay out of the actor code.
package metrics

import (
	"context"
	"errors"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// scope is the instrumentation scope, matching the one the server uses for the
// observable counters so every conveyor.* series shares one scope.
const scope = "github.com/conveyorq/conveyor"

// queueAttr labels the timing histograms with the queue name.
const queueAttr = "queue"

// Engine holds the synchronous instruments recorded at the engine's event
// sites. They record into the process-global meter provider — the server
// installs a Prometheus-backed one; without it the records are no-ops.
type Engine struct {
	processDuration    metric.Float64Histogram
	queueLatency       metric.Float64Histogram
	leaseExpired       metric.Int64Counter
	wakeupsSwept       metric.Int64Counter
	breakerOpen        metric.Int64Counter
	rateLimited        metric.Int64Counter
	concurrencyLimited metric.Int64Counter
}

// NewEngine creates the engine instruments from the global meter. The returned
// value is always usable; a non-nil error only reports a registration problem
// the caller can log, since OpenTelemetry still hands back working instruments.
func NewEngine() (*Engine, error) {
	meter := otel.Meter(scope)

	processDuration, e1 := meter.Float64Histogram("conveyor.process.duration",
		metric.WithUnit("s"), metric.WithDescription("Task execution time from dispatch to completion."))
	queueLatency, e2 := meter.Float64Histogram("conveyor.queue.latency",
		metric.WithUnit("s"), metric.WithDescription("Time a task waited from enqueue to dispatch."))
	leaseExpired, e3 := meter.Int64Counter("conveyor.lease.expired",
		metric.WithDescription("Queues with expired leases reclaimed by the reaper."))
	wakeupsSwept, e4 := meter.Int64Counter("conveyor.wakeups.swept",
		metric.WithDescription("Queues re-woken by the reaper's pending sweep (recovers lost wake-ups)."))
	breakerOpen, e5 := meter.Int64Counter("conveyor.breaker.open",
		metric.WithDescription("Completions deferred because a task type's circuit breaker was open."))
	rateLimited, e6 := meter.Int64Counter("conveyor.ratelimit.throttled",
		metric.WithDescription("Lease cycles a queue deferred because its dispatch rate limit was exhausted."))
	concurrencyLimited, e7 := meter.Int64Counter("conveyor.concurrency.throttled",
		metric.WithDescription("Lease cycles in which a queue held a task back because its concurrency key was saturated."))

	return &Engine{
		processDuration:    processDuration,
		queueLatency:       queueLatency,
		leaseExpired:       leaseExpired,
		wakeupsSwept:       wakeupsSwept,
		breakerOpen:        breakerOpen,
		rateLimited:        rateLimited,
		concurrencyLimited: concurrencyLimited,
	}, errors.Join(e1, e2, e3, e4, e5, e6, e7)
}

// RecordProcessDuration records one execution's dispatch→completion time.
func (e *Engine) RecordProcessDuration(ctx context.Context, seconds float64, queue string) {
	e.processDuration.Record(ctx, seconds, metric.WithAttributes(attribute.String(queueAttr, queue)))
}

// RecordQueueLatency records one task's enqueue→dispatch wait.
func (e *Engine) RecordQueueLatency(ctx context.Context, seconds float64, queue string) {
	e.queueLatency.Record(ctx, seconds, metric.WithAttributes(attribute.String(queueAttr, queue)))
}

// LeaseExpired counts queues whose expired leases the reaper reclaimed.
func (e *Engine) LeaseExpired(ctx context.Context, queues int) {
	e.leaseExpired.Add(ctx, int64(queues))
}

// WakeupsSwept counts queues the reaper re-woke from its pending sweep.
func (e *Engine) WakeupsSwept(ctx context.Context, queues int) {
	e.wakeupsSwept.Add(ctx, int64(queues))
}

// BreakerOpen counts a completion deferred by an open circuit breaker.
func (e *Engine) BreakerOpen(ctx context.Context) {
	e.breakerOpen.Add(ctx, 1)
}

// RateLimited counts one lease cycle a queue deferred on an exhausted rate
// limit, labeled by queue.
func (e *Engine) RateLimited(ctx context.Context, queue string) {
	e.rateLimited.Add(ctx, 1, metric.WithAttributes(attribute.String(queueAttr, queue)))
}

// ConcurrencyLimited counts one lease cycle in which a queue held a task back
// because its concurrency key was saturated, labeled by queue.
func (e *Engine) ConcurrencyLimited(ctx context.Context, queue string) {
	e.concurrencyLimited.Add(ctx, 1, metric.WithAttributes(attribute.String(queueAttr, queue)))
}
