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
	processDuration metric.Float64Histogram
	queueLatency    metric.Float64Histogram
	leaseExpired    metric.Int64Counter
	wakeupsSwept    metric.Int64Counter
	breakerOpen     metric.Int64Counter
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

	return &Engine{
		processDuration: processDuration,
		queueLatency:    queueLatency,
		leaseExpired:    leaseExpired,
		wakeupsSwept:    wakeupsSwept,
		breakerOpen:     breakerOpen,
	}, errors.Join(e1, e2, e3, e4, e5)
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
