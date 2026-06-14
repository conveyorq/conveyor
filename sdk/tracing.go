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

package conveyor

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// tracerName is the instrumentation scope of worker-side spans.
const tracerName = "github.com/conveyorq/conveyor/sdk"

// taskPropagator carries the W3C trace context the server stamped into the
// task metadata. The SDK speaks this format directly rather than the process
// global propagator, so propagation works whether or not the worker installed
// one.
var taskPropagator = propagation.TraceContext{}

// traced runs the handler inside a span linked to the enqueue trace carried in
// the task metadata, recording an error outcome on the span. When the worker
// process has not configured an OpenTelemetry tracer provider, the span is a
// no-op and the handler runs unchanged.
func traced(ctx context.Context, task *Task, run func(context.Context) error) error {
	ctx = taskPropagator.Extract(ctx, propagation.MapCarrier(task.metadata))

	ctx, span := otel.Tracer(tracerName).Start(ctx, "conveyor.process "+task.taskType,
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("conveyor.task.id", task.id),
			attribute.String("conveyor.task.type", task.taskType),
			attribute.String("conveyor.queue", task.queue),
		))
	defer span.End()

	err := run(ctx)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}

	return err
}
