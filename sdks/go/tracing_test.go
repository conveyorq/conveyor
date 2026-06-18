// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package conveyor

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func recorderProvider(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()

	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	otel.SetTracerProvider(provider)

	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

	return recorder
}

// TestTracedLinksToEnqueueTrace is the Phase 6 trace-propagation acceptance:
// the worker's execution span continues the trace the server stamped into the
// task metadata at enqueue time.
func TestTracedLinksToEnqueueTrace(t *testing.T) {
	recorder := recorderProvider(t)

	// Stand in for the server: produce an enqueue span and inject its W3C
	// trace context into the task metadata exactly as the server does.
	enqueueCtx, enqueueSpan := otel.Tracer("server").Start(context.Background(), "conveyor.enqueue")
	metadata := map[string]string{}
	propagation.TraceContext{}.Inject(enqueueCtx, propagation.MapCarrier(metadata))
	enqueueSpan.End()

	require.NotEmpty(t, metadata["traceparent"], "server must stamp a traceparent")

	task := &Task{id: "t1", taskType: "email:welcome", queue: "default", metadata: metadata}
	require.NoError(t, traced(context.Background(), task, func(context.Context) error { return nil }))

	var process, enqueue sdktrace.ReadOnlySpan

	for _, span := range recorder.Ended() {
		switch span.Name() {
		case "conveyor.process email:welcome":
			process = span
		case "conveyor.enqueue":
			enqueue = span
		}
	}

	require.NotNil(t, process, "worker must emit a process span")
	require.NotNil(t, enqueue)
	require.Equal(t, enqueue.SpanContext().TraceID(), process.SpanContext().TraceID(),
		"process span must share the enqueue trace")
	require.Equal(t, enqueue.SpanContext().SpanID(), process.Parent().SpanID(),
		"the enqueue span must be the process span's parent")
}

func TestTracedRecordsHandlerError(t *testing.T) {
	recorder := recorderProvider(t)

	task := &Task{id: "t1", taskType: "email:welcome", queue: "default"}
	want := errors.New("boom")

	err := traced(context.Background(), task, func(context.Context) error { return want })
	require.ErrorIs(t, err, want)

	spans := recorder.Ended()
	require.Len(t, spans, 1)
	require.Equal(t, codes.Error, spans[0].Status().Code)
}
