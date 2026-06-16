// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
)

// TestInjectTaskTraceStampsTraceparent verifies the enqueue trace lands in the
// envelope metadata as a W3C traceparent carrying the active span's trace id,
// without disturbing existing metadata.
func TestInjectTaskTraceStampsTraceparent(t *testing.T) {
	provider := sdktrace.NewTracerProvider()
	otel.SetTracerProvider(provider)

	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

	ctx, span := otel.Tracer("server").Start(context.Background(), "conveyor.enqueue")
	defer span.End()

	envelope := &conveyorv1.TaskEnvelope{Metadata: map[string]string{"tenant": "acme"}}
	injectTaskTrace(ctx, envelope)

	traceparent := envelope.GetMetadata()["traceparent"]
	require.NotEmpty(t, traceparent)
	require.True(t, strings.Contains(traceparent, span.SpanContext().TraceID().String()),
		"traceparent must carry the active trace id")
	require.Equal(t, "acme", envelope.GetMetadata()["tenant"], "user metadata must survive")
}

// TestInjectTaskTraceInitializesMetadata verifies a nil metadata map is created
// rather than panicking.
func TestInjectTaskTraceInitializesMetadata(t *testing.T) {
	provider := sdktrace.NewTracerProvider()
	otel.SetTracerProvider(provider)

	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

	ctx, span := otel.Tracer("server").Start(context.Background(), "conveyor.enqueue")
	defer span.End()

	envelope := &conveyorv1.TaskEnvelope{}
	injectTaskTrace(ctx, envelope)

	require.NotEmpty(t, envelope.GetMetadata()["traceparent"])
}
