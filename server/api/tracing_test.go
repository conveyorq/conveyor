// MIT License
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
