// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"context"

	"go.opentelemetry.io/otel/propagation"

	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
)

// tracerName is the instrumentation scope of server-side enqueue spans.
const tracerName = "github.com/conveyorq/conveyor/server"

// taskPropagator stamps the W3C trace context into the task envelope. The
// server speaks this format directly rather than the process global
// propagator, so a worker can always link its execution span to the enqueue.
var taskPropagator = propagation.TraceContext{}

// injectTaskTrace writes the current span's trace context into the envelope
// metadata, leaving any user metadata in place.
func injectTaskTrace(ctx context.Context, envelope *conveyorv1.TaskEnvelope) {
	if envelope.Metadata == nil {
		envelope.Metadata = map[string]string{}
	}

	taskPropagator.Inject(ctx, propagation.MapCarrier(envelope.Metadata))
}
