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
