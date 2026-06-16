// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	conveyor "github.com/conveyorq/conveyor/sdk"
)

// collectorImage pins the OpenTelemetry Collector used to receive exported
// spans over OTLP.
const collectorImage = "otel/opentelemetry-collector:0.114.0"

// collectorOTLPPort is the collector's OTLP/gRPC receiver port.
const collectorOTLPPort = "4317/tcp"

// collectorConfig configures the collector to accept OTLP/gRPC traces and echo
// each span to stdout in detail, so the test can assert on the span names and
// trace IDs that actually crossed the wire.
const collectorConfig = `receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317
exporters:
  debug:
    verbosity: detailed
service:
  telemetry:
    metrics:
      level: none
  pipelines:
    traces:
      receivers: [otlp]
      exporters: [debug]
    metrics:
      receivers: [otlp]
      exporters: [debug]
`

// spanMarker begins a span block in the detailed exporter's output; metric
// blocks never carry one, so it gates span-name parsing away from metrics.
var spanMarker = regexp.MustCompile(`Span #\d+`)

// metricMarker begins a metric block in the detailed exporter's output.
var metricMarker = regexp.MustCompile(`Metric #\d+`)

// traceIDLine matches the collector's detailed-exporter "Trace ID" line.
var traceIDLine = regexp.MustCompile(`Trace ID\s*:\s*([0-9a-f]+)`)

// spanNameLine matches the collector's detailed-exporter "Name" line.
var spanNameLine = regexp.MustCompile(`Name\s*:\s*(\S+)`)

// TestTracingReachesRealCollector is the end-to-end trace check: a conveyor
// server configured with an OTLP endpoint enqueues a task that a real worker
// processes, and a real OpenTelemetry Collector must receive both the
// server-side enqueue span and the worker-side process span sharing one trace
// ID. This proves the OTLP export pipeline and W3C context propagation work
// against a genuine collector, not just an in-memory recorder.
func TestTracingReachesRealCollector(t *testing.T) {
	if testing.Short() {
		t.Skip("collector trace e2e skipped in -short mode")
	}

	ctx := context.Background()

	configPath := filepath.Join(t.TempDir(), "collector.yaml")
	require.NoError(t, os.WriteFile(configPath, []byte(collectorConfig), 0o600))

	collector, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        collectorImage,
			ExposedPorts: []string{collectorOTLPPort},
			Files: []testcontainers.ContainerFile{{
				HostFilePath:      configPath,
				ContainerFilePath: "/etc/otelcol/config.yaml",
				FileMode:          0o644,
			}},
			WaitingFor: wait.ForLog("Everything is ready. Begin running and processing data."),
		},
		Started: true,
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = collector.Terminate(stopCtx)
	})

	endpoint, err := collector.PortEndpoint(ctx, collectorOTLPPort, "")
	require.NoError(t, err)

	node := startTracingTestServer(t, endpoint)
	addr := "http://" + node.Addr()

	worker, err := conveyor.NewWorker(addr, conveyor.WithConcurrency(4), conveyor.WithQueues(map[string]int{"default": 1}))
	require.NoError(t, err)

	mux := conveyor.NewMux()
	mux.HandleFunc("trace:noop", func(context.Context, *conveyor.Task) error { return nil })

	workerCtx, stopWorker := context.WithCancel(context.Background())
	t.Cleanup(stopWorker)

	go func() { _ = worker.Run(workerCtx, mux) }()

	client, err := conveyor.NewClient(addr)
	require.NoError(t, err)

	_, err = client.Enqueue(context.Background(),
		conveyor.NewTask("trace:noop", conveyor.Bytes(nil)), conveyor.Retention(time.Hour))
	require.NoError(t, err)

	// Spans flush on the batch processor's schedule, so poll the collector's
	// log until both expected spans have arrived sharing one trace ID.
	var enqueueTrace, processTrace string

	require.Eventually(t, func() bool {
		byName := collectorSpanTraces(t, collector)
		enqueueTrace = byName["conveyor.enqueue"]

		for name, traceID := range byName {
			if strings.HasPrefix(name, "conveyor.process") {
				processTrace = traceID
			}
		}

		return enqueueTrace != "" && processTrace != ""
	}, 30*time.Second, 500*time.Millisecond, "collector must receive both the enqueue and process spans")

	require.Equal(t, enqueueTrace, processTrace, "enqueue and process spans must share one trace ID")
}

// startTracingTestServer boots a dev-config server that exports traces to the
// given OTLP endpoint, on ephemeral API and metrics ports.
func startTracingTestServer(t *testing.T, otlpEndpoint string) *Server {
	t.Helper()

	ports := freePorts(t, 3)

	config := DevConfig()
	config.API.Listen = "127.0.0.1:0"
	config.Metrics.Listen = "127.0.0.1:0"
	config.Cluster.RemotingPort = ports[0]
	config.Cluster.DiscoveryPort = ports[1]
	config.Cluster.PeersPort = ports[2]
	config.Log.Level = LogLevelError
	config.Otel.Endpoint = otlpEndpoint

	node, err := New(config, NewLogger(config.Log))
	require.NoError(t, err)
	require.NoError(t, node.Start(context.Background()))

	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := node.Stop(stopCtx); err != nil {
			t.Errorf("stop: %v", err)
		}
	})

	return node
}

// collectorSpanTraces reads the collector's logs and maps each exported span
// name to its trace ID. In the detailed exporter's output the "Trace ID" line
// precedes the "Name" line within a span block, so the most recent trace ID
// seen labels the next span name.
func collectorSpanTraces(t *testing.T, collector testcontainers.Container) map[string]string {
	t.Helper()

	reader, err := collector.Logs(context.Background())
	require.NoError(t, err)

	defer func() { _ = reader.Close() }()

	raw, err := io.ReadAll(reader)
	require.NoError(t, err)

	byName := map[string]string{}
	currentTrace := ""
	inSpan := false

	for line := range strings.SplitSeq(string(raw), "\n") {
		switch {
		case spanMarker.MatchString(line):
			inSpan = true

		case metricMarker.MatchString(line):
			inSpan = false
		}

		if match := traceIDLine.FindStringSubmatch(line); inSpan && match != nil {
			currentTrace = match[1]

			continue
		}

		if match := spanNameLine.FindStringSubmatch(line); inSpan && match != nil {
			byName[match[1]] = currentTrace
		}
	}

	return byName
}
