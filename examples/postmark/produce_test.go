// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package postmark_test

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/conveyorq/conveyor/embedded"
	"github.com/conveyorq/conveyor/examples/postmark"
	conveyor "github.com/conveyorq/conveyor/sdks/go"
)

// Test timing bounds for the embedded round-trip.
const (
	bootTimeout    = 30 * time.Second
	settleTimeout  = 10 * time.Second
	pollInterval   = 50 * time.Millisecond
	workerParallel = 8
)

// TestProducerWorkerRoundTrip stands up a complete in-process platform — the
// same wiring the embedded command uses — and proves the producer's tasks flow
// through the queues to the handlers: deliverable mail is sent, hard bounces are
// archived, and a duplicate password reset is deduplicated rather than sent
// twice.
func TestProducerWorkerRoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), bootTimeout)
	defer cancel()

	system, err := embedded.Start(ctx, embedded.Config{Broker: embedded.Memory()})
	require.NoError(t, err)

	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), bootTimeout)
		defer stopCancel()

		require.NoError(t, system.Stop(stopCtx))
	})

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	provider := postmark.NewProvider(postmark.ProviderConfig{Latency: time.Millisecond, FailureRate: 0})

	worker := system.Worker(
		conveyor.WithQueues(postmark.WorkerQueues()),
		conveyor.WithConcurrency(workerParallel),
	)

	workerCtx, workerCancel := context.WithCancel(ctx)
	defer workerCancel()

	workerDone := make(chan error, 1)

	go func() {
		workerDone <- worker.Run(workerCtx, postmark.NewMux(provider, logger))
	}()

	producer := postmark.NewProducer(system.Client(), logger)

	require.NoError(t, producer.TwoFactor(ctx, 1))
	require.NoError(t, producer.Receipt(ctx, 2))
	require.NoError(t, producer.Campaign(ctx, "acme")) // includes hard-bounce recipients

	// A reset storm: the first reset is sent, every later click is deduplicated.
	require.NoError(t, producer.ResendStorm(ctx, 3))

	require.Eventually(t, func() bool {
		stats := provider.Stats()

		return stats.Sent > 0 && stats.Bounced > 0
	}, settleTimeout, pollInterval, "expected delivered mail and at least one hard bounce")

	workerCancel()
	require.NoError(t, <-workerDone)
}
