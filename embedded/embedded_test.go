// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package embedded

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	conveyor "github.com/conveyorq/conveyor/sdks/go"
	"github.com/conveyorq/conveyor/server"
)

// startTimeout bounds the boot of one embedded system in tests.
const startTimeout = 30 * time.Second

// processedTimeout bounds the wait for a task to complete end-to-end.
const processedTimeout = 10 * time.Second

// pollInterval is the cadence of completion polling.
const pollInterval = 50 * time.Millisecond

// orderPayload is the payload of the test task type.
type orderPayload struct {
	// OrderID identifies the order being processed.
	OrderID int `json:"order_id"`
}

func TestEmbeddedRoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), startTimeout)
	defer cancel()

	system, err := Start(ctx, Config{Broker: Memory()})
	require.NoError(t, err)

	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), startTimeout)
		defer stopCancel()

		require.NoError(t, system.Stop(stopCtx))
	})

	var processed atomic.Int64

	mux := conveyor.NewMux()

	mux.HandleFunc("order:process", func(_ context.Context, task *conveyor.Task) error {
		var payload orderPayload
		if err := task.Bind(&payload); err != nil {
			return conveyor.SkipRetry(err)
		}

		processed.Add(1)

		return nil
	})

	worker := system.Worker(
		conveyor.WithQueues(map[string]int{"default": 1}),
		conveyor.WithConcurrency(4),
	)

	workerCtx, workerCancel := context.WithCancel(ctx)
	defer workerCancel()

	workerDone := make(chan error, 1)

	go func() {
		workerDone <- worker.Run(workerCtx, mux)
	}()

	info, err := system.Client().Enqueue(ctx,
		conveyor.NewTask("order:process", conveyor.JSON(orderPayload{OrderID: 42})),
		conveyor.Retention(time.Hour),
	)
	require.NoError(t, err)
	require.NotEmpty(t, info.ID)

	require.Eventually(t, func() bool {
		return processed.Load() == 1
	}, processedTimeout, pollInterval, "task was never processed")

	require.Eventually(t, func() bool {
		current, err := system.Client().GetTask(ctx, info.ID)

		return err == nil && current.State == conveyor.TaskStateCompleted
	}, processedTimeout, pollInterval, "task never reached the completed state")

	workerCancel()
	require.NoError(t, <-workerDone)
}

func TestEmbeddedAddrReportsListener(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), startTimeout)
	defer cancel()

	system, err := Start(ctx, Config{})
	require.NoError(t, err)

	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), startTimeout)
		defer stopCancel()

		require.NoError(t, system.Stop(stopCtx))
	})

	require.NotEmpty(t, system.Addr())
	require.Equal(t, "http://"+system.Addr(), system.baseURL)
}

func TestEmbeddedStartRejectsInvalidConfig(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), startTimeout)
	defer cancel()

	// A Postgres broker with an empty DSN fails server.New's config
	// validation before the node ever starts.
	system, err := Start(ctx, Config{Broker: Postgres("")})
	require.Error(t, err)
	require.Nil(t, system)
	require.ErrorContains(t, err, "embedded:")
}

func TestEmbeddedStartFailsWhenBrokerUnreachable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), startTimeout)
	defer cancel()

	// A syntactically valid DSN pointing at a closed port: config validation
	// passes, so the failure surfaces when the node pings the broker on Start.
	system, err := Start(ctx, Config{Broker: Postgres("postgres://postgres:postgres@127.0.0.1:1/postgres")})
	require.Error(t, err)
	require.Nil(t, system)
	require.ErrorContains(t, err, "embedded: starting node")
}

func TestEmbeddedZeroConfigDefaultsToMemory(t *testing.T) {
	serverConfig, err := buildServerConfig(Config{})
	require.NoError(t, err)
	require.Equal(t, server.BrokerMemory, serverConfig.Broker.Driver)
	require.Empty(t, serverConfig.Broker.DSN)
}

func TestEmbeddedPostgresBrokerConfig(t *testing.T) {
	serverConfig, err := buildServerConfig(Config{Broker: Postgres("postgres://localhost/conveyor")})
	require.NoError(t, err)
	require.Equal(t, server.BrokerPostgres, serverConfig.Broker.Driver)
	require.Equal(t, "postgres://localhost/conveyor", serverConfig.Broker.DSN)
}

func TestEmbeddedServerConfigStaysOnLoopback(t *testing.T) {
	serverConfig, err := buildServerConfig(Config{})
	require.NoError(t, err)
	require.Equal(t, loopbackAnyPort, serverConfig.API.Listen)
	require.True(t, serverConfig.AuthDisabled())
}

func TestEmbeddedWorkerPanicsOnInvalidOptions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), startTimeout)
	defer cancel()

	system, err := Start(ctx, Config{})
	require.NoError(t, err)

	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), startTimeout)
		defer stopCancel()

		require.NoError(t, system.Stop(stopCtx))
	})

	require.Panics(t, func() {
		system.Worker() // no queues declared
	})
}
