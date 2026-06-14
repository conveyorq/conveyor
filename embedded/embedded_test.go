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

package embedded

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	conveyor "github.com/conveyorq/conveyor/sdk"
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
