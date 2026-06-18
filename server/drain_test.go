// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	"github.com/conveyorq/conveyor/internal/clock"
	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
	"github.com/conveyorq/conveyor/internal/proto/conveyor/v1/conveyorv1connect"
	"github.com/conveyorq/conveyor/internal/wire"
	conveyor "github.com/conveyorq/conveyor/sdks/go"
)

// drainStopBudget bounds Stop in the drain test. It is far below the
// default 60s lease TTL, so a prompt return proves the session drain
// chain fired rather than the node waiting out streams or leases.
const drainStopBudget = 20 * time.Second

// TestStopDrainsLiveWorkerSessions exercises the full coordinated
// shutdown chain — server.Stop → engine stop → shutdown hook →
// DrainSessions → stream close — with a live session holding an
// unresolved task. It pins the wiring an isolated DrainSessions test
// cannot: the drainer registration and the hook ordering.
func TestStopDrainsLiveWorkerSessions(t *testing.T) {
	ports := freePorts(t, 3)

	config := DevConfig()
	config.API.Listen = "127.0.0.1:0"
	config.Metrics.Listen = "127.0.0.1:0"
	config.Cluster.RemotingPort = ports[0]
	config.Cluster.DiscoveryPort = ports[1]
	config.Cluster.PeersPort = ports[2]

	node, err := New(config, NewLogger(LogConfig{Level: LogLevelError, Format: LogFormatText}))
	require.NoError(t, err)
	require.NoError(t, node.Start(context.Background()))

	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), drainStopBudget)
		defer cancel()
		_ = node.Stop(stopCtx)
	})

	ctx := context.Background()
	baseURL := "http://" + node.Addr()

	// A raw session stands in for a worker mid-handler: it accepts one
	// dispatch and never reports a result.
	stream := conveyorv1connect.NewWorkerServiceClient(wire.NewH2CClient(), baseURL).Session(ctx)

	t.Cleanup(func() {
		_ = stream.CloseRequest()
		_ = stream.CloseResponse()
	})

	hello := &conveyorv1.WorkerMessage{
		Frame: &conveyorv1.WorkerMessage_Hello{
			Hello: &conveyorv1.Hello{Queues: map[string]int32{"default": 1}, Concurrency: 1},
		},
	}
	require.NoError(t, stream.Send(hello))

	welcome, err := stream.Receive()
	require.NoError(t, err)
	require.NotNil(t, welcome.GetWelcome())

	client, err := conveyor.NewClient(baseURL)
	require.NoError(t, err)

	_, err = client.Enqueue(ctx, conveyor.NewTask("email:welcome", conveyor.Bytes(nil)))
	require.NoError(t, err)

	dispatched, err := stream.Receive()
	require.NoError(t, err)
	require.NotNil(t, dispatched.GetDispatch())

	// Stop with the session still holding the task. Without the drain
	// chain, http.Shutdown blocks on the live stream until the context
	// deadline; with it, the session ends and Stop returns promptly. The
	// bound sits below the gateway's 10s drain-request timeout, so a
	// drain that silently degrades into that timeout also fails here.
	stopCtx, cancel := context.WithTimeout(context.Background(), drainStopBudget)
	defer cancel()

	timeSource := clock.System()
	started := timeSource.Now()
	require.NoError(t, node.Stop(stopCtx))
	require.Less(t, timeSource.Now().Sub(started), 8*time.Second,
		"Stop must return through the drain chain, not by exhausting timeouts")

	// The worker observes its stream ending.
	_, err = stream.Receive()
	require.Error(t, err, "drained session stream must be closed")
	require.NotEqual(t, connect.CodeDeadlineExceeded, connect.CodeOf(err))
}
