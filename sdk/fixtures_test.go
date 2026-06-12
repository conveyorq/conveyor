package conveyor

import (
	"context"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tochemey/conveyor/server"
)

// testLoopback is the host every test server binds to.
const testLoopback = "127.0.0.1"

// freeTestPorts reserves n distinct free TCP ports.
func freeTestPorts(t *testing.T, n int) []int {
	t.Helper()

	ports := make([]int, 0, n)
	listeners := make([]net.Listener, 0, n)

	for range n {
		listener, err := net.Listen("tcp", testLoopback+":0")
		require.NoError(t, err)

		listeners = append(listeners, listener)
		ports = append(ports, listener.Addr().(*net.TCPAddr).Port)
	}

	for _, listener := range listeners {
		require.NoError(t, listener.Close())
	}

	return ports
}

// startTestServer boots a dev-mode conveyord node on ephemeral ports with
// fast recovery settings and returns its base URL.
func startTestServer(t *testing.T, tokens []string) string {
	t.Helper()

	ports := freeTestPorts(t, 3)

	config := server.DevConfig()
	config.API.Listen = testLoopback + ":0"
	config.API.AuthTokens = tokens
	config.Cluster.RemotingPort = ports[0]
	config.Cluster.DiscoveryPort = ports[1]
	config.Cluster.PeersPort = ports[2]
	config.Engine.LeaseTTL = 2 * time.Second
	config.Engine.ReapInterval = 200 * time.Millisecond
	config.Engine.PromoteInterval = 100 * time.Millisecond

	node, err := server.New(config, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	require.NoError(t, node.Start(context.Background()))

	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = node.Stop(stopCtx)
	})

	return "http://" + node.Addr()
}

// awaitTaskState polls GetTask until the task reaches the wanted state.
func awaitTaskState(t *testing.T, client *Client, id string, want TaskState) {
	t.Helper()

	require.Eventuallyf(t, func() bool {
		info, err := client.GetTask(context.Background(), id)

		return err == nil && info.State == want
	}, 30*time.Second, 25*time.Millisecond, "task %s should reach state %s", id, want)
}
