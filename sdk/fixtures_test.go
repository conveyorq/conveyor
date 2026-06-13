package conveyor

import (
	"context"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/conveyorq/conveyor/internal/dynaport"
	"github.com/conveyorq/conveyor/server"
)

// testLoopback is the host every test server binds to.
const testLoopback = "127.0.0.1"

// freeTestPorts reserves n distinct free loopback ports.
func freeTestPorts(t *testing.T, n int) []int {
	t.Helper()

	ports, err := dynaport.Get(n)
	require.NoError(t, err)

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

// droppingProxy forwards TCP between the worker and a backend and can cut
// every live connection on demand, simulating a network drop or a server
// going away mid-session.
type droppingProxy struct {
	// listener accepts worker connections.
	listener net.Listener
	// backend is the real server address.
	backend string
	// mutex guards conns.
	mutex sync.Mutex
	// conns are all live connections, worker- and backend-side.
	conns []net.Conn
}

// newDroppingProxy starts a proxy in front of backend on a free loopback
// port.
func newDroppingProxy(t *testing.T, backend string) *droppingProxy {
	t.Helper()

	listener, err := net.Listen("tcp", testLoopback+":0")
	require.NoError(t, err)

	proxy := &droppingProxy{listener: listener, backend: backend}

	go proxy.serve()

	t.Cleanup(func() {
		require.NoError(t, listener.Close())
		proxy.dropAll()
	})

	return proxy
}

// addr returns the address workers should connect to.
func (p *droppingProxy) addr() string {
	return p.listener.Addr().String()
}

// serve accepts worker connections and pipes them to the backend until
// the listener closes.
func (p *droppingProxy) serve() {
	for {
		workerConn, err := p.listener.Accept()
		if err != nil {
			return
		}

		backendConn, err := net.Dial("tcp", p.backend)
		if err != nil {
			_ = workerConn.Close()

			continue
		}

		p.mutex.Lock()
		p.conns = append(p.conns, workerConn, backendConn)
		p.mutex.Unlock()

		go pipeConn(workerConn, backendConn)
		go pipeConn(backendConn, workerConn)
	}
}

// pipeConn copies one direction and closes both ends when it stops.
func pipeConn(dst, src net.Conn) {
	_, _ = io.Copy(dst, src)
	_ = dst.Close()
	_ = src.Close()
}

// dropAll severs every live connection.
func (p *droppingProxy) dropAll() {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	for _, conn := range p.conns {
		_ = conn.Close()
	}

	p.conns = nil
}

// awaitTaskState polls GetTask until the task reaches the wanted state.
func awaitTaskState(t *testing.T, client *Client, id string, want TaskState) {
	t.Helper()

	require.Eventuallyf(t, func() bool {
		info, err := client.GetTask(context.Background(), id)

		return err == nil && info.State == want
	}, 30*time.Second, 25*time.Millisecond, "task %s should reach state %s", id, want)
}
