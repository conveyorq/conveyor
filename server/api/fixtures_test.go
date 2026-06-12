package api

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	"github.com/tochemey/goakt/v4/discovery/static"

	"github.com/tochemey/conveyor/internal/actors"
	"github.com/tochemey/conveyor/internal/broker"
	"github.com/tochemey/conveyor/internal/broker/memory"
	"github.com/tochemey/conveyor/internal/clock"
	"github.com/tochemey/conveyor/internal/proto/conveyor/v1/conveyorv1connect"
)

// testLoopback is the host every test component binds to.
const testLoopback = "127.0.0.1"

// testDefaultMaxRetry is the configured default retry budget in tests.
const testDefaultMaxRetry = int32(25)

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

// startTestEngine boots an engine node with fast settings on a fresh
// memory broker.
func startTestEngine(t *testing.T) (*actors.Engine, broker.Broker) {
	t.Helper()

	taskLog := memory.New(clock.System())
	ports := freeTestPorts(t, 3)
	self := fmt.Sprintf("%s:%d", testLoopback, ports[1])

	engine := actors.NewEngine(taskLog, clock.System(), slog.New(slog.DiscardHandler), actors.Config{
		Name:          "conveyor-api-test",
		BindAddr:      testLoopback,
		RemotingPort:  ports[0],
		DiscoveryPort: ports[1],
		PeersPort:     ports[2],
		Provider:      static.NewDiscovery(&static.Config{Hosts: []string{self}}),
		Settings: actors.Settings{
			LeaseTTL:        2 * time.Second,
			LeaseBatchMax:   100,
			ReapInterval:    200 * time.Millisecond,
			PromoteInterval: 100 * time.Millisecond,
			PassivateAfter:  5 * time.Minute,
		},
	})

	require.NoError(t, engine.Start(context.Background()))

	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = engine.Stop(stopCtx)
	})

	return engine, taskLog
}

// startAPIServer serves the ConnectRPC services over unencrypted HTTP/2,
// mirroring the production mux assembly, and returns the base URL.
func startAPIServer(t *testing.T, engine *actors.Engine, taskLog broker.Broker, tokens []string) string {
	t.Helper()

	var options []connect.HandlerOption
	if len(tokens) > 0 {
		options = append(options, connect.WithInterceptors(NewAuthInterceptor(tokens)))
	}

	mux := http.NewServeMux()
	mux.Handle(conveyorv1connect.NewTaskServiceHandler(
		NewTaskService(engine, taskLog, clock.System(), testDefaultMaxRetry), options...))
	mux.Handle(conveyorv1connect.NewWorkerServiceHandler(
		NewWorkerService(engine, slog.New(slog.DiscardHandler)), options...))

	server := httptest.NewUnstartedServer(mux)

	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	protocols.SetHTTP2(true)
	protocols.SetUnencryptedHTTP2(true)
	server.Config.Protocols = protocols

	server.Start()
	t.Cleanup(server.Close)

	return server.URL
}

// h2cHTTPClient builds an HTTP client speaking unencrypted HTTP/2, which
// the session stream requires.
func h2cHTTPClient() *http.Client {
	protocols := new(http.Protocols)
	protocols.SetHTTP2(true)
	protocols.SetUnencryptedHTTP2(true)

	return &http.Client{Transport: &http.Transport{Protocols: protocols}}
}
