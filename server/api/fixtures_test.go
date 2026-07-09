// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	"github.com/tochemey/goakt/v4/discovery/static"

	"github.com/conveyorq/conveyor/internal/actors"
	"github.com/conveyorq/conveyor/internal/broker"
	"github.com/conveyorq/conveyor/internal/broker/memory"
	"github.com/conveyorq/conveyor/internal/clock"
	"github.com/conveyorq/conveyor/internal/dynaport"
	"github.com/conveyorq/conveyor/internal/proto/conveyor/v1/conveyorv1connect"
)

// testLoopback is the host every test component binds to.
const testLoopback = "127.0.0.1"

// testDefaultMaxRetry is the configured default retry budget in tests.
const testDefaultMaxRetry = int32(25)

// freeTestPorts reserves n distinct free loopback ports.
func freeTestPorts(t *testing.T, n int) []int {
	t.Helper()

	ports, err := dynaport.Get(n)
	require.NoError(t, err)

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
	workerService := NewWorkerService(engine, slog.New(slog.DiscardHandler), clock.System())
	mux.Handle(conveyorv1connect.NewWorkerServiceHandler(workerService, options...))
	mux.Handle(conveyorv1connect.NewAdminServiceHandler(
		NewAdminService(engine, taskLog, clock.System(), workerService, true), options...))
	// Lease-token authenticated, mounted without the bearer interceptor,
	// mirroring production.
	mux.Handle(conveyorv1connect.NewWebhookServiceHandler(NewWebhookService(engine, taskLog)))

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
