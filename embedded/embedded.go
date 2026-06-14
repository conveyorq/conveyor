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

// Package embedded runs a complete Conveyor node inside the host process
// and hands back regular SDK clients and workers wired to it over the
// loopback interface. User code is identical to the remote deployment;
// moving to a real cluster is swapping Start for conveyor.NewClient and
// conveyor.NewWorker with a URL.
package embedded

import (
	"context"
	"errors"
	"fmt"

	"github.com/conveyorq/conveyor/internal/dynaport"
	conveyor "github.com/conveyorq/conveyor/sdk"
	"github.com/conveyorq/conveyor/server"
)

// loopbackAnyPort binds a loopback listener on an OS-assigned free port,
// keeping the embedded node invisible to the network.
const loopbackAnyPort = "127.0.0.1:0"

// clusterPortCount is how many loopback ports the embedded engine needs:
// remoting, discovery, and peers.
const clusterPortCount = 3

// Broker selects the durable task log of an embedded system. Build one
// with Memory or Postgres; the zero value selects the in-memory broker.
type Broker struct {
	// driver is the broker driver name; empty means memory.
	driver string
	// dsn is the database connection string (postgres only).
	dsn string
}

// Memory selects the in-memory broker: zero infrastructure, no durability
// across restarts. The right choice for tests and single-process apps that
// can afford to lose queued work on crash.
func Memory() Broker {
	return Broker{driver: server.BrokerMemory}
}

// Postgres selects the Postgres broker at dsn, giving the embedded system
// the same durability as a remote deployment.
func Postgres(dsn string) Broker {
	return Broker{driver: server.BrokerPostgres, dsn: dsn}
}

// Config configures an embedded system.
type Config struct {
	// Broker selects the durable task log; the zero value is Memory().
	Broker Broker
}

// System is one in-process Conveyor node. Build SDK handles with Client
// and Worker, and shut the node down with Stop when done.
type System struct {
	// node is the in-process server.
	node *server.Server
	// client is the SDK client bound to the node.
	client *conveyor.Client
	// baseURL is the node's loopback API base URL.
	baseURL string
}

// Start boots a Conveyor node in-process: broker, engine, and an API
// listener on a free loopback port. The node serves until Stop.
func Start(ctx context.Context, config Config) (*System, error) {
	serverConfig, err := buildServerConfig(config)
	if err != nil {
		return nil, err
	}

	node, err := server.New(serverConfig, server.NewLogger(serverConfig.Log))
	if err != nil {
		return nil, fmt.Errorf("embedded: %w", err)
	}

	if err := node.Start(ctx); err != nil {
		return nil, fmt.Errorf("embedded: starting node: %w", err)
	}

	baseURL := "http://" + node.Addr()

	client, err := conveyor.NewClient(baseURL)
	if err != nil {
		stopCtx, cancel := context.WithTimeout(context.Background(), serverConfig.Engine.ShutdownTimeout)
		defer cancel()

		return nil, errors.Join(err, node.Stop(stopCtx))
	}

	return &System{node: node, client: client, baseURL: baseURL}, nil
}

// Client returns the SDK client bound to this system.
func (s *System) Client() *conveyor.Client {
	return s.client
}

// Worker builds an SDK worker bound to this system. Invalid options panic:
// embedded wiring is startup code, where failing fast beats failing on
// first dispatch, mirroring Mux.HandleFunc.
func (s *System) Worker(opts ...conveyor.Option) *conveyor.Worker {
	worker, err := conveyor.NewWorker(s.baseURL, opts...)
	if err != nil {
		panic(err)
	}

	return worker
}

// Addr returns the loopback address the embedded API listens on, e.g. to
// point the conveyor CLI at the running system.
func (s *System) Addr() string {
	return s.node.Addr()
}

// Stop gracefully shuts the embedded node down, honoring the context
// deadline: in-flight work is released for redelivery after restart (the
// memory broker loses it instead).
func (s *System) Stop(ctx context.Context) error {
	return s.node.Stop(ctx)
}

// buildServerConfig maps an embedded Config to a full node configuration:
// loopback-only listeners on free ports, auth off, quiet logs.
func buildServerConfig(config Config) (*server.Config, error) {
	ports, err := dynaport.Get(clusterPortCount)
	if err != nil {
		return nil, fmt.Errorf("embedded: reserving cluster ports: %w", err)
	}

	driver := config.Broker.driver
	if driver == "" {
		driver = server.BrokerMemory
	}

	serverConfig := server.DefaultConfig()
	serverConfig.Broker = server.BrokerConfig{Driver: driver, DSN: config.Broker.dsn}
	serverConfig.API.Listen = loopbackAnyPort
	serverConfig.Cluster.RemotingPort = ports[0]
	serverConfig.Cluster.DiscoveryPort = ports[1]
	serverConfig.Cluster.PeersPort = ports[2]
	serverConfig.Log = server.LogConfig{Level: server.LogLevelWarn, Format: server.LogFormatText}

	return serverConfig, nil
}
