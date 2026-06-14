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

package server

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// staticTestProvider is a DiscoveryProvider that returns a fixed peer set,
// used to exercise the public SPI without any external discovery backend.
type staticTestProvider struct {
	id    string
	peers []string
}

func (p *staticTestProvider) ID() string                       { return p.id }
func (p *staticTestProvider) Initialize() error                { return nil }
func (p *staticTestProvider) Register() error                  { return nil }
func (p *staticTestProvider) Deregister() error                { return nil }
func (p *staticTestProvider) DiscoverPeers() ([]string, error) { return p.peers, nil }
func (p *staticTestProvider) Close() error                     { return nil }

// registerForTest registers a provider and unregisters it on cleanup, so the
// package-level registry does not leak across tests (and -count>1 is safe).
func registerForTest(t *testing.T, name string, factory DiscoveryFactory) {
	t.Helper()

	RegisterDiscovery(name, factory)

	t.Cleanup(func() {
		discoveryRegistry.mu.Lock()
		delete(discoveryRegistry.factories, name)
		discoveryRegistry.mu.Unlock()
	})
}

func TestRegisterDiscoveryAndLookup(t *testing.T) {
	factory := func(DiscoveryConfig) (DiscoveryProvider, error) {
		return &staticTestProvider{id: "lookup"}, nil
	}

	registerForTest(t, "spi-lookup", factory)

	got, ok := lookupDiscovery("spi-lookup")
	require.True(t, ok, "registered provider must be found")
	require.NotNil(t, got)

	_, ok = lookupDiscovery("never-registered")
	require.False(t, ok)
}

func TestRegisterDiscoveryRejectsMisuse(t *testing.T) {
	factory := func(DiscoveryConfig) (DiscoveryProvider, error) {
		return &staticTestProvider{}, nil
	}

	require.Panics(t, func() { RegisterDiscovery("", factory) }, "empty name")
	require.Panics(t, func() { RegisterDiscovery("nil-factory", nil) }, "nil factory")
	require.Panics(t, func() { RegisterDiscovery(DiscoveryStatic, factory) }, "built-in collision")

	registerForTest(t, "spi-dup", factory)
	require.Panics(t, func() { RegisterDiscovery("spi-dup", factory) }, "duplicate name")
}

func TestDiscoveryAdapterForwards(t *testing.T) {
	provider := &staticTestProvider{id: "adapt", peers: []string{"a:1", "b:2"}}
	adapter := &discoveryAdapter{provider: provider}

	require.Equal(t, "adapt", adapter.ID())
	require.NoError(t, adapter.Initialize())
	require.NoError(t, adapter.Register())
	require.NoError(t, adapter.Deregister())
	require.NoError(t, adapter.Close())

	peers, err := adapter.DiscoverPeers()
	require.NoError(t, err)
	require.Equal(t, []string{"a:1", "b:2"}, peers)
}

func TestCustomProviderUnknownNameFailsValidation(t *testing.T) {
	config := DevConfig()
	config.Mode = ModeCluster
	config.Cluster.Discovery = "not-registered"

	require.Error(t, config.Validate())
}

// TestCustomProviderFormsCluster is the Phase 5 SPI acceptance check: a
// provider registered only through the public RegisterDiscovery surface is
// selectable by cluster.discovery and forms a real two-node cluster.
func TestCustomProviderFormsCluster(t *testing.T) {
	if testing.Short() {
		t.Skip("cluster formation skipped in -short mode")
	}

	const providerName = "spi-cluster"

	portsA := freePorts(t, 3)
	portsB := freePorts(t, 3)

	// Both nodes advertise both gossip addresses; the provider reads them
	// from cluster.options, proving the Options plumbing end to end.
	peerOption := fmt.Sprintf("%s:%d,%s:%d", defaultBindAddr, portsA[1], defaultBindAddr, portsB[1])

	registerForTest(t, providerName, func(config DiscoveryConfig) (DiscoveryProvider, error) {
		raw := config.Options["peers"]
		if raw == "" {
			return nil, fmt.Errorf("peers option is required")
		}

		return &staticTestProvider{id: providerName, peers: strings.Split(raw, ",")}, nil
	})

	nodeA := startClusterNode(t, providerName, peerOption, portsA)
	nodeB := startClusterNode(t, providerName, peerOption, portsB)

	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		peers, err := nodeA.engine.System().Peers(ctx, 2*time.Second)

		return err == nil && len(peers) == 1
	}, 30*time.Second, 250*time.Millisecond, "nodes must discover each other through the custom provider")

	// Sanity: the second node sees the cluster too.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	peers, err := nodeB.engine.System().Peers(ctx, 2*time.Second)
	require.NoError(t, err)
	require.Len(t, peers, 1)
}

// startClusterNode boots an in-process cluster node using the named custom
// discovery provider.
func startClusterNode(t *testing.T, providerName, peerOption string, ports []int) *Server {
	t.Helper()

	config := DevConfig()
	config.Mode = ModeCluster
	config.API.Listen = "127.0.0.1:0"
	config.Metrics.Listen = "127.0.0.1:0"
	config.Cluster.Discovery = providerName
	config.Cluster.BindAddr = defaultBindAddr
	config.Cluster.RemotingPort = ports[0]
	config.Cluster.DiscoveryPort = ports[1]
	config.Cluster.PeersPort = ports[2]
	config.Cluster.Options = map[string]string{"peers": peerOption}

	node, err := New(config, NewLogger(LogConfig{Level: LogLevelError, Format: LogFormatText}))
	require.NoError(t, err)
	require.NoError(t, node.Start(context.Background()))

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		_ = node.Stop(ctx)
	})

	return node
}
