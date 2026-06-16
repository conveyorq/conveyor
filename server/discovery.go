// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"fmt"
	"log/slog"
	"sync"

	"github.com/tochemey/goakt/v4/discovery"
)

// DiscoveryProvider is a cluster peer-discovery source. It mirrors the
// methods conveyord's cluster engine needs without exposing any engine type,
// so a custom provider is written against this package alone. Built-in
// providers (static, kubernetes) and user providers registered through
// RegisterDiscovery are selected identically by the cluster.discovery key.
type DiscoveryProvider interface {
	// ID returns the provider's stable name.
	ID() string
	// Initialize prepares the provider; called once before discovery starts.
	Initialize() error
	// Register advertises this node to the discovery backend.
	Register() error
	// Deregister withdraws this node from the discovery backend.
	Deregister() error
	// DiscoverPeers returns the current peer addresses as host:port strings
	// pointing at each peer's gossip (discovery) port.
	DiscoverPeers() ([]string, error)
	// Close releases any resources the provider holds.
	Close() error
}

// DiscoveryConfig is handed to a provider factory at boot. Options carries the
// cluster.options map verbatim from configuration, so a provider reads its own
// settings by key; Logger is the node's process logger.
type DiscoveryConfig struct {
	// Options is the cluster.options map from configuration.
	Options map[string]string
	// Logger is the node's process logger.
	Logger *slog.Logger
}

// DiscoveryFactory builds a provider from its configuration.
type DiscoveryFactory func(DiscoveryConfig) (DiscoveryProvider, error)

// discoveryRegistry holds the custom providers registered by name.
var discoveryRegistry = struct {
	mu        sync.RWMutex
	factories map[string]DiscoveryFactory
}{factories: map[string]DiscoveryFactory{}}

// RegisterDiscovery makes a provider selectable by name via the
// cluster.discovery configuration key. Call it from main before constructing
// the server (the recompile path: build your own conveyord that imports this
// package and registers the provider). It panics on an empty name, a nil
// factory, a name that collides with a built-in, or a duplicate registration,
// because all of these are programming errors discoverable at startup.
func RegisterDiscovery(name string, factory DiscoveryFactory) {
	if name == "" {
		panic("conveyor: RegisterDiscovery called with an empty name")
	}

	if factory == nil {
		panic(fmt.Sprintf("conveyor: RegisterDiscovery %q called with a nil factory", name))
	}

	if isBuiltinDiscovery(name) {
		panic(fmt.Sprintf("conveyor: RegisterDiscovery %q collides with a built-in provider", name))
	}

	discoveryRegistry.mu.Lock()
	defer discoveryRegistry.mu.Unlock()

	if _, exists := discoveryRegistry.factories[name]; exists {
		panic(fmt.Sprintf("conveyor: discovery provider %q is already registered", name))
	}

	discoveryRegistry.factories[name] = factory
}

// lookupDiscovery returns the registered factory for name, if any.
func lookupDiscovery(name string) (DiscoveryFactory, bool) {
	discoveryRegistry.mu.RLock()
	defer discoveryRegistry.mu.RUnlock()

	factory, ok := discoveryRegistry.factories[name]

	return factory, ok
}

// isBuiltinDiscovery reports whether name is one of the providers conveyord
// wires directly, which custom registrations may not shadow.
func isBuiltinDiscovery(name string) bool {
	switch name {
	case DiscoveryStatic, DiscoveryKubernetes, DiscoveryNATS, DiscoveryConsul,
		DiscoveryEtcd, DiscoveryMDNS, DiscoveryDNSSD:
		return true

	default:
		return false
	}
}

// discoveryAdapter implements GoAkt's discovery.Provider over the public
// DiscoveryProvider, so the cluster engine stays swappable: a change to
// GoAkt's interface is absorbed here, never in a user's provider.
type discoveryAdapter struct {
	provider DiscoveryProvider
}

// ID returns the wrapped provider's name.
func (a *discoveryAdapter) ID() string { return a.provider.ID() }

// Initialize prepares the wrapped provider.
func (a *discoveryAdapter) Initialize() error { return a.provider.Initialize() }

// Register advertises this node via the wrapped provider.
func (a *discoveryAdapter) Register() error { return a.provider.Register() }

// Deregister withdraws this node via the wrapped provider.
func (a *discoveryAdapter) Deregister() error { return a.provider.Deregister() }

// DiscoverPeers returns the wrapped provider's peers.
func (a *discoveryAdapter) DiscoverPeers() ([]string, error) { return a.provider.DiscoverPeers() }

// Close releases the wrapped provider.
func (a *discoveryAdapter) Close() error { return a.provider.Close() }

// buildCustomDiscovery instantiates a registered provider by name and adapts
// it to GoAkt's discovery.Provider.
func (s *Server) buildCustomDiscovery(name string) (discovery.Provider, error) {
	factory, ok := lookupDiscovery(name)
	if !ok {
		return nil, fmt.Errorf("cluster.discovery: provider %q is not wired and no custom provider is registered under that name", name)
	}

	provider, err := factory(DiscoveryConfig{Options: s.config.Cluster.Options, Logger: s.logger})
	if err != nil {
		return nil, fmt.Errorf("cluster.discovery: building custom provider %q: %w", name, err)
	}

	return &discoveryAdapter{provider: provider}, nil
}
