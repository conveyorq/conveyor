// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResolveHealthAddr(t *testing.T) {
	// Flag wins over env and default.
	require.Equal(t, "127.0.0.1:9000", resolveHealthAddr("127.0.0.1:9000", "0.0.0.0:8080"))
	// Env is used when the flag is empty.
	require.Equal(t, "127.0.0.1:9090", resolveHealthAddr("", "127.0.0.1:9090"))
	// Nothing set falls back to the default API port on loopback.
	require.Equal(t, "127.0.0.1:8080", resolveHealthAddr("", ""))
	// A bind-all address is rewritten to loopback for the local probe.
	require.Equal(t, "127.0.0.1:8080", resolveHealthAddr(":8080", ""))
	require.Equal(t, "127.0.0.1:7000", resolveHealthAddr("0.0.0.0:7000", ""))
	require.Equal(t, "127.0.0.1:6000", resolveHealthAddr("[::]:6000", ""))
}

func TestRunHealthcheckHealthy(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/healthz", r.URL.Path)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	require.NoError(t, runHealthcheck(server.Listener.Addr().String()))
}

func TestRunHealthcheckUnhealthy(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	require.ErrorContains(t, runHealthcheck(server.Listener.Addr().String()), "status 503")
}

func TestRunHealthcheckConnectionRefused(t *testing.T) {
	// A server that is immediately closed leaves a port nothing listens on.
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	addr := server.Listener.Addr().String()
	server.Close()

	require.Error(t, runHealthcheck(addr))
}

func TestHealthcheckCommandWired(t *testing.T) {
	root := newRootCommand()

	var found bool
	for _, c := range root.Commands() {
		if c.Name() == "healthcheck" {
			found = true
		}
	}

	require.True(t, found, "healthcheck subcommand should be registered on the root command")
}
