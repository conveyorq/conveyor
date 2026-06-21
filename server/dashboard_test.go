// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"io"
	"io/fs"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/conveyorq/conveyor/web/dashboard"
)

// TestDashboardServedByDefault verifies a dev server serves the embedded SPA
// shell at the API root with no extra configuration.
func TestDashboardServedByDefault(t *testing.T) {
	requireDashboardBuilt(t)

	node := startTestServer(t)

	resp, err := http.Get("http://" + node.Addr() + "/")
	require.NoError(t, err)

	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Contains(t, resp.Header.Get("Content-Type"), "text/html")
	require.Contains(t, string(body), `id="root"`)
}

// TestDashboardSecurityHeaders verifies the SPA shell carries the hardening
// headers and a no-cache policy so a redeploy is seen immediately.
func TestDashboardSecurityHeaders(t *testing.T) {
	requireDashboardBuilt(t)

	node := startTestServer(t)

	resp, err := http.Get("http://" + node.Addr() + "/")
	require.NoError(t, err)

	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, "nosniff", resp.Header.Get("X-Content-Type-Options"))
	require.Equal(t, "DENY", resp.Header.Get("X-Frame-Options"))
	require.Equal(t, "no-cache", resp.Header.Get("Cache-Control"))
}

// TestDashboardAssetsCachedImmutable verifies content-hashed assets are served
// with a long immutable cache policy so reloads hit the browser cache.
func TestDashboardAssetsCachedImmutable(t *testing.T) {
	requireDashboardBuilt(t)

	root, err := dashboard.Assets()
	require.NoError(t, err)

	var asset string

	require.NoError(t, fs.WalkDir(root, "assets", func(p string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}

		if !entry.IsDir() && asset == "" {
			asset = p
		}

		return nil
	}))

	require.NotEmpty(t, asset, "expected a built asset under assets/")

	node := startTestServer(t)

	resp, err := http.Get("http://" + node.Addr() + "/" + asset)
	require.NoError(t, err)

	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, assetCacheControl, resp.Header.Get("Cache-Control"))
}

// TestDashboardConfigNotCached verifies the runtime-config endpoint is never
// cached, so an operator setting change is reflected on the next load.
func TestDashboardConfigNotCached(t *testing.T) {
	node := startTestServer(t)

	resp, err := http.Get("http://" + node.Addr() + "/dashboard-config.json")
	require.NoError(t, err)

	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, "no-cache", resp.Header.Get("Cache-Control"))
	require.Equal(t, "nosniff", resp.Header.Get("X-Content-Type-Options"))
}

// TestDashboardDisabled verifies that with api.dashboard off the root path is
// not served, leaving the API reachable without the UI.
func TestDashboardDisabled(t *testing.T) {
	node := startServerWithConfig(t, func(c *Config) { c.API.Dashboard = false })

	resp, err := http.Get("http://" + node.Addr() + "/")
	require.NoError(t, err)

	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusNotFound, resp.StatusCode)
}

// TestDashboardConfigEndpoint verifies the SPA runtime-config endpoint returns
// the configured Grafana URL as JSON.
func TestDashboardConfigEndpoint(t *testing.T) {
	node := startServerWithConfig(t, func(c *Config) { c.API.GrafanaURL = "https://grafana.example.com/d/abc" })

	resp, err := http.Get("http://" + node.Addr() + "/dashboard-config.json")
	require.NoError(t, err)

	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Contains(t, resp.Header.Get("Content-Type"), "application/json")
	require.Contains(t, string(body), `"grafanaUrl":"https://grafana.example.com/d/abc"`)
	require.Contains(t, string(body), `"readOnly":false`)
}

// TestDashboardConfigReportsReadOnly verifies the runtime-config endpoint
// reflects the configured read-only mode.
func TestDashboardConfigReportsReadOnly(t *testing.T) {
	node := startServerWithConfig(t, func(c *Config) { c.API.ReadOnly = true })

	resp, err := http.Get("http://" + node.Addr() + "/dashboard-config.json")
	require.NoError(t, err)

	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	require.Contains(t, string(body), `"readOnly":true`)
}

// startServerWithConfig boots a dev-config server with a mutator applied,
// bound to ephemeral ports, stopped with the test.
func startServerWithConfig(t *testing.T, mutate func(*Config)) *Server {
	t.Helper()

	ports := freePorts(t, 3)

	config := DevConfig()
	config.API.Listen = "127.0.0.1:0"
	config.Metrics.Listen = "127.0.0.1:0"
	config.Cluster.RemotingPort = ports[0]
	config.Cluster.DiscoveryPort = ports[1]
	config.Cluster.PeersPort = ports[2]
	config.Log.Level = LogLevelError

	mutate(config)

	node, err := New(config, NewLogger(config.Log))
	require.NoError(t, err)
	require.NoError(t, node.Start(context.Background()))

	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := node.Stop(stopCtx); err != nil {
			t.Errorf("stop: %v", err)
		}
	})

	return node
}

// requireDashboardBuilt skips when the SPA bundle has not been built (the repo
// ships only a .gitkeep; dist/ is built in CI and the image).
func requireDashboardBuilt(t *testing.T) {
	t.Helper()

	root, err := dashboard.Assets()
	require.NoError(t, err)

	if _, statErr := fs.Stat(root, "index.html"); statErr != nil {
		t.Skip("dashboard bundle not built; run `make dashboard`")
	}
}
