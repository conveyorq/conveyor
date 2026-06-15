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
