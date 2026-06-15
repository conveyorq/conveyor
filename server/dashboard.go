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
	"encoding/json"
	"net/http"

	"github.com/conveyorq/conveyor/web/dashboard"
)

// dashboardRoot is the mux pattern the embedded dashboard is served under. It
// is the catch-all: the Connect service paths and the health endpoints match
// more specifically and take precedence.
const dashboardRoot = "/"

// dashboardConfigPath serves the runtime config the SPA reads on load
// (operator settings that aren't baked into the bundle).
const dashboardConfigPath = "/dashboard-config.json"

// dashboardConfig is the runtime config delivered to the SPA.
type dashboardConfig struct {
	// GrafanaURL is the operator's Grafana link, or empty to hide it.
	GrafanaURL string `json:"grafanaUrl"`
}

// mountDashboard serves the embedded operations console at the API root when
// api.dashboard is enabled, plus the runtime-config endpoint the SPA reads.
// A handler build failure is logged and the route is left unmounted rather
// than failing server startup — the API stays up.
func (s *Server) mountDashboard(mux *http.ServeMux) {
	if !s.config.API.Dashboard {
		return
	}

	handler, err := dashboard.Handler()
	if err != nil {
		s.logger.Error("dashboard disabled: building handler failed", "error", err)

		return
	}

	mux.HandleFunc(dashboardConfigPath, s.serveDashboardConfig)
	mux.Handle(dashboardRoot, handler)
}

// serveDashboardConfig returns the SPA's runtime config as JSON.
func (s *Server) serveDashboardConfig(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(dashboardConfig{GrafanaURL: s.config.API.GrafanaURL})
}
