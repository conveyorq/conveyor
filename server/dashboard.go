// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

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
	// ReadOnly mirrors the admin read-only mode so the SPA hides its action
	// controls; the server enforces it regardless.
	ReadOnly bool `json:"readOnly"`
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
	_ = json.NewEncoder(w).Encode(dashboardConfig{
		GrafanaURL: s.config.API.GrafanaURL,
		ReadOnly:   s.config.API.ReadOnly,
	})
}
