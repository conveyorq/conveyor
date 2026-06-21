// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/conveyorq/conveyor/web/dashboard"
)

// dashboardRoot is the mux pattern the embedded dashboard is served under. It
// is the catch-all: the Connect service paths and the health endpoints match
// more specifically and take precedence.
const dashboardRoot = "/"

// dashboardConfigPath serves the runtime config the SPA reads on load
// (operator settings that aren't baked into the bundle).
const dashboardConfigPath = "/dashboard-config.json"

// dashboardAssetsPrefix is the request-path prefix Vite emits for its
// content-hashed JS/CSS bundles. Those names change on every build, so they are
// safe to cache forever; the entrypoint HTML must never be.
const dashboardAssetsPrefix = "/assets/"

// assetCacheControl caches content-hashed assets for a year. Their names carry
// a content hash, so a new build is fetched under a new name, never stale.
const assetCacheControl = "public, max-age=31536000, immutable"

// htmlCacheControl forces the SPA entrypoint and runtime config to revalidate on
// every load, so a redeploy is picked up immediately.
const htmlCacheControl = "no-cache"

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
	mux.Handle(dashboardRoot, withDashboardHeaders(handler))
}

// withDashboardHeaders adds defensive response headers to the static console
// and the caching policy its content-hashed assets allow: hashed bundles under
// /assets/ are immutable and cached for a year, while the entrypoint HTML
// revalidates every load so a redeploy is seen at once. The security headers
// block MIME sniffing, framing (clickjacking), and referrer leakage; the shell
// holds no secrets, but the bearer token lives in the page, so locking it down
// costs nothing and removes an attack surface.
func withDashboardHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		header := w.Header()
		header.Set("X-Content-Type-Options", "nosniff")
		header.Set("X-Frame-Options", "DENY")
		header.Set("Referrer-Policy", "no-referrer")

		if strings.HasPrefix(r.URL.Path, dashboardAssetsPrefix) {
			header.Set("Cache-Control", assetCacheControl)
		} else {
			header.Set("Cache-Control", htmlCacheControl)
		}

		next.ServeHTTP(w, r)
	})
}

// serveDashboardConfig returns the SPA's runtime config as JSON. It is never
// cached, so a read-only or Grafana-link change is reflected on the next load.
func (s *Server) serveDashboardConfig(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", htmlCacheControl)
	w.Header().Set("X-Content-Type-Options", "nosniff")
	_ = json.NewEncoder(w).Encode(dashboardConfig{
		GrafanaURL: s.config.API.GrafanaURL,
		ReadOnly:   s.config.API.ReadOnly,
	})
}
