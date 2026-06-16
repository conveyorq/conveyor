// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestCORSAllowsListedOrigin verifies a listed origin is echoed and a preflight
// is answered, while an unlisted origin stays closed.
func TestCORSAllowsListedOrigin(t *testing.T) {
	const allowed = "https://ui.example.com"

	handler := withCORS([]string{allowed}, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// Listed origin: echoed on a normal request.
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/conveyor.v1.AdminService/ListQueues", nil)
	req.Header.Set("Origin", allowed)
	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, allowed, rec.Header().Get("Access-Control-Allow-Origin"))

	// Preflight: answered directly with no body.
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodOptions, "/conveyor.v1.AdminService/ListQueues", nil)
	req.Header.Set("Origin", allowed)
	handler.ServeHTTP(rec, req)

	require.Equal(t, http.StatusNoContent, rec.Code)
	require.Equal(t, allowed, rec.Header().Get("Access-Control-Allow-Origin"))

	// Unlisted origin: no allow header.
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/conveyor.v1.AdminService/ListQueues", nil)
	req.Header.Set("Origin", "https://evil.example.com")
	handler.ServeHTTP(rec, req)

	require.Empty(t, rec.Header().Get("Access-Control-Allow-Origin"))
}

// TestCORSDisabledByDefault verifies an empty origin list adds no CORS headers
// and passes requests straight through.
func TestCORSDisabledByDefault(t *testing.T) {
	called := false

	handler := withCORS(nil, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	}))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Origin", "https://ui.example.com")
	handler.ServeHTTP(rec, req)

	require.True(t, called)
	require.Empty(t, rec.Header().Get("Access-Control-Allow-Origin"))
}
