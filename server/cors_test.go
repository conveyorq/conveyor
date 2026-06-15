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
