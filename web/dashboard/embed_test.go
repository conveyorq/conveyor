// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package dashboard_test

import (
	"io/fs"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/conveyorq/conveyor/web/dashboard"
)

// requireBuilt skips a test when the SPA bundle has not been built yet (the
// committed tree carries only a .gitkeep; dist/ is built in CI and the image).
func requireBuilt(t *testing.T) {
	t.Helper()

	root, err := dashboard.Assets()
	require.NoError(t, err)

	if _, statErr := fs.Stat(root, indexFile); statErr != nil {
		t.Skip("dashboard bundle not built; run `make dashboard`")
	}
}

// indexFile is the SPA entrypoint the build produces.
const indexFile = "index.html"

// TestHandlerServesIndex verifies the root path returns the SPA shell as HTML.
func TestHandlerServesIndex(t *testing.T) {
	requireBuilt(t)

	handler, err := dashboard.Handler()
	require.NoError(t, err)

	resp := serve(t, handler, "/")

	require.Equal(t, http.StatusOK, resp.Code)
	require.Contains(t, resp.Header().Get("Content-Type"), "text/html")
	require.Contains(t, resp.Body.String(), `id="root"`)
}

// TestHandlerFallsBackToIndex verifies an unknown (client-routed) path serves
// the SPA shell rather than 404, so the app's own router handles it.
func TestHandlerFallsBackToIndex(t *testing.T) {
	requireBuilt(t)

	handler, err := dashboard.Handler()
	require.NoError(t, err)

	resp := serve(t, handler, "/queues/critical")

	require.Equal(t, http.StatusOK, resp.Code)
	require.Contains(t, resp.Body.String(), `id="root"`)
}

// TestHandlerServesBuiltAsset verifies a real built asset is served with a
// non-HTML content type, proving the embedded dist/ tree is wired through.
func TestHandlerServesBuiltAsset(t *testing.T) {
	requireBuilt(t)

	root, err := dashboard.Assets()
	require.NoError(t, err)

	asset := firstAsset(t, root)

	handler, err := dashboard.Handler()
	require.NoError(t, err)

	resp := serve(t, handler, "/"+asset)

	require.Equal(t, http.StatusOK, resp.Code)
	require.NotContains(t, resp.Header().Get("Content-Type"), "text/html")
}

// serve runs one GET request against the handler and returns the recorder.
func serve(t *testing.T, handler http.Handler, path string) *httptest.ResponseRecorder {
	t.Helper()

	request := httptest.NewRequest(http.MethodGet, path, nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	return recorder
}

// firstAsset returns the path of the first file under assets/, which Vite
// always populates with the hashed JS/CSS bundles.
func firstAsset(t *testing.T, root fs.FS) string {
	t.Helper()

	var found string

	require.NoError(t, fs.WalkDir(root, "assets", func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if !entry.IsDir() && found == "" {
			found = path
		}

		return nil
	}))

	require.NotEmpty(t, found, "expected at least one built asset under assets/")
	require.True(t, strings.HasPrefix(found, "assets/"))

	return found
}
