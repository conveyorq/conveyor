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
