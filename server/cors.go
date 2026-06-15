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
	"slices"
	"strings"
)

// corsWildcard, when present in the allowed list, permits any origin.
const corsWildcard = "*"

// corsAllowHeaders are the request headers a browser may send on a
// cross-origin API call: bearer auth plus the Connect protocol headers.
const corsAllowHeaders = "Authorization, Content-Type, Connect-Protocol-Version, Connect-Timeout-Ms"

// corsAllowMethods are the methods the API accepts cross-origin. Connect
// unary and streaming calls are POST; GET covers the dashboard assets.
const corsAllowMethods = "GET, POST, OPTIONS"

// corsMaxAge caches a successful preflight for a day.
const corsMaxAge = "86400"

// withCORS wraps next with cross-origin support for the configured origins,
// so a dashboard hosted on a different origin can call the API from a browser.
// An empty list returns next unchanged: CORS stays off by default, and no
// Access-Control headers are emitted. An allowed cross-origin request echoes
// its origin (never a blanket wildcard alongside credentials), and a preflight
// OPTIONS is answered directly.
func withCORS(origins []string, next http.Handler) http.Handler {
	if len(origins) == 0 {
		return next
	}

	allowAny := slices.Contains(origins, corsWildcard)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")

		if origin != "" && (allowAny || slices.Contains(origins, origin)) {
			header := w.Header()
			header.Set("Access-Control-Allow-Origin", origin)
			header.Add("Vary", "Origin")
			header.Set("Access-Control-Allow-Methods", corsAllowMethods)
			header.Set("Access-Control-Allow-Headers", corsAllowHeaders)
			header.Set("Access-Control-Max-Age", corsMaxAge)
		}

		if strings.EqualFold(r.Method, http.MethodOptions) {
			w.WriteHeader(http.StatusNoContent)

			return
		}

		next.ServeHTTP(w, r)
	})
}
