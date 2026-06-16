// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

// Package dashboard embeds the built read+write operations console and serves
// it as an http.Handler. The single-page app is compiled by Vite into dist/
// and committed, so the Go build never depends on a Node toolchain.
package dashboard

import (
	"embed"
	"errors"
	"io/fs"
	"net/http"
)

// distFS holds the built single-page app. The committed dist/ tree is produced
// by `make dashboard` (Vite); see web/dashboard/README.md.
//
//go:embed all:dist
var distFS embed.FS

// indexFile is the SPA entrypoint served for the root and for any path that
// does not map to a built asset (client-side routing).
const indexFile = "index.html"

// Assets returns the embedded dist/ tree as a filesystem, rooted at dist/.
func Assets() (fs.FS, error) {
	return fs.Sub(distFS, "dist")
}

// Handler returns an http.Handler serving the embedded dashboard. It serves
// over the standard library file server (which cleans paths and rejects
// traversal), with an index.html fallback so client-side routing works.
func Handler() (http.Handler, error) {
	root, err := Assets()
	if err != nil {
		return nil, err
	}

	return http.FileServerFS(spaFS{root}), nil
}

// spaFS is the embedded filesystem with single-page-app fallback: any name
// that does not resolve to a built file (an unknown client-routed path)
// resolves to index.html instead, so the app's own router handles it. Because
// every missing lookup returns index.html, the file server never lists a
// directory and never 404s a client route.
type spaFS struct {
	// FS is the embedded dist/ tree.
	fs.FS
}

// Open returns the named file, falling back to index.html when it is absent.
func (f spaFS) Open(name string) (fs.File, error) {
	file, err := f.FS.Open(name)
	if errors.Is(err, fs.ErrNotExist) {
		return f.FS.Open(indexFile)
	}

	return file, err
}
