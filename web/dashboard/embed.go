// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

// Package dashboard embeds the built read+write operations console and serves
// it as an http.Handler. The single-page app is compiled by Vite into dist/,
// which is git-ignored and produced by `make dashboard` (CI and the Docker
// build run it). A committed dist/.gitkeep keeps the //go:embed directive
// compiling on a fresh checkout, so the Go build never depends on a Node
// toolchain even before the bundle is built.
package dashboard

import (
	"embed"
	"errors"
	"io/fs"
	"net/http"
	"path"
)

// distFS holds the built single-page app. The dist/ tree is produced by
// `make dashboard` (Vite) and is git-ignored; only dist/.gitkeep is committed,
// so this embed compiles on a fresh checkout. See web/dashboard/README.md.
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

// spaFS is the embedded filesystem with single-page-app fallback: an unknown
// client-routed path (one with no file extension) resolves to index.html
// instead, so the app handles it. A missing path that looks like an asset (it
// has an extension, e.g. a stale hashed bundle) returns a real not-exist error
// so the file server answers 404 rather than masking a broken deploy by serving
// HTML where JS was expected.
type spaFS struct {
	// FS is the embedded dist/ tree.
	fs.FS
}

// Open returns the named file, falling back to index.html for extensionless
// (client-routed) names that are absent.
func (f spaFS) Open(name string) (fs.File, error) {
	file, err := f.FS.Open(name)
	if errors.Is(err, fs.ErrNotExist) && path.Ext(name) == "" {
		return f.FS.Open(indexFile)
	}

	return file, err
}
