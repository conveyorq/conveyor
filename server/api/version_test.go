// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResolveServerVersionNeverEmpty(t *testing.T) {
	// Under `go test` the binary carries no module version, so resolution
	// falls back to the dev marker rather than returning an empty string.
	require.NotEmpty(t, resolveServerVersion())
}

func TestServerVersionInitialized(t *testing.T) {
	require.NotEmpty(t, serverVersion)
}
