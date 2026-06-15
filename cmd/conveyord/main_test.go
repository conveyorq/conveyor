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

package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/conveyorq/conveyor/server"
)

func TestLoadConfigDevPreset(t *testing.T) {
	config, err := loadConfig("", "", true)
	require.NoError(t, err)
	require.Equal(t, server.ModeStandalone, config.Mode)
	require.Equal(t, server.BrokerMemory, config.Broker.Driver)
	require.True(t, config.AuthDisabled())
}

func TestLoadConfigDevRejectsOtherFlags(t *testing.T) {
	_, err := loadConfig("conveyor.yaml", "", true)
	require.ErrorContains(t, err, "--dev cannot be combined")

	_, err = loadConfig("", "cluster", true)
	require.ErrorContains(t, err, "--dev cannot be combined")
}

func TestLoadConfigFromFileWithModeOverride(t *testing.T) {
	path := filepath.Join(t.TempDir(), "conveyor.yaml")

	require.NoError(t, os.WriteFile(path, []byte("mode: standalone\nbroker:\n  driver: memory\napi:\n  allow_unauthenticated: true\n"), 0o600))

	config, err := loadConfig(path, "cluster", false)
	require.NoError(t, err)
	require.Equal(t, server.ModeCluster, config.Mode)
}

func TestLoadConfigRejectsInvalidModeOverride(t *testing.T) {
	path := filepath.Join(t.TempDir(), "conveyor.yaml")

	require.NoError(t, os.WriteFile(path, []byte("broker:\n  driver: memory\napi:\n  allow_unauthenticated: true\n"), 0o600))

	_, err := loadConfig(path, "warp-drive", false)
	require.ErrorContains(t, err, "mode")
}

func TestLoadConfigRequiresPostgresDSNByDefault(t *testing.T) {
	_, err := loadConfig("", "", false)
	require.ErrorContains(t, err, "broker.dsn")
}
