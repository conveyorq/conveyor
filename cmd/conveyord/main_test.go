package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/tochemey/conveyor/server"
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

	require.NoError(t, os.WriteFile(path, []byte("mode: standalone\nbroker:\n  driver: memory\n"), 0o600))

	config, err := loadConfig(path, "cluster", false)
	require.NoError(t, err)
	require.Equal(t, server.ModeCluster, config.Mode)
}

func TestLoadConfigRejectsInvalidModeOverride(t *testing.T) {
	path := filepath.Join(t.TempDir(), "conveyor.yaml")

	require.NoError(t, os.WriteFile(path, []byte("broker:\n  driver: memory\n"), 0o600))

	_, err := loadConfig(path, "warp-drive", false)
	require.ErrorContains(t, err, "mode")
}

func TestLoadConfigRequiresPostgresDSNByDefault(t *testing.T) {
	_, err := loadConfig("", "", false)
	require.ErrorContains(t, err, "broker.dsn")
}
