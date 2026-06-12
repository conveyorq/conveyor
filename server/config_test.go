package server

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// writeConfigFile drops a YAML config into a temp dir and returns its path.
func writeConfigFile(t *testing.T, content string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "conveyor.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	return path
}

func TestDefaultConfigRequiresDSN(t *testing.T) {
	err := DefaultConfig().Validate()
	if err == nil || !strings.Contains(err.Error(), "broker.dsn") {
		t.Fatalf("expected broker.dsn validation error, got %v", err)
	}
}

func TestDevConfigIsValid(t *testing.T) {
	config := DevConfig()
	if err := config.Validate(); err != nil {
		t.Fatalf("dev config must validate: %v", err)
	}

	if config.Broker.Driver != BrokerMemory {
		t.Errorf("dev broker = %q, want %q", config.Broker.Driver, BrokerMemory)
	}

	if !config.AuthDisabled() {
		t.Error("dev config must disable auth")
	}
}

func TestLoadConfigFromFile(t *testing.T) {
	path := writeConfigFile(t, `
mode: cluster
broker:
  driver: postgres
  dsn: postgres://localhost/conveyor
api:
  listen: :9090
  auth_tokens: [secret]
engine:
  lease_ttl: 90s
`)

	config, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}

	if config.Mode != ModeCluster {
		t.Errorf("mode = %q, want %q", config.Mode, ModeCluster)
	}

	if config.Engine.LeaseTTL != 90*time.Second {
		t.Errorf("lease_ttl = %s, want 90s", config.Engine.LeaseTTL)
	}

	// Untouched keys keep their defaults.
	if config.Engine.LeaseBatchMax != 100 {
		t.Errorf("lease_batch_max = %d, want default 100", config.Engine.LeaseBatchMax)
	}

	if config.AuthDisabled() {
		t.Error("auth_tokens set, AuthDisabled() must be false")
	}
}

func TestLoadConfigExpandsEnvInFile(t *testing.T) {
	t.Setenv("TEST_DATABASE_URL", "postgres://expanded/conveyor")

	path := writeConfigFile(t, `
broker:
  driver: postgres
  dsn: ${TEST_DATABASE_URL}
`)

	config, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}

	if config.Broker.DSN != "postgres://expanded/conveyor" {
		t.Errorf("dsn = %q, want expanded value", config.Broker.DSN)
	}
}

func TestLoadConfigEnvOverridesFile(t *testing.T) {
	t.Setenv("CONVEYOR_BROKER__DSN", "postgres://from-env/conveyor")
	t.Setenv("CONVEYOR_API__AUTH_TOKENS", "tok1,tok2")

	path := writeConfigFile(t, `
broker:
  driver: postgres
  dsn: postgres://from-file/conveyor
`)

	config, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}

	if config.Broker.DSN != "postgres://from-env/conveyor" {
		t.Errorf("dsn = %q, env must beat file", config.Broker.DSN)
	}

	if len(config.API.AuthTokens) != 2 {
		t.Errorf("auth_tokens = %v, want 2 tokens from comma-separated env", config.API.AuthTokens)
	}
}

func TestLoadDevConfigHonorsEnvOverrides(t *testing.T) {
	t.Setenv("CONVEYOR_API__LISTEN", "127.0.0.1:18080")

	config, err := LoadDevConfig()
	if err != nil {
		t.Fatal(err)
	}

	if config.API.Listen != "127.0.0.1:18080" {
		t.Errorf("api.listen = %q, env must override the dev preset", config.API.Listen)
	}

	if config.Broker.Driver != BrokerMemory {
		t.Errorf("broker = %q, dev preset must survive where not overridden", config.Broker.Driver)
	}
}

func TestLoadConfigMissingFile(t *testing.T) {
	_, err := LoadConfig(filepath.Join(t.TempDir(), "absent.yaml"))
	if err == nil {
		t.Fatal("expected error for missing config file")
	}
}

func TestValidateRejections(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*Config)
		wantKey string
	}{
		{"bad mode", func(c *Config) { c.Mode = "serverless" }, "mode"},
		{"bad driver", func(c *Config) { c.Broker.Driver = "redis" }, "broker.driver"},
		{"empty listen", func(c *Config) { c.API.Listen = "" }, "api.listen"},
		{"half tls", func(c *Config) { c.API.TLS.CertFile = "cert.pem" }, "api.tls"},
		{"bad discovery", func(c *Config) { c.Cluster.Discovery = "zookeeper" }, "cluster.discovery"},
		{"bad port", func(c *Config) { c.Cluster.RemotingPort = 0 }, "cluster.remoting_port"},
		{"zero lease ttl", func(c *Config) { c.Engine.LeaseTTL = 0 }, "engine.lease_ttl"},
		{"zero batch", func(c *Config) { c.Engine.LeaseBatchMax = 0 }, "engine.lease_batch_max"},
		{"negative retry", func(c *Config) { c.Engine.DefaultMaxRetry = -1 }, "engine.default_max_retry"},
		{"bad log level", func(c *Config) { c.Log.Level = "verbose" }, "log.level"},
		{"bad log format", func(c *Config) { c.Log.Format = "xml" }, "log.format"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			config := DevConfig()
			tc.mutate(config)

			err := config.Validate()
			if err == nil || !strings.Contains(err.Error(), tc.wantKey) {
				t.Fatalf("want error mentioning %q, got %v", tc.wantKey, err)
			}
		})
	}
}

func TestValidateAcceptsAllModesAndProviders(t *testing.T) {
	for _, mode := range []string{ModeStandalone, ModeCluster, ModeKubernetes} {
		config := DevConfig()
		config.Mode = mode

		if err := config.Validate(); err != nil {
			t.Errorf("mode %q must validate: %v", mode, err)
		}
	}

	providers := []string{
		DiscoveryStatic, DiscoveryNATS, DiscoveryConsul, DiscoveryEtcd,
		DiscoveryMDNS, DiscoveryDNSSD, DiscoveryKubernetes,
	}

	for _, p := range providers {
		config := DevConfig()
		config.Cluster.Discovery = p

		if err := config.Validate(); err != nil {
			t.Errorf("discovery %q must validate: %v", p, err)
		}
	}
}

func TestLoadConfigInvalidFails(t *testing.T) {
	path := writeConfigFile(t, `
broker:
  driver: redis
`)

	_, err := LoadConfig(path)
	if err == nil || !strings.Contains(err.Error(), "broker.driver") {
		t.Fatalf("expected broker.driver validation error from LoadConfig, got %v", err)
	}
}
