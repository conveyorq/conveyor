package server

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// startTestServer boots a dev-config server on an ephemeral port and
// registers a graceful stop as cleanup.
func startTestServer(t *testing.T) *Server {
	t.Helper()

	ports := freePorts(t, 3)

	config := DevConfig()
	config.API.Listen = "127.0.0.1:0"
	config.Cluster.RemotingPort = ports[0]
	config.Cluster.DiscoveryPort = ports[1]
	config.Cluster.PeersPort = ports[2]
	config.Log.Level = LogLevelError

	node, err := New(config, NewLogger(config.Log))
	if err != nil {
		t.Fatal(err)
	}

	if err := node.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := node.Stop(ctx); err != nil {
			t.Errorf("stop: %v", err)
		}
	})

	return node
}

func TestHealthz(t *testing.T) {
	node := startTestServer(t)

	resp, err := http.Get(fmt.Sprintf("http://%s%s", node.Addr(), healthzPath))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s = %d, want 200", healthzPath, resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}

	if string(body) != "ok" {
		t.Fatalf("body = %q, want %q", body, "ok")
	}
}

func TestNewRejectsInvalidConfig(t *testing.T) {
	config := DevConfig()
	config.Broker.Driver = "redis"

	if _, err := New(config, NewLogger(config.Log)); err == nil {
		t.Fatal("New must reject an invalid configuration")
	}
}

func TestStopIsGraceful(t *testing.T) {
	node := startTestServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := node.Stop(ctx); err != nil {
		t.Fatalf("graceful stop failed: %v", err)
	}

	if _, err := http.Get(fmt.Sprintf("http://%s%s", node.Addr(), healthzPath)); err == nil {
		t.Fatal("listener must be closed after Stop")
	}
}

func TestReadyzReportsReadiness(t *testing.T) {
	node := startTestServer(t)

	resp, err := http.Get(fmt.Sprintf("http://%s%s", node.Addr(), readyzPath))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s = %d, want 200", readyzPath, resp.StatusCode)
	}
}

func TestReadyzBeforeStartIsUnavailable(t *testing.T) {
	config := DevConfig()
	config.API.Listen = "127.0.0.1:0"

	node, err := New(config, NewLogger(config.Log))
	if err != nil {
		t.Fatal(err)
	}

	recorder := httptest.NewRecorder()
	node.readyz(recorder, httptest.NewRequest(http.MethodGet, readyzPath, nil))

	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("readyz before start = %d, want 503", recorder.Code)
	}
}

func TestAddrBeforeStartIsEmpty(t *testing.T) {
	config := DevConfig()

	node, err := New(config, NewLogger(config.Log))
	if err != nil {
		t.Fatal(err)
	}

	if addr := node.Addr(); addr != "" {
		t.Fatalf("Addr before start = %q, want empty", addr)
	}
}

func TestBuildBrokerRejectsBadPostgresDSN(t *testing.T) {
	config := DefaultConfig()
	config.Broker.DSN = "postgres://nobody@127.0.0.1:1/nothing?connect_timeout=1"

	node, err := New(config, NewLogger(LogConfig{Level: LogLevelError, Format: LogFormatText}))
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := node.buildBroker(ctx); err == nil {
		t.Fatal("expected a connection error for an unreachable DSN")
	}
}

func TestBuildBrokerRejectsUnknownDriver(t *testing.T) {
	node := &Server{config: &Config{Broker: BrokerConfig{Driver: "tape"}}}

	if _, err := node.buildBroker(context.Background()); err == nil {
		t.Fatal("expected an unknown-driver error")
	}
}

func TestBuildDiscoveryRejectsUnwiredProviders(t *testing.T) {
	config := DevConfig()
	config.Cluster.Discovery = DiscoveryNATS

	node, err := New(config, NewLogger(config.Log))
	if err != nil {
		t.Fatal(err)
	}

	if _, err := node.buildDiscovery(); err == nil {
		t.Fatal("expected an error for a provider that is not wired yet")
	}
}

func TestBuildDiscoveryWiresKubernetes(t *testing.T) {
	config := DevConfig()
	config.Cluster.Discovery = DiscoveryKubernetes
	config.Cluster.Kubernetes.Namespace = "conveyor"
	config.Cluster.Kubernetes.PodLabels = map[string]string{"app": "conveyor"}

	node, err := New(config, NewLogger(config.Log))
	if err != nil {
		t.Fatal(err)
	}

	provider, err := node.buildDiscovery()
	if err != nil {
		t.Fatalf("kubernetes discovery must be wired: %v", err)
	}

	if provider == nil {
		t.Fatal("expected a non-nil kubernetes discovery provider")
	}
}

func TestNewLoggerCoversAllLevelsAndFormats(t *testing.T) {
	for _, level := range []string{LogLevelDebug, LogLevelInfo, LogLevelWarn, LogLevelError} {
		for _, format := range []string{LogFormatJSON, LogFormatText} {
			if NewLogger(LogConfig{Level: level, Format: format}) == nil {
				t.Fatalf("NewLogger(%s, %s) returned nil", level, format)
			}
		}
	}
}
