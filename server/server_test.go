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
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/conveyorq/conveyor/internal/clock"
)

// startTestServer boots a dev-config server on an ephemeral port and
// registers a graceful stop as cleanup.
func startTestServer(t *testing.T) *Server {
	t.Helper()

	ports := freePorts(t, 3)

	config := DevConfig()
	config.API.Listen = "127.0.0.1:0"
	config.Metrics.Listen = "127.0.0.1:0"
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
	config.Metrics.Listen = "127.0.0.1:0"

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

// writeTestKeyPair generates a self-signed certificate and writes the
// certificate and private key as PEM files in dir, returning their paths.
func writeTestKeyPair(t *testing.T, dir string) (certFile, keyFile string) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "conveyor-test"},
		NotBefore:    clock.System().Now().Add(-time.Hour),
		NotAfter:     clock.System().Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		IsCA:         true,
	}

	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}

	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}

	certFile = filepath.Join(dir, "cert.pem")
	keyFile = filepath.Join(dir, "key.pem")

	writePEM(t, certFile, "CERTIFICATE", der)
	writePEM(t, keyFile, "PRIVATE KEY", keyDER)

	return certFile, keyFile
}

// writePEM encodes block as a PEM file of the given type at path.
func writePEM(t *testing.T, path, blockType string, der []byte) {
	t.Helper()

	encoded := pem.EncodeToMemory(&pem.Block{Type: blockType, Bytes: der})
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestBuildClusterTLSDisabledWhenNoCert(t *testing.T) {
	node := &Server{config: DevConfig()}

	info, err := node.buildClusterTLS()
	if err != nil {
		t.Fatal(err)
	}

	if info != nil {
		t.Fatalf("expected nil TLS info when no certificate is configured, got %v", info)
	}
}

func TestBuildClusterTLSServerOnly(t *testing.T) {
	dir := t.TempDir()
	certFile, keyFile := writeTestKeyPair(t, dir)

	config := DevConfig()
	config.Cluster.TLS = TLSConfig{CertFile: certFile, KeyFile: keyFile}
	node := &Server{config: config}

	info, err := node.buildClusterTLS()
	if err != nil {
		t.Fatal(err)
	}

	if info == nil || info.ServerConfig == nil || info.ClientConfig == nil {
		t.Fatal("expected both server and client TLS configs to be set")
	}

	if info.ServerConfig.ClientAuth != tls.NoClientCert {
		t.Errorf("client auth = %v, must not require client certs without a CA", info.ServerConfig.ClientAuth)
	}
}

func TestBuildClusterTLSMutualWithCA(t *testing.T) {
	dir := t.TempDir()
	certFile, keyFile := writeTestKeyPair(t, dir)

	config := DevConfig()
	config.Cluster.TLS = TLSConfig{CertFile: certFile, KeyFile: keyFile, CAFile: certFile}
	node := &Server{config: config}

	info, err := node.buildClusterTLS()
	if err != nil {
		t.Fatal(err)
	}

	if info.ServerConfig.ClientAuth != tls.RequireAndVerifyClientCert {
		t.Errorf("client auth = %v, want RequireAndVerifyClientCert with a CA", info.ServerConfig.ClientAuth)
	}

	if info.ServerConfig.ClientCAs == nil || info.ClientConfig.RootCAs == nil {
		t.Fatal("expected the CA pool wired into both server ClientCAs and client RootCAs")
	}
}

func TestBuildClusterTLSRejectsMissingFiles(t *testing.T) {
	config := DevConfig()
	config.Cluster.TLS = TLSConfig{
		CertFile: filepath.Join(t.TempDir(), "absent-cert.pem"),
		KeyFile:  filepath.Join(t.TempDir(), "absent-key.pem"),
	}
	node := &Server{config: config}

	if _, err := node.buildClusterTLS(); err == nil {
		t.Fatal("expected an error for a missing key pair")
	}
}

func TestBuildClusterTLSRejectsMissingCA(t *testing.T) {
	dir := t.TempDir()
	certFile, keyFile := writeTestKeyPair(t, dir)

	config := DevConfig()
	config.Cluster.TLS = TLSConfig{
		CertFile: certFile,
		KeyFile:  keyFile,
		CAFile:   filepath.Join(dir, "absent-ca.pem"),
	}
	node := &Server{config: config}

	if _, err := node.buildClusterTLS(); err == nil {
		t.Fatal("expected an error for an unreadable CA file")
	}
}

func TestBuildClusterTLSRejectsEmptyCA(t *testing.T) {
	dir := t.TempDir()
	certFile, keyFile := writeTestKeyPair(t, dir)

	caFile := filepath.Join(dir, "empty-ca.pem")
	if err := os.WriteFile(caFile, []byte("not a certificate"), 0o600); err != nil {
		t.Fatal(err)
	}

	config := DevConfig()
	config.Cluster.TLS = TLSConfig{CertFile: certFile, KeyFile: keyFile, CAFile: caFile}
	node := &Server{config: config}

	if _, err := node.buildClusterTLS(); err == nil {
		t.Fatal("expected an error for a CA file with no certificates")
	}
}
