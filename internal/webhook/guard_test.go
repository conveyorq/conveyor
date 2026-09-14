// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package webhook

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsDisallowedIP(t *testing.T) {
	cases := map[string]struct {
		ip         string
		disallowed bool
	}{
		"loopback v4":          {"127.0.0.1", true},
		"loopback v6":          {"::1", true},
		"private 10":           {"10.0.0.1", true},
		"private 192.168":      {"192.168.1.1", true},
		"private 172.16":       {"172.16.5.4", true},
		"link-local":           {"169.254.169.254", true},
		"unique-local v6":      {"fd00::1", true},
		"unspecified v4":       {"0.0.0.0", true},
		"multicast":            {"224.0.0.1", true},
		"mapped loopback":      {"::ffff:127.0.0.1", true},
		"broadcast v4":         {"255.255.255.255", true},
		"carrier-grade nat":    {"100.64.0.1", true},
		"carrier-grade nat hi": {"100.127.255.254", true},
		"reserved 240":         {"240.0.0.1", true},
		"site-local v6":        {"fec0::1", true},
		"public v4":            {"93.184.216.34", false},
		"public v6":            {"2606:2800:220:1:248:1893:25c8:1946", false},
		"public cloudflare v4": {"1.1.1.1", false},
		// 100.63.x and 100.128.x sit just outside carrier-grade NAT and stay
		// public, so the range check must not over-reach.
		"just below cgnat": {"100.63.255.255", false},
		"just above cgnat": {"100.128.0.0", false},
		// IPv6 transition addresses are judged by the IPv4 address they embed:
		// a NAT64 or 6to4 gateway forwards to it, so an embedded internal
		// address reaches the inside while an embedded public one does not.
		"nat64 metadata":  {"64:ff9b::a9fe:a9fe", true},
		"nat64 loopback":  {"64:ff9b::7f00:1", true},
		"nat64 private":   {"64:ff9b::a00:1", true},
		"nat64 local-use": {"64:ff9b:1::a00:1", true},
		"6to4 private":    {"2002:a00:1::", true},
		"nat64 public":    {"64:ff9b::101:101", false},
		"6to4 public":     {"2002:101:101::", false},
	}

	for name, testCase := range cases {
		ip := net.ParseIP(testCase.ip)
		require.NotNil(t, ip, "case %s: %q did not parse", name, testCase.ip)
		require.Equal(t, testCase.disallowed, isDisallowedIP(ip), "case %s", name)
	}
}

func TestCheckURLTarget(t *testing.T) {
	cases := map[string]struct {
		url          string
		allowPrivate bool
		wantErr      bool
	}{
		"public host passes":        {"https://example.com/tasks", false, false},
		"loopback literal rejected": {"http://127.0.0.1/tasks", false, true},
		"metadata literal rejected": {"http://169.254.169.254/latest", false, true},
		"private literal rejected":  {"https://10.1.2.3/tasks", false, true},
		"public literal passes":     {"https://1.1.1.1/tasks", false, false},
		"loopback allowed when set": {"http://127.0.0.1/tasks", true, false},
		"hostname deferred to dial": {"https://internal.svc.local/x", false, false},
	}

	for name, testCase := range cases {
		err := CheckURLTarget(testCase.url, testCase.allowPrivate)

		if testCase.wantErr {
			require.Error(t, err, "case %s", name)

			continue
		}

		require.NoError(t, err, "case %s", name)
	}
}

// TestGuardedDialerRefusesLoopback proves the dial-time guard is authoritative:
// the same loopback endpoint that a private-allowing client reaches is refused
// by a public-only client, even though a hostname passes registration.
func TestGuardedDialerRefusesLoopback(t *testing.T) {
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request Request
		require.NoError(t, json.NewDecoder(r.Body).Decode(&request))

		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":"` + request.ID + `","result":{"status":"completed"}}`))
	}))
	defer endpoint.Close()

	execute := NewExecuteRequest("lease-1", &TaskParams{TaskID: "t1"})

	_, err := NewClient(false).Call(context.Background(), endpoint.URL, nil, execute)
	require.Error(t, err, "a public-only client must refuse a loopback endpoint")

	_, err = NewClient(true).Call(context.Background(), endpoint.URL, nil, execute)
	require.NoError(t, err, "a private-allowing client reaches the same endpoint")
}

// TestGuardedDialerRefusesHostnameResolvingPrivate covers the resolve-then-vet
// branch: a hostname passes registration because no name is resolved there, and
// the dialer is what refuses it once the name turns out to point inside. The
// error names the setting that permits it, so an operator running private
// endpoints on purpose can act on the message.
func TestGuardedDialerRefusesHostnameResolvingPrivate(t *testing.T) {
	dial := guardedDialContext(false)

	// localhost resolves to loopback on every supported platform, which makes it
	// a hostname whose every address is non-public.
	_, err := dial(context.Background(), "tcp", "localhost:9")
	require.Error(t, err, "a hostname resolving only to loopback must be refused")
	require.ErrorContains(t, err, allowPrivateHint)

	// The same name is reached when the guard is off.
	allowed := guardedDialContext(true)
	conn, err := allowed(context.Background(), "tcp", net.JoinHostPort("127.0.0.1", "9"))

	if err == nil {
		require.NoError(t, conn.Close())
	}
}
