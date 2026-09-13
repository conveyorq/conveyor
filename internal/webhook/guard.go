// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package webhook

import (
	"context"
	"fmt"
	"net"
	"net/url"
)

// isDisallowedIP reports whether an address falls in a range a webhook must
// never target: loopback, link-local (which includes the cloud metadata
// address 169.254.169.254), private, unspecified, and multicast. A genuine
// public delivery endpoint never resolves to one of these, so refusing them
// closes the server-side request forgery path an admin token would otherwise
// open onto internal services.
func isDisallowedIP(ip net.IP) bool {
	return ip.IsLoopback() ||
		ip.IsLinkLocalUnicast() ||
		ip.IsLinkLocalMulticast() ||
		ip.IsPrivate() ||
		ip.IsUnspecified() ||
		ip.IsMulticast()
}

// CheckURLTarget rejects a webhook URL whose host is an IP literal in a
// non-public range, unless allowPrivate is set. It performs no name
// resolution: a hostname passes here and is verified against its resolved
// address at dial time by the guarded client. Registration and config
// validation call it for immediate feedback on the obvious misconfiguration (a
// literal loopback, link-local, or private target) without doing network I/O
// where none belongs.
func CheckURLTarget(rawURL string, allowPrivate bool) error {
	if allowPrivate {
		return nil
	}

	parsed, err := url.Parse(rawURL)
	if err != nil {
		// The caller validates URL syntax before reaching here; a malformed
		// URL is its error to report, not this guard's.
		return nil
	}

	ip := net.ParseIP(parsed.Hostname())
	if ip == nil {
		// A hostname; its resolved address is verified at dial time.
		return nil
	}

	if isDisallowedIP(ip) {
		return fmt.Errorf("webhook target %s is a non-public address; set webhooks.allow_private_targets to permit it", parsed.Hostname())
	}

	return nil
}

// guardedDialContext builds a DialContext that resolves the host itself and
// connects only to a vetted public address, pinning the exact IP it checked.
// Pinning defeats DNS rebinding: the address the guard approved is the address
// the connection uses, with no second lookup in between. When allowPrivate is
// set the guard is bypassed and the standard dialer is used.
func guardedDialContext(allowPrivate bool) func(ctx context.Context, network, addr string) (net.Conn, error) {
	var dialer net.Dialer

	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		if allowPrivate {
			return dialer.DialContext(ctx, network, addr)
		}

		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, fmt.Errorf("webhook: parsing dial address %q: %w", addr, err)
		}

		if ip := net.ParseIP(host); ip != nil {
			if isDisallowedIP(ip) {
				return nil, fmt.Errorf("webhook: refusing to dial non-public address %s", ip)
			}

			return dialer.DialContext(ctx, network, addr)
		}

		addresses, err := net.DefaultResolver.LookupIPAddr(ctx, host)
		if err != nil {
			return nil, fmt.Errorf("webhook: resolving %s: %w", host, err)
		}

		for _, candidate := range addresses {
			if !isDisallowedIP(candidate.IP) {
				return dialer.DialContext(ctx, network, net.JoinHostPort(candidate.IP.String(), port))
			}
		}

		return nil, fmt.Errorf("webhook: %s resolves only to non-public addresses", host)
	}
}
