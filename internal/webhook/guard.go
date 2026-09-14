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

// allowPrivateHint names the setting that permits a non-public target, so an
// operator delivering to an in-cluster or on-premises endpoint learns the remedy
// from the failure itself rather than from the source.
const allowPrivateHint = "set webhooks.allow_private_targets to permit it"

// reservedRanges are non-public ranges that the standard library's own
// predicates do not classify: carrier-grade NAT, which addresses tailnets and
// some cluster pod networks; the reserved IPv4 space; and the deprecated IPv6
// site-local block. An endpoint in one of these is as internal as a private
// address, so it is refused alongside them.
var reservedRanges = []*net.IPNet{
	mustParseCIDR("100.64.0.0/10"),
	mustParseCIDR("240.0.0.0/4"),
	mustParseCIDR("fec0::/10"),
}

// embeddedIPv4Ranges are IPv6 blocks that carry an IPv4 address inside them
// and route to it: the NAT64 well-known prefix and its local-use variant, in
// which the last four bytes are the IPv4 address, and 6to4, in which bytes two
// through five are. An address in one of these is as reachable as the IPv4
// address it embeds, so it is judged by that address.
var embeddedIPv4Ranges = []struct {
	// network is the transition block.
	network *net.IPNet
	// offset is where the embedded IPv4 address starts in the 16-byte form.
	offset int
}{
	{network: mustParseCIDR("64:ff9b::/96"), offset: 12},
	{network: mustParseCIDR("64:ff9b:1::/48"), offset: 12},
	{network: mustParseCIDR("2002::/16"), offset: 2},
}

// mustParseCIDR parses a CIDR block fixed at compile time, panicking on a
// malformed one the way regexp.MustCompile does.
func mustParseCIDR(cidr string) *net.IPNet {
	_, network, err := net.ParseCIDR(cidr)
	if err != nil {
		panic("webhook: malformed reserved range " + cidr)
	}

	return network
}

// isDisallowedIP reports whether an address falls in a range a webhook must
// never target. Anything that is not a global unicast address is refused, which
// covers loopback, link-local (and with it the cloud metadata address
// 169.254.169.254), multicast, the unspecified address, and the IPv4 broadcast
// address; private ranges and the reserved ranges above are refused on top of
// that. A genuine public delivery endpoint never resolves to one of these, so
// refusing them closes the server-side request forgery path an admin token would
// otherwise open onto internal services.
func isDisallowedIP(ip net.IP) bool {
	if embedded := embeddedIPv4(ip); embedded != nil {
		return isDisallowedIP(embedded)
	}

	if !ip.IsGlobalUnicast() || ip.IsPrivate() {
		return true
	}

	for _, reserved := range reservedRanges {
		if reserved.Contains(ip) {
			return true
		}
	}

	return false
}

// embeddedIPv4 returns the IPv4 address an IPv6 transition address routes to,
// or nil for an address that embeds none. IPv4-mapped addresses are not
// unwrapped here: the standard predicates already see through them.
func embeddedIPv4(ip net.IP) net.IP {
	full := ip.To16()
	if full == nil || ip.To4() != nil {
		return nil
	}

	for _, transition := range embeddedIPv4Ranges {
		if transition.network.Contains(full) {
			return net.IPv4(full[transition.offset], full[transition.offset+1], full[transition.offset+2], full[transition.offset+3])
		}
	}

	return nil
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
		return fmt.Errorf("webhook target %s is a non-public address; %s", parsed.Hostname(), allowPrivateHint)
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
				return nil, fmt.Errorf("webhook: refusing to dial non-public address %s; %s", ip, allowPrivateHint)
			}

			return dialer.DialContext(ctx, network, addr)
		}

		addresses, err := net.DefaultResolver.LookupIPAddr(ctx, host)
		if err != nil {
			return nil, fmt.Errorf("webhook: resolving %s: %w", host, err)
		}

		// Try every vetted address before giving up, as the standard dialer does
		// across a host's records: one draining endpoint must not fail delivery
		// while a healthy sibling record answers.
		var lastErr error

		for _, candidate := range addresses {
			if isDisallowedIP(candidate.IP) {
				continue
			}

			conn, err := dialer.DialContext(ctx, network, net.JoinHostPort(candidate.IP.String(), port))
			if err == nil {
				return conn, nil
			}

			lastErr = err
		}

		if lastErr != nil {
			return nil, lastErr
		}

		return nil, fmt.Errorf("webhook: %s resolves only to non-public addresses; %s", host, allowPrivateHint)
	}
}
