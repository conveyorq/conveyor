// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package webhook

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/conveyorq/conveyor/internal/clock"
)

func TestHMACSignerStampsVerifiableHeaders(t *testing.T) {
	signer := NewHMACSigner("secret", clock.NewFake(time.Unix(1751980800, 0)))

	header := http.Header{}
	body := []byte(`{"jsonrpc":"2.0"}`)
	signer.Sign(header, body)

	require.Equal(t, "1751980800", header.Get(TimestampHeader))

	signature := header.Get(SignatureHeader)
	require.True(t, strings.HasPrefix(signature, "v1="), "signature carries the scheme version")

	expected := SignBody("secret", "1751980800", body)
	require.Equal(t, "v1="+expected, signature, "a receiver recomputing the HMAC matches")

	// A different secret, timestamp, or body all change the signature.
	require.NotEqual(t, expected, SignBody("other", "1751980800", body))
	require.NotEqual(t, expected, SignBody("secret", "1751980801", body))
	require.NotEqual(t, expected, SignBody("secret", "1751980800", []byte("tampered")))
}
