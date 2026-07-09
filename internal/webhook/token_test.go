// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package webhook

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLeaseTokenRoundTrip(t *testing.T) {
	token := MintLeaseToken("secret", "hooks.prod", "task-1", "lease-1")

	claims, err := ParseLeaseToken(token)
	require.NoError(t, err)
	require.Equal(t, "hooks.prod", claims.Registration)
	require.Equal(t, "task-1", claims.TaskID)
	require.Equal(t, "lease-1", claims.LeaseID)

	require.NoError(t, VerifyLeaseToken(token, claims, []string{"secret"}))
}

func TestLeaseTokenVerifiesAgainstAnySecret(t *testing.T) {
	token := MintLeaseToken("old-secret", "hooks", "t", "l")

	claims, err := ParseLeaseToken(token)
	require.NoError(t, err)

	// A rotation in progress verifies tokens minted with the older secret.
	require.NoError(t, VerifyLeaseToken(token, claims, []string{"new-secret", "old-secret"}))

	err = VerifyLeaseToken(token, claims, []string{"new-secret"})
	require.ErrorIs(t, err, ErrInvalidToken, "a removed secret must stop verifying")
}

func TestLeaseTokenRejectsTampering(t *testing.T) {
	token := MintLeaseToken("secret", "hooks", "task-1", "lease-1")

	claims, err := ParseLeaseToken(token)
	require.NoError(t, err)

	// A token whose claims were swapped onto another delivery fails
	// verification: the signature binds all three claims.
	claims.TaskID = "task-2"
	require.ErrorIs(t, VerifyLeaseToken(token, claims, []string{"secret"}), ErrInvalidToken, "tampered task must fail")

	claims.TaskID = "task-1"
	claims.LeaseID = "lease-2"
	require.ErrorIs(t, VerifyLeaseToken(token, claims, []string{"secret"}), ErrInvalidToken, "tampered lease must fail")
}

func TestParseLeaseTokenRejectsMalformed(t *testing.T) {
	valid := "aGVsbG8" // "hello" in raw base64url, a decodable segment.

	cases := map[string]string{
		"empty":             "",
		"too few segments":  "a.b",
		"too many segments": "a.b.c.d.e",
		"bad registration":  "!!!." + valid + "." + valid + ".mac",
		"bad task id":       valid + ".!!!." + valid + ".mac",
		"bad lease id":      valid + "." + valid + ".!!!.mac",
	}

	for name, token := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := ParseLeaseToken(token)
			require.ErrorIs(t, err, ErrInvalidToken, "token %q must not parse", token)
		})
	}
}

// TestVerifyLeaseTokenRejectsMalformed proves verification rejects a token
// with the wrong segment count before it ever computes a MAC.
func TestVerifyLeaseTokenRejectsMalformed(t *testing.T) {
	claims := &LeaseClaims{Registration: "hooks", TaskID: "t", LeaseID: "l"}

	err := VerifyLeaseToken("a.b", claims, []string{"secret"})
	require.ErrorIs(t, err, ErrInvalidToken, "a token with too few segments must not verify")
}
