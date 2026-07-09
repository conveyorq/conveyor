// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package webhook

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

// Lease tokens authenticate the asynchronous-completion callbacks of one
// webhook delivery. A token binds (registration, task, lease) under an HMAC
// keyed by the registration's signing secret, so it authorizes nothing
// beyond one delivery's heartbeat and result, and a token minted for a
// superseded lease cannot touch the current one. Liveness needs no expiry
// field: the broker's lease check is the gate, and a token outlives its
// lease only as a key to a lock that no longer exists.

// ErrInvalidToken rejects a lease token that is malformed or fails
// verification against every registration secret.
var ErrInvalidToken = errors.New("webhook: invalid lease token")

// tokenSegments is the number of dot-separated token segments.
const tokenSegments = 4

// LeaseClaims are the verified contents of one lease token.
type LeaseClaims struct {
	// Registration is the webhook worker registration name.
	Registration string
	// TaskID is the delivered task.
	TaskID string
	// LeaseID is the delivery the token was minted for.
	LeaseID string
}

// MintLeaseToken builds the lease token for one delivery, signed with the
// registration's newest secret.
func MintLeaseToken(secret, registration, taskID, leaseID string) string {
	encode := base64.RawURLEncoding.EncodeToString

	return strings.Join([]string{
		encode([]byte(registration)),
		encode([]byte(taskID)),
		encode([]byte(leaseID)),
		tokenMAC(secret, registration, taskID, leaseID),
	}, ".")
}

// ParseLeaseToken decodes a token's claims without verifying them. The
// caller uses the registration name to load the secrets, then verifies with
// VerifyLeaseToken.
func ParseLeaseToken(token string) (*LeaseClaims, error) {
	segments := strings.Split(token, ".")
	if len(segments) != tokenSegments {
		return nil, ErrInvalidToken
	}

	decode := func(segment string) (string, error) {
		raw, err := base64.RawURLEncoding.DecodeString(segment)
		if err != nil {
			return "", fmt.Errorf("%w: %w", ErrInvalidToken, err)
		}

		return string(raw), nil
	}

	registration, err := decode(segments[0])
	if err != nil {
		return nil, err
	}

	taskID, err := decode(segments[1])
	if err != nil {
		return nil, err
	}

	leaseID, err := decode(segments[2])
	if err != nil {
		return nil, err
	}

	return &LeaseClaims{Registration: registration, TaskID: taskID, LeaseID: leaseID}, nil
}

// VerifyLeaseToken checks a parsed token's signature against the
// registration's secrets; any of them verifies, so a rotation in progress
// keeps old tokens valid.
func VerifyLeaseToken(token string, claims *LeaseClaims, secrets []string) error {
	segments := strings.Split(token, ".")
	if len(segments) != tokenSegments {
		return ErrInvalidToken
	}

	provided := []byte(segments[tokenSegments-1])

	for _, secret := range secrets {
		expected := []byte(tokenMAC(secret, claims.Registration, claims.TaskID, claims.LeaseID))

		if hmac.Equal(provided, expected) {
			return nil
		}
	}

	return ErrInvalidToken
}

// tokenMAC computes the hex HMAC-SHA256 binding one delivery's claims.
func tokenMAC(secret, registration, taskID, leaseID string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(registration))
	mac.Write([]byte{0})
	mac.Write([]byte(taskID))
	mac.Write([]byte{0})
	mac.Write([]byte(leaseID))

	return hex.EncodeToString(mac.Sum(nil))
}
