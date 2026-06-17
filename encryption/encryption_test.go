// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package encryption

import (
	"bytes"
	"context"
	"errors"
	"testing"
)

// TestEncryptorRoundTripThroughInterface exercises the built-in implementation
// through the Encryptor interface, the way Conveyor's callers use it.
func TestEncryptorRoundTripThroughInterface(t *testing.T) {
	var enc Encryptor

	enc, err := NewAESGCM("primary", testKey("primary", 0x01))
	if err != nil {
		t.Fatalf("NewAESGCM: %v", err)
	}

	ctx := context.Background()
	plaintext := []byte("through the seam")

	ciphertext, err := enc.Encrypt(ctx, plaintext)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}

	opened, err := enc.Decrypt(ctx, ciphertext)
	if err != nil {
		t.Fatalf("Decrypt: %v", err)
	}

	if !bytes.Equal(opened, plaintext) {
		t.Fatalf("round trip = %q, want %q", opened, plaintext)
	}
}

// TestSentinelErrorsAreDistinct guards against two sentinels collapsing into
// one, which would let errors.Is match the wrong cause.
func TestSentinelErrorsAreDistinct(t *testing.T) {
	sentinels := []error{
		ErrUnknownKeyID,
		ErrMalformedCiphertext,
		ErrAuthentication,
		ErrInvalidKey,
	}

	for i, outer := range sentinels {
		for j, inner := range sentinels {
			if i == j {
				continue
			}

			if errors.Is(outer, inner) {
				t.Fatalf("sentinel %v matches %v", outer, inner)
			}
		}
	}
}
