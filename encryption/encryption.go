// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

// Package encryption defines the pluggable seam Conveyor uses to encrypt task
// payloads and results. It is the bring-your-own extension point: provide any
// Encryptor and Conveyor stores and relays only its ciphertext, never the
// plaintext. A built-in AES-256-GCM implementation (AESGCM) ships for callers
// who do not need their own key-management integration.
//
// The same Encryptor serves both placements: an SDK-side codec that encrypts
// before enqueue and decrypts on dispatch (end to end, the server never holds
// plaintext), and a server-side broker decorator that encrypts at rest. A
// deployment uses one or the other for a given payload, never both.
package encryption

import (
	"context"
	"errors"
)

// Errors returned when decrypting. Match them with errors.Is.
var (
	// ErrUnknownKeyID reports ciphertext sealed under a key id the Encryptor
	// does not hold — typically a key that was retired before its data was
	// re-encrypted, or ciphertext from a different keyring.
	ErrUnknownKeyID = errors.New("encryption: unknown key id")

	// ErrMalformedCiphertext reports ciphertext that is truncated or does not
	// match the framing this package writes.
	ErrMalformedCiphertext = errors.New("encryption: malformed ciphertext")

	// ErrAuthentication reports ciphertext that failed its authentication tag:
	// it was tampered with, corrupted, or sealed under a different key.
	ErrAuthentication = errors.New("encryption: authentication failed")

	// ErrInvalidKey reports a key that cannot be used to build an Encryptor,
	// e.g. an empty id or wrong-length secret.
	ErrInvalidKey = errors.New("encryption: invalid key")
)

// Encryptor seals and opens opaque byte slices. Implementations must be safe
// for concurrent use: Conveyor calls Encrypt and Decrypt from many goroutines.
//
// Decrypt must accept any ciphertext a prior Encrypt produced, including
// ciphertext sealed under a now-retired key the implementation still holds, so
// that key rotation never strands stored data.
//
// The context carries cancellation and deadlines for implementations that call
// out to a key-management service; the built-in AESGCM is local and ignores it.
type Encryptor interface {
	// Encrypt seals plaintext, returning ciphertext that Decrypt can later open.
	Encrypt(ctx context.Context, plaintext []byte) (ciphertext []byte, err error)

	// Decrypt opens ciphertext produced by Encrypt, returning the plaintext.
	Decrypt(ctx context.Context, ciphertext []byte) (plaintext []byte, err error)
}
