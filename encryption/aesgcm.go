// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package encryption

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"fmt"
)

// AES-256-GCM framing parameters.
const (
	// schemeVersion is the first byte of every ciphertext; it lets the framing
	// evolve without ambiguity.
	schemeVersion byte = 1
	// keyBytes is the AES-256 key length.
	keyBytes = 32
	// nonceBytes is the standard GCM nonce length; NewGCM produces this size.
	nonceBytes = 12
	// maxKeyIDBytes is the largest key id the framing can record, bounded by the
	// single length byte that precedes it.
	maxKeyIDBytes = 255
	// headerFixedBytes is the version byte plus the key-id length byte that
	// precede the key id.
	headerFixedBytes = 2
)

// Key is a named AES-256 secret. The id labels ciphertext so the matching
// secret can be found again after the active key rotates; it is stored in the
// clear and must not itself be sensitive.
type Key struct {
	// ID labels ciphertext sealed under this key. It must be non-empty and at
	// most 255 bytes.
	ID string
	// Secret is the 32-byte AES-256 key material.
	Secret []byte
}

// AESGCM satisfies Encryptor.
var _ Encryptor = (*AESGCM)(nil)

// AESGCM is the built-in Encryptor: AES-256-GCM with a fresh random nonce per
// call and a key id framed into each ciphertext. It holds a keyring so a
// retired key still opens data sealed under it, and seals new data under the
// active key. It is safe for concurrent use.
type AESGCM struct {
	// ring maps key id to its AEAD, holding every key that can still decrypt.
	ring map[string]cipher.AEAD
	// activeID is the key id new ciphertext is sealed under.
	activeID string
	// active is the AEAD for activeID, kept aside to avoid a map lookup on the
	// hot encrypt path.
	active cipher.AEAD
}

// NewAESGCM builds an AESGCM that seals under the key identified by activeKeyID
// and can open data sealed under any key in keys. Every key must have a
// non-empty id of at most 255 bytes, a 32-byte secret, and a unique id; one of
// them must be activeKeyID. Pass several keys to support rotation: keep the old
// key to decrypt existing data while a new active key seals fresh data.
func NewAESGCM(activeKeyID string, keys ...Key) (*AESGCM, error) {
	if activeKeyID == "" {
		return nil, fmt.Errorf("%w: active key id is empty", ErrInvalidKey)
	}

	if len(keys) == 0 {
		return nil, fmt.Errorf("%w: no keys provided", ErrInvalidKey)
	}

	ring := make(map[string]cipher.AEAD, len(keys))

	for _, key := range keys {
		aead, err := newAEAD(key)
		if err != nil {
			return nil, err
		}

		if _, duplicate := ring[key.ID]; duplicate {
			return nil, fmt.Errorf("%w: duplicate key id %q", ErrInvalidKey, key.ID)
		}

		ring[key.ID] = aead
	}

	active, ok := ring[activeKeyID]
	if !ok {
		return nil, fmt.Errorf("%w: active key id %q not among keys", ErrInvalidKey, activeKeyID)
	}

	return &AESGCM{ring: ring, activeID: activeKeyID, active: active}, nil
}

// newAEAD validates one key and returns its AES-256-GCM AEAD.
func newAEAD(key Key) (cipher.AEAD, error) {
	switch {
	case key.ID == "":
		return nil, fmt.Errorf("%w: empty key id", ErrInvalidKey)

	case len(key.ID) > maxKeyIDBytes:
		return nil, fmt.Errorf("%w: key id %q exceeds %d bytes", ErrInvalidKey, key.ID, maxKeyIDBytes)

	case len(key.Secret) != keyBytes:
		return nil, fmt.Errorf("%w: key %q secret is %d bytes, want %d", ErrInvalidKey, key.ID, len(key.Secret), keyBytes)
	}

	block, err := aes.NewCipher(key.Secret)
	if err != nil {
		return nil, fmt.Errorf("%w: key %q: %v", ErrInvalidKey, key.ID, err)
	}

	return cipher.NewGCM(block)
}

// Encrypt seals plaintext under the active key. The returned ciphertext is the
// framed header, a fresh random nonce, and the AES-256-GCM sealed bytes; the
// header is authenticated as additional data, binding the key id to the result.
func (a *AESGCM) Encrypt(_ context.Context, plaintext []byte) ([]byte, error) {
	header := encodeHeader(a.activeID)

	nonce := make([]byte, nonceBytes)
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("encryption: reading nonce: %w", err)
	}

	// Build the output as a fresh buffer holding header || nonce, distinct from
	// header (passed to Seal as additional data, so the destination must not
	// alias it); Seal appends the sealed bytes. Only the fixed-size prefix is
	// sized here: folding len(plaintext) into the capacity risks an integer
	// overflow in the size computation, and Seal grows the buffer in one
	// allocation regardless.
	out := make([]byte, len(header)+nonceBytes)
	copy(out, header)
	copy(out[len(header):], nonce)

	return a.active.Seal(out, nonce, plaintext, header), nil
}

// Decrypt opens ciphertext produced by Encrypt, looking up the key named in the
// framed header. It returns ErrMalformedCiphertext for truncated or unknown
// framing, ErrUnknownKeyID when the key is not held, and ErrAuthentication when
// the authentication tag does not verify.
func (a *AESGCM) Decrypt(_ context.Context, ciphertext []byte) ([]byte, error) {
	keyID, headerLen, err := decodeHeader(ciphertext)
	if err != nil {
		return nil, err
	}

	aead, ok := a.ring[keyID]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrUnknownKeyID, keyID)
	}

	rest := ciphertext[headerLen:]
	if len(rest) < nonceBytes {
		return nil, fmt.Errorf("%w: missing nonce", ErrMalformedCiphertext)
	}

	nonce, sealed := rest[:nonceBytes], rest[nonceBytes:]

	plaintext, err := aead.Open(nil, nonce, sealed, ciphertext[:headerLen])
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrAuthentication, err)
	}

	return plaintext, nil
}

// encodeHeader frames the scheme version, the key-id length, and the key id.
// The result is also passed as GCM additional data, so a tampered header fails
// authentication.
func encodeHeader(keyID string) []byte {
	header := make([]byte, 0, headerFixedBytes+len(keyID))
	header = append(header, schemeVersion, byte(len(keyID)))

	return append(header, keyID...)
}

// decodeHeader parses the header written by encodeHeader, returning the key id
// and the total header length so the caller can slice past it.
func decodeHeader(ciphertext []byte) (keyID string, headerLen int, err error) {
	if len(ciphertext) < headerFixedBytes {
		return "", 0, fmt.Errorf("%w: shorter than header", ErrMalformedCiphertext)
	}

	if ciphertext[0] != schemeVersion {
		return "", 0, fmt.Errorf("%w: unsupported version %d", ErrMalformedCiphertext, ciphertext[0])
	}

	idLen := int(ciphertext[1])
	headerLen = headerFixedBytes + idLen

	if len(ciphertext) < headerLen {
		return "", 0, fmt.Errorf("%w: truncated key id", ErrMalformedCiphertext)
	}

	return string(ciphertext[headerFixedBytes:headerLen]), headerLen, nil
}
