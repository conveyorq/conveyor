// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package encryption

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
)

// testKey builds a Key whose secret is filled with the given byte, so each
// test key is distinct and the right length without ceremony.
func testKey(id string, fill byte) Key {
	secret := bytes.Repeat([]byte{fill}, keyBytes)

	return Key{ID: id, Secret: secret}
}

func TestNewAESGCMValidation(t *testing.T) {
	tests := []struct {
		name      string
		activeKey string
		keys      []Key
	}{
		{
			name:      "no active key id",
			activeKey: "",
			keys:      []Key{testKey("primary", 0x01)},
		},
		{
			name:      "no keys",
			activeKey: "primary",
			keys:      nil,
		},
		{
			name:      "active key absent from ring",
			activeKey: "missing",
			keys:      []Key{testKey("primary", 0x01)},
		},
		{
			name:      "empty key id",
			activeKey: "primary",
			keys:      []Key{{ID: "", Secret: bytes.Repeat([]byte{0x01}, keyBytes)}},
		},
		{
			name:      "short secret",
			activeKey: "primary",
			keys:      []Key{{ID: "primary", Secret: bytes.Repeat([]byte{0x01}, keyBytes-1)}},
		},
		{
			name:      "long secret",
			activeKey: "primary",
			keys:      []Key{{ID: "primary", Secret: bytes.Repeat([]byte{0x01}, keyBytes+1)}},
		},
		{
			name:      "duplicate key id",
			activeKey: "primary",
			keys:      []Key{testKey("primary", 0x01), testKey("primary", 0x02)},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewAESGCM(test.activeKey, test.keys...)
			if !errors.Is(err, ErrInvalidKey) {
				t.Fatalf("NewAESGCM error = %v, want ErrInvalidKey", err)
			}
		})
	}
}

func TestNewAESGCMRejectsOversizeKeyID(t *testing.T) {
	id := string(bytes.Repeat([]byte{'a'}, maxKeyIDBytes+1))

	_, err := NewAESGCM(id, testKey(id, 0x01))
	if !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("NewAESGCM error = %v, want ErrInvalidKey", err)
	}
}

func TestAESGCMRoundTrip(t *testing.T) {
	enc, err := NewAESGCM("primary", testKey("primary", 0x01))
	if err != nil {
		t.Fatalf("NewAESGCM: %v", err)
	}

	ctx := context.Background()

	tests := []struct {
		name      string
		plaintext []byte
	}{
		{name: "empty", plaintext: []byte{}},
		{name: "nil", plaintext: nil},
		{name: "small", plaintext: []byte("hello")},
		{name: "binary", plaintext: bytes.Repeat([]byte{0x00, 0xff, 0x7f}, 100)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ciphertext, err := enc.Encrypt(ctx, test.plaintext)
			if err != nil {
				t.Fatalf("Encrypt: %v", err)
			}

			plaintext, err := enc.Decrypt(ctx, ciphertext)
			if err != nil {
				t.Fatalf("Decrypt: %v", err)
			}

			if !bytes.Equal(plaintext, test.plaintext) {
				t.Fatalf("round trip = %q, want %q", plaintext, test.plaintext)
			}
		})
	}
}

func TestAESGCMCiphertextHidesPlaintext(t *testing.T) {
	enc, err := NewAESGCM("primary", testKey("primary", 0x01))
	if err != nil {
		t.Fatalf("NewAESGCM: %v", err)
	}

	plaintext := []byte("the launch code is hunter2")

	ciphertext, err := enc.Encrypt(context.Background(), plaintext)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}

	if bytes.Contains(ciphertext, plaintext) {
		t.Fatal("ciphertext contains plaintext")
	}
}

func TestAESGCMNonceIsFreshPerCall(t *testing.T) {
	enc, err := NewAESGCM("primary", testKey("primary", 0x01))
	if err != nil {
		t.Fatalf("NewAESGCM: %v", err)
	}

	ctx := context.Background()
	plaintext := []byte("same input")

	first, err := enc.Encrypt(ctx, plaintext)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}

	second, err := enc.Encrypt(ctx, plaintext)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}

	if bytes.Equal(first, second) {
		t.Fatal("encrypting the same plaintext twice produced identical ciphertext")
	}
}

func TestAESGCMRotation(t *testing.T) {
	old, err := NewAESGCM("v1", testKey("v1", 0x01))
	if err != nil {
		t.Fatalf("NewAESGCM old: %v", err)
	}

	ctx := context.Background()
	plaintext := []byte("written under the old key")

	sealedUnderOld, err := old.Encrypt(ctx, plaintext)
	if err != nil {
		t.Fatalf("Encrypt old: %v", err)
	}

	// After rotation the active key is v2 but v1 is retained to open old data.
	rotated, err := NewAESGCM("v2", testKey("v1", 0x01), testKey("v2", 0x02))
	if err != nil {
		t.Fatalf("NewAESGCM rotated: %v", err)
	}

	opened, err := rotated.Decrypt(ctx, sealedUnderOld)
	if err != nil {
		t.Fatalf("Decrypt old ciphertext after rotation: %v", err)
	}

	if !bytes.Equal(opened, plaintext) {
		t.Fatalf("opened = %q, want %q", opened, plaintext)
	}

	// New data seals under v2.
	sealedUnderNew, err := rotated.Encrypt(ctx, plaintext)
	if err != nil {
		t.Fatalf("Encrypt rotated: %v", err)
	}

	keyID, _, err := decodeHeader(sealedUnderNew)
	if err != nil {
		t.Fatalf("decodeHeader: %v", err)
	}

	if keyID != "v2" {
		t.Fatalf("active key id = %q, want v2", keyID)
	}
}

func TestAESGCMUnknownKeyID(t *testing.T) {
	producer, err := NewAESGCM("v1", testKey("v1", 0x01))
	if err != nil {
		t.Fatalf("NewAESGCM producer: %v", err)
	}

	ctx := context.Background()

	ciphertext, err := producer.Encrypt(ctx, []byte("secret"))
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}

	// A consumer that holds a different keyring cannot find v1.
	consumer, err := NewAESGCM("v2", testKey("v2", 0x02))
	if err != nil {
		t.Fatalf("NewAESGCM consumer: %v", err)
	}

	_, err = consumer.Decrypt(ctx, ciphertext)
	if !errors.Is(err, ErrUnknownKeyID) {
		t.Fatalf("Decrypt error = %v, want ErrUnknownKeyID", err)
	}
}

func TestAESGCMTamperedCiphertextFails(t *testing.T) {
	enc, err := NewAESGCM("primary", testKey("primary", 0x01))
	if err != nil {
		t.Fatalf("NewAESGCM: %v", err)
	}

	ctx := context.Background()

	ciphertext, err := enc.Encrypt(ctx, []byte("authentic payload"))
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}

	// Flipping the final byte corrupts the sealed body and its tag.
	tampered := bytes.Clone(ciphertext)
	tampered[len(tampered)-1] ^= 0xff

	_, err = enc.Decrypt(ctx, tampered)
	if !errors.Is(err, ErrAuthentication) {
		t.Fatalf("Decrypt error = %v, want ErrAuthentication", err)
	}
}

func TestAESGCMTamperedHeaderFails(t *testing.T) {
	// Two keys with swappable ids: a header rewritten from v1 to v2 must still
	// fail, because the header is authenticated as additional data.
	enc, err := NewAESGCM("v1", testKey("v1", 0x01), testKey("v2", 0x02))
	if err != nil {
		t.Fatalf("NewAESGCM: %v", err)
	}

	ctx := context.Background()

	ciphertext, err := enc.Encrypt(ctx, []byte("bound to its key id"))
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}

	keyID, headerLen, err := decodeHeader(ciphertext)
	if err != nil {
		t.Fatalf("decodeHeader: %v", err)
	}

	if keyID != "v1" || headerLen != headerFixedBytes+len("v1") {
		t.Fatalf("unexpected header: id=%q len=%d", keyID, headerLen)
	}

	// Rewrite the key id in place from "v1" to "v2"; same length, valid key.
	tampered := bytes.Clone(ciphertext)
	tampered[headerLen-1] = '2'

	_, err = enc.Decrypt(ctx, tampered)
	if !errors.Is(err, ErrAuthentication) {
		t.Fatalf("Decrypt error = %v, want ErrAuthentication", err)
	}
}

func TestAESGCMMalformedCiphertext(t *testing.T) {
	enc, err := NewAESGCM("primary", testKey("primary", 0x01))
	if err != nil {
		t.Fatalf("NewAESGCM: %v", err)
	}

	ctx := context.Background()

	tests := []struct {
		name       string
		ciphertext []byte
	}{
		{name: "empty", ciphertext: []byte{}},
		{name: "header only fixed bytes", ciphertext: []byte{schemeVersion, 7}},
		{name: "unsupported version", ciphertext: []byte{schemeVersion + 1, 0}},
		{name: "truncated key id", ciphertext: []byte{schemeVersion, 4, 'p', 'r'}},
		{name: "missing nonce", ciphertext: append([]byte{schemeVersion, byte(len("primary"))}, []byte("primary")...)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := enc.Decrypt(ctx, test.ciphertext)
			if !errors.Is(err, ErrMalformedCiphertext) {
				t.Fatalf("Decrypt error = %v, want ErrMalformedCiphertext", err)
			}
		})
	}
}

func TestAESGCMConcurrentUse(t *testing.T) {
	enc, err := NewAESGCM("primary", testKey("primary", 0x01))
	if err != nil {
		t.Fatalf("NewAESGCM: %v", err)
	}

	ctx := context.Background()
	plaintext := []byte("shared across goroutines")

	const goroutines = 32

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for range goroutines {
		go func() {
			defer wg.Done()

			ciphertext, err := enc.Encrypt(ctx, plaintext)
			if err != nil {
				t.Errorf("Encrypt: %v", err)

				return
			}

			opened, err := enc.Decrypt(ctx, ciphertext)
			if err != nil {
				t.Errorf("Decrypt: %v", err)

				return
			}

			if !bytes.Equal(opened, plaintext) {
				t.Errorf("round trip = %q, want %q", opened, plaintext)
			}
		}()
	}

	wg.Wait()
}
