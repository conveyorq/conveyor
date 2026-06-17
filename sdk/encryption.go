// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package conveyor

import (
	"context"
	"fmt"
	"maps"

	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
)

// Encryption metadata marker. A client sets it when it seals a payload; a
// worker decrypts only payloads carrying it, so encrypted and plaintext tasks
// can share a queue. It travels in the task metadata map, never the wire
// contract, so it needs no proto change.
const (
	// encryptionMarkerKey is the task-metadata key set on an encrypted payload.
	encryptionMarkerKey = "conveyor.encryption"
	// encryptionMarkerValue identifies the framing version, leaving room for the
	// scheme to evolve without ambiguity.
	encryptionMarkerValue = "1"
)

// withEncryptionMarker returns a copy of metadata carrying the encryption
// marker, leaving the caller's map untouched so the same Task can be enqueued
// again.
func withEncryptionMarker(metadata map[string]string) map[string]string {
	marked := make(map[string]string, len(metadata)+1)
	maps.Copy(marked, metadata)
	marked[encryptionMarkerKey] = encryptionMarkerValue

	return marked
}

// openEnvelope decrypts a dispatched envelope's payload in place when it
// carries the encryption marker, then drops the marker so handlers see only
// their own metadata. An unmarked envelope is returned unchanged. A marked
// envelope fails — and the execution is reported, never run — when the worker
// has no Encryptor or the payload does not decrypt, so a worker never hands
// ciphertext to a handler.
func (s *workerSession) openEnvelope(ctx context.Context, envelope *conveyorv1.TaskEnvelope) error {
	metadata := envelope.GetMetadata()
	if metadata[encryptionMarkerKey] == "" {
		return nil
	}

	if s.encryptor == nil {
		return fmt.Errorf("conveyor: task %q is encrypted but the worker has no Encryptor (WithEncryption)", envelope.GetId())
	}

	plaintext, err := s.encryptor.Decrypt(ctx, envelope.GetPayload())
	if err != nil {
		return fmt.Errorf("conveyor: decrypting task %q payload: %w", envelope.GetId(), err)
	}

	envelope.Payload = plaintext
	delete(metadata, encryptionMarkerKey)

	return nil
}
