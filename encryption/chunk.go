// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package encryption

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"iter"
)

// chunkHeaderBytes is the plaintext header sealed into every chunk: an 8-byte
// big-endian sequence number followed by a 1-byte last-chunk flag. Sealing the
// header with the data lets the opener detect reordered, dropped, or appended
// chunks, which per-chunk authentication alone would not catch.
const chunkHeaderBytes = 9

// chunkLastFlag marks the final chunk of a payload in the sealed header.
const chunkLastFlag = 1

// Errors returned when opening a chunked payload. Match them with errors.Is.
var (
	// ErrChunkOrder reports a chunk whose sequence number is not the next one
	// expected, or a chunk arriving after the chunk marked last.
	ErrChunkOrder = errors.New("encryption: chunk out of order")

	// ErrChunkTruncated reports a chunked payload that ended before the chunk
	// marked last.
	ErrChunkTruncated = errors.New("encryption: chunked payload truncated")
)

// ChunkSealer seals a chunked payload one chunk at a time, numbering chunks
// from zero. A ChunkSealer is not safe for concurrent use; use one per payload.
//
// The framing is shared by every SDK and the server's at-rest encryption:
// each chunk is Encrypt(sequence || last || data), so any of them can open
// what another sealed.
type ChunkSealer struct {
	// encryptor seals each framed chunk.
	encryptor Encryptor
	// next is the sequence number of the next chunk to seal.
	next uint64
}

// NewChunkSealer returns a sealer for one chunked payload.
func NewChunkSealer(encryptor Encryptor) *ChunkSealer {
	return &ChunkSealer{encryptor: encryptor}
}

// Seal frames and encrypts the next chunk. last must be true on the final
// chunk, and only on it; a payload sealed for streaming always has at least
// one chunk, so an empty payload is sealed as one empty chunk marked last.
func (s *ChunkSealer) Seal(ctx context.Context, data []byte, last bool) ([]byte, error) {
	framed := make([]byte, chunkHeaderBytes+len(data))
	binary.BigEndian.PutUint64(framed, s.next)

	if last {
		framed[chunkHeaderBytes-1] = chunkLastFlag
	}

	copy(framed[chunkHeaderBytes:], data)

	sealed, err := s.encryptor.Encrypt(ctx, framed)
	if err != nil {
		return nil, err
	}

	s.next++

	return sealed, nil
}

// SealChunks seals each chunk of plain with one ChunkSealer, looking one chunk
// ahead so the final chunk carries the last flag; an empty payload is sealed as
// one empty chunk marked last. A source error, or a sealing failure, is yielded
// as is and ends the sequence.
func SealChunks(ctx context.Context, encryptor Encryptor, plain iter.Seq2[[]byte, error]) iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {
		sealer := NewChunkSealer(encryptor)

		var (
			pending []byte
			started bool
		)

		seal := func(data []byte, last bool) bool {
			sealed, err := sealer.Seal(ctx, data, last)
			if err != nil {
				yield(nil, err)

				return false
			}

			return yield(sealed, nil)
		}

		for data, err := range plain {
			if err != nil {
				yield(nil, err)

				return
			}

			if started && !seal(pending, false) {
				return
			}

			pending = data
			started = true
		}

		seal(pending, true)
	}
}

// ChunkOpener opens a chunked payload sealed by ChunkSealer, enforcing that
// chunks arrive in order and end with the chunk marked last. A ChunkOpener is
// not safe for concurrent use; use one per payload.
type ChunkOpener struct {
	// encryptor opens each framed chunk.
	encryptor Encryptor
	// next is the sequence number the next chunk must carry.
	next uint64
	// start is the sequence number of the first chunk this opener expects.
	start uint64
	// done records that the chunk marked last was opened.
	done bool
}

// NewChunkOpener returns an opener for one chunked payload.
func NewChunkOpener(encryptor Encryptor) *ChunkOpener {
	return &ChunkOpener{encryptor: encryptor}
}

// NewChunkOpenerFrom returns an opener for a run of a chunked payload that
// starts at chunk sequence, for a reader that fetches the payload in ranges.
// Finish then only covers chunks opened by this opener.
func NewChunkOpenerFrom(encryptor Encryptor, sequence uint64) *ChunkOpener {
	return &ChunkOpener{encryptor: encryptor, next: sequence, start: sequence}
}

// Open decrypts the next chunk and returns its data. It fails with
// ErrChunkOrder when the chunk is not the next in sequence or follows the
// chunk marked last, and with ErrMalformedCiphertext when the decrypted chunk
// is too short to carry the framing header.
func (o *ChunkOpener) Open(ctx context.Context, sealed []byte) ([]byte, error) {
	if o.done {
		return nil, fmt.Errorf("%w: chunk after the last chunk", ErrChunkOrder)
	}

	framed, err := o.encryptor.Decrypt(ctx, sealed)
	if err != nil {
		return nil, err
	}

	if len(framed) < chunkHeaderBytes {
		return nil, ErrMalformedCiphertext
	}

	sequence := binary.BigEndian.Uint64(framed)
	if sequence != o.next {
		return nil, fmt.Errorf("%w: got chunk %d, want %d", ErrChunkOrder, sequence, o.next)
	}

	o.next++
	o.done = framed[chunkHeaderBytes-1] == chunkLastFlag

	return framed[chunkHeaderBytes:], nil
}

// Finish reports whether the payload ended where it should: after the chunk
// marked last. A payload of no chunks at all is not chunked and passes.
func (o *ChunkOpener) Finish() error {
	if o.next > o.start && !o.done {
		return ErrChunkTruncated
	}

	return nil
}
