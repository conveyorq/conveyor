// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package encryption

import (
	"context"
	"errors"
	"iter"
	"testing"

	"github.com/stretchr/testify/require"
)

// failingEncryptor fails every call with err.
type failingEncryptor struct {
	// err is returned by Encrypt and Decrypt.
	err error
}

func (f failingEncryptor) Encrypt(context.Context, []byte) ([]byte, error) { return nil, f.err }

func (f failingEncryptor) Decrypt(context.Context, []byte) ([]byte, error) { return nil, f.err }

// newChunkTestEncryptor returns an AES-GCM encryptor for chunk tests.
func newChunkTestEncryptor(t *testing.T) *AESGCM {
	t.Helper()

	encryptor, err := NewAESGCM("primary", testKey("primary", 0x01))
	require.NoError(t, err)

	return encryptor
}

// sealChunks seals each data slice as one chunk, marking the final one last.
func sealChunks(t *testing.T, encryptor Encryptor, chunks ...[]byte) [][]byte {
	t.Helper()

	sealer := NewChunkSealer(encryptor)
	sealed := make([][]byte, 0, len(chunks))

	for index, data := range chunks {
		chunk, err := sealer.Seal(context.Background(), data, index == len(chunks)-1)
		require.NoError(t, err)

		sealed = append(sealed, chunk)
	}

	return sealed
}

func TestChunkRoundTrip(t *testing.T) {
	encryptor := newChunkTestEncryptor(t)
	sealed := sealChunks(t, encryptor, []byte("alpha"), []byte("beta"), []byte(""))

	opener := NewChunkOpener(encryptor)

	var opened []string

	for _, chunk := range sealed {
		data, err := opener.Open(context.Background(), chunk)
		require.NoError(t, err)

		opened = append(opened, string(data))
	}

	require.Equal(t, []string{"alpha", "beta", ""}, opened)
	require.NoError(t, opener.Finish())
}

func TestChunkOpenerRejectsReorder(t *testing.T) {
	encryptor := newChunkTestEncryptor(t)
	sealed := sealChunks(t, encryptor, []byte("first"), []byte("second"))

	_, err := NewChunkOpener(encryptor).Open(context.Background(), sealed[1])
	require.ErrorIs(t, err, ErrChunkOrder)
}

func TestChunkOpenerRejectsChunkAfterLast(t *testing.T) {
	encryptor := newChunkTestEncryptor(t)
	only := sealChunks(t, encryptor, []byte("only"))
	opener := NewChunkOpener(encryptor)

	_, err := opener.Open(context.Background(), only[0])
	require.NoError(t, err)

	_, err = opener.Open(context.Background(), only[0])
	require.ErrorIs(t, err, ErrChunkOrder)
}

func TestChunkOpenerDetectsTruncation(t *testing.T) {
	encryptor := newChunkTestEncryptor(t)
	sealed := sealChunks(t, encryptor, []byte("first"), []byte("second"))
	opener := NewChunkOpener(encryptor)

	_, err := opener.Open(context.Background(), sealed[0])
	require.NoError(t, err)
	require.ErrorIs(t, opener.Finish(), ErrChunkTruncated)
}

func TestChunkOpenerWithNoChunksFinishes(t *testing.T) {
	require.NoError(t, NewChunkOpener(newChunkTestEncryptor(t)).Finish())
}

func TestChunkOpenerRejectsShortPlaintext(t *testing.T) {
	encryptor := newChunkTestEncryptor(t)

	unframed, err := encryptor.Encrypt(context.Background(), []byte("short"))
	require.NoError(t, err)

	_, err = NewChunkOpener(encryptor).Open(context.Background(), unframed)
	require.ErrorIs(t, err, ErrMalformedCiphertext)
}

func TestChunkSealerAndOpenerPropagateEncryptorErrors(t *testing.T) {
	failure := errors.New("kms down")
	broken := failingEncryptor{err: failure}

	_, err := NewChunkSealer(broken).Seal(context.Background(), []byte("data"), true)
	require.ErrorIs(t, err, failure)

	_, err = NewChunkOpener(broken).Open(context.Background(), []byte("sealed"))
	require.ErrorIs(t, err, failure)
}

func TestChunkOpenerFromOpensARunMidPayload(t *testing.T) {
	encryptor := newChunkTestEncryptor(t)
	sealed := sealChunks(t, encryptor, []byte("first"), []byte("second"), []byte("third"))

	opener := NewChunkOpenerFrom(encryptor, 1)

	data, err := opener.Open(context.Background(), sealed[1])
	require.NoError(t, err)
	require.Equal(t, "second", string(data))
	require.ErrorIs(t, opener.Finish(), ErrChunkTruncated, "a run that stops before the last chunk is incomplete")

	_, err = NewChunkOpenerFrom(encryptor, 1).Open(context.Background(), sealed[0])
	require.ErrorIs(t, err, ErrChunkOrder)

	require.NoError(t, NewChunkOpenerFrom(encryptor, 2).Finish(), "an opener that opened nothing has nothing to finish")
}

// plainChunks yields the given chunks in order.
func plainChunks(chunks ...[]byte) iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {
		for _, chunk := range chunks {
			if !yield(chunk, nil) {
				return
			}
		}
	}
}

// openAll opens a sealed sequence in order and checks it ends on the last flag.
func openAll(t *testing.T, encryptor Encryptor, sealed iter.Seq2[[]byte, error]) ([][]byte, error) {
	t.Helper()

	opener := NewChunkOpener(encryptor)

	var opened [][]byte

	for chunk, err := range sealed {
		if err != nil {
			return opened, err
		}

		data, openErr := opener.Open(context.Background(), chunk)
		require.NoError(t, openErr)

		opened = append(opened, data)
	}

	return opened, opener.Finish()
}

func TestSealChunksMarksTheFinalChunkLast(t *testing.T) {
	encryptor := newChunkTestEncryptor(t)

	opened, err := openAll(t, encryptor, SealChunks(context.Background(), encryptor, plainChunks([]byte("a"), []byte("b"))))
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("a"), []byte("b")}, opened)
}

func TestSealChunksSealsAnEmptyPayloadAsOneChunk(t *testing.T) {
	encryptor := newChunkTestEncryptor(t)

	opened, err := openAll(t, encryptor, SealChunks(context.Background(), encryptor, plainChunks()))
	require.NoError(t, err)
	require.Equal(t, [][]byte{{}}, opened)
}

func TestSealChunksYieldsSourceAndSealingErrors(t *testing.T) {
	failure := errors.New("source broken")
	broken := func(yield func([]byte, error) bool) {
		if yield([]byte("first"), nil) {
			yield(nil, failure)
		}
	}

	_, err := openAll(t, newChunkTestEncryptor(t), SealChunks(context.Background(), newChunkTestEncryptor(t), broken))
	require.ErrorIs(t, err, failure)

	kmsDown := errors.New("kms down")
	for _, err := range SealChunks(context.Background(), failingEncryptor{err: kmsDown}, plainChunks([]byte("a"))) {
		require.ErrorIs(t, err, kmsDown)
	}
}
