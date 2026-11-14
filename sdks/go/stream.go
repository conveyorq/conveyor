// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package conveyor

import (
	"bytes"
	"context"
	"errors"
	"io"
	"iter"
	"sync/atomic"

	"github.com/conveyorq/conveyor/encryption"
	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
)

// inlinePayloadBytes is the largest payload the server accepts inline on
// Enqueue. A larger payload, or one supplied as a reader, is uploaded in
// chunks through EnqueueStream instead. It mirrors the server's fixed inline
// limit and is restated here so SDK consumers never import the server.
const inlinePayloadBytes = 1 << 20

// payloadChunkBytes is the plaintext size of one uploaded chunk. The server
// admits chunks up to 2 MiB, leaving room for encryption overhead.
const payloadChunkBytes = 1 << 20

// defaultPayloadChunkWindow is the per-task chunk window assumed when the
// server's Welcome advertises none.
const defaultPayloadChunkWindow = 16

// errSessionEnded ends a payload still arriving when its session ends.
var errSessionEnded = errors.New("conveyor: session ended before the payload arrived")

// errChunkWindowExceeded ends a streamed payload whose server sent more chunks
// than the advertised window allows.
var errChunkWindowExceeded = errors.New("conveyor: server exceeded the payload chunk window")

// errServerPayloadRead prefixes the failure reported when the server ends a
// payload early because it could not read it.
var errServerPayloadRead = errors.New("conveyor: server could not read the payload")

// uploadChunks yields a payload as upload chunks of at most payloadChunkBytes,
// read from reader when it is set and from data otherwise. With an encryptor,
// each chunk is sealed on its own (see encryption.ChunkSealer), and an empty
// payload is still sent as one sealed chunk so its end is authenticated.
func uploadChunks(ctx context.Context, data []byte, reader io.Reader, encryptor encryption.Encryptor) iter.Seq2[[]byte, error] {
	plain := bytesChunks(data)
	if reader != nil {
		plain = readerChunks(reader)
	}

	if encryptor == nil {
		return plain
	}

	return encryption.SealChunks(ctx, encryptor, plain)
}

// bytesChunks splits data into payloadChunkBytes slices without copying.
func bytesChunks(data []byte) iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {
		for offset := 0; offset < len(data); offset += payloadChunkBytes {
			if !yield(data[offset:min(offset+payloadChunkBytes, len(data))], nil) {
				return
			}
		}
	}
}

// readerChunks reads reader to its end in payloadChunkBytes chunks, each in a
// fresh buffer, yielding a read failure as the final element.
func readerChunks(reader io.Reader) iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {
		for {
			buffer := make([]byte, payloadChunkBytes)
			read, err := io.ReadFull(reader, buffer)

			if read > 0 && !yield(buffer[:read], nil) {
				return
			}

			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return
			}

			if err != nil {
				yield(nil, err)

				return
			}
		}
	}
}

// chunkStream is the io.Reader a streaming handler reads a chunked payload
// from. The session's receive loop pushes chunks as they arrive. The server
// sends at most the advertised chunk window of unacknowledged chunks per task,
// and the buffer is sized to that window, so a push never waits: the session
// keeps reading its stream however slowly the handler reads. Each chunk the
// handler takes out is acknowledged, granting the server one more. A handler
// that returns without reading to the end abandons the stream, and later
// chunks are dropped.
type chunkStream struct {
	// chunks carries pushed chunks to the reader; closed by end.
	chunks chan []byte
	// abandoned records that the handler is done with the stream.
	abandoned atomic.Bool
	// ack acknowledges one chunk the reader took out of the buffer.
	ack func()
	// err is the failure the reader reports after the last chunk; written by
	// end before chunks is closed.
	err error
	// current is the unread rest of the chunk being read.
	current []byte
}

// newChunkStream returns an empty stream buffering window chunks, calling ack
// for each chunk the reader takes.
func newChunkStream(window int, ack func()) *chunkStream {
	return &chunkStream{chunks: make(chan []byte, window), ack: ack}
}

// push hands one chunk to the reader without waiting. It reports false when
// the buffer is full and the reader still wants chunks: the server sent more
// than its window allows.
func (c *chunkStream) push(data []byte) bool {
	select {
	case c.chunks <- data:
		return true
	default:
		return c.abandoned.Load()
	}
}

// end closes the stream: the reader sees err after the last chunk, or io.EOF
// when err is nil. It is called once, by the receive loop.
func (c *chunkStream) end(err error) {
	c.err = err
	close(c.chunks)
}

// Read implements io.Reader.
func (c *chunkStream) Read(p []byte) (int, error) {
	for len(c.current) == 0 {
		data, open := <-c.chunks
		if !open {
			if c.err != nil {
				return 0, c.err
			}

			return 0, io.EOF
		}

		c.ack()
		c.current = data
	}

	read := copy(p, c.current)
	c.current = c.current[read:]

	return read, nil
}

// abandon marks the stream as no longer read, so later pushes are dropped.
func (c *chunkStream) abandon() {
	c.abandoned.Store(true)
}

// payloadAssembly collects one chunked payload as its PayloadChunk frames
// arrive, either into a buffer handed to complete or into a streaming
// handler's reader. Only the session's receive loop touches it.
type payloadAssembly struct {
	// opener decrypts each chunk of an end-to-end encrypted payload; nil for
	// plaintext.
	opener *encryption.ChunkOpener
	// buffer collects the payload when stream is nil.
	buffer bytes.Buffer
	// stream feeds a streaming handler; nil when the payload is buffered.
	stream *chunkStream
	// ack acknowledges one buffered chunk, so the server sends the next; a
	// streamed chunk is acknowledged by its reader instead.
	ack func()
	// complete receives the buffered payload, or the failure that ended it.
	complete func(payload []byte, err error)
}

// add applies one chunk frame and reports whether the payload is finished,
// successfully or not.
func (a *payloadAssembly) add(ctx context.Context, chunk *conveyorv1.PayloadChunk) bool {
	if chunk.GetError() != "" {
		a.finish(nil, errors.Join(errServerPayloadRead, errors.New(chunk.GetError())))

		return true
	}

	if chunk.GetLast() {
		var err error
		if a.opener != nil {
			err = a.opener.Finish()
		}

		a.finish(a.buffer.Bytes(), err)

		return true
	}

	data := chunk.GetData()

	if a.opener != nil {
		opened, err := a.opener.Open(ctx, data)
		if err != nil {
			a.finish(nil, err)

			return true
		}

		data = opened
	}

	if a.stream != nil {
		if !a.stream.push(data) {
			a.finish(nil, errChunkWindowExceeded)

			return true
		}

		return false
	}

	a.buffer.Write(data)
	a.ack()

	return false
}

// finish ends the payload: a streamed one through its reader, a buffered one
// through complete.
func (a *payloadAssembly) finish(payload []byte, err error) {
	if a.stream != nil {
		a.stream.end(err)

		return
	}

	if err != nil {
		payload = nil
	}

	a.complete(payload, err)
}
