// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package conveyor

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"iter"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/conveyorq/conveyor/encryption"
	"github.com/conveyorq/conveyor/internal/broker"
	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
)

// streamTestPayloadCap is the payload cap of the streaming end-to-end server.
const streamTestPayloadCap = 16 << 20

// largePayloadBytes is a payload size above the inline limit, spanning several
// upload chunks with a partial last one.
const largePayloadBytes = 3*payloadChunkBytes + 12345

// errBrokenSource is the failure of a reader that breaks mid-payload.
var errBrokenSource = errors.New("source broken")

// largePayload returns a deterministic payload of largePayloadBytes.
func largePayload() []byte {
	payload := make([]byte, largePayloadBytes)
	for index := range payload {
		payload[index] = byte(index % 251)
	}

	return payload
}

// collectChunks drains a chunk sequence, returning the chunks and its error.
func collectChunks(sequence iter.Seq2[[]byte, error]) ([][]byte, error) {
	var chunks [][]byte

	for data, err := range sequence {
		if err != nil {
			return chunks, err
		}

		chunks = append(chunks, data)
	}

	return chunks, nil
}

// brokenReader returns its data, then errBrokenSource.
type brokenReader struct {
	// data is returned before the failure.
	data []byte
}

// Read implements io.Reader.
func (r *brokenReader) Read(p []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, errBrokenSource
	}

	read := copy(p, r.data)
	r.data = r.data[read:]

	return read, nil
}

// failingEncryptor fails every call.
type failingEncryptor struct{}

func (failingEncryptor) Encrypt(context.Context, []byte) ([]byte, error) {
	return nil, errBrokenSource
}

func (failingEncryptor) Decrypt(context.Context, []byte) ([]byte, error) {
	return nil, errBrokenSource
}

func TestInlinePayloadBytesMatchesTheServer(t *testing.T) {
	require.Equal(t, broker.InlinePayloadBytes, inlinePayloadBytes)
}

func TestUploadChunksSplitsBytes(t *testing.T) {
	payload := largePayload()

	chunks, err := collectChunks(uploadChunks(context.Background(), payload, nil, nil))
	require.NoError(t, err)
	require.Len(t, chunks, 4)
	require.Equal(t, payload, bytes.Join(chunks, nil))
}

func TestUploadChunksReadsReader(t *testing.T) {
	payload := largePayload()

	chunks, err := collectChunks(uploadChunks(context.Background(), nil, bytes.NewReader(payload), nil))
	require.NoError(t, err)
	require.Len(t, chunks, 4)
	require.Equal(t, payload, bytes.Join(chunks, nil))
}

func TestUploadChunksReportsReaderFailure(t *testing.T) {
	_, err := collectChunks(uploadChunks(context.Background(), nil, &brokenReader{data: []byte("partial")}, nil))
	require.ErrorIs(t, err, errBrokenSource)
}

func TestUploadChunksSealsEachChunk(t *testing.T) {
	encryptor := testAESGCM(t, "k1", 0x01)
	payload := largePayload()

	sealed, err := collectChunks(uploadChunks(context.Background(), payload, nil, encryptor))
	require.NoError(t, err)
	require.Len(t, sealed, 4)

	opener := encryption.NewChunkOpener(encryptor)

	var opened bytes.Buffer

	for _, chunk := range sealed {
		data, openErr := opener.Open(context.Background(), chunk)
		require.NoError(t, openErr)
		opened.Write(data)
	}

	require.NoError(t, opener.Finish())
	require.Equal(t, payload, opened.Bytes())
}

func TestUploadChunksSealsAnEmptyPayloadAsOneChunk(t *testing.T) {
	sealed, err := collectChunks(uploadChunks(context.Background(), nil, bytes.NewReader(nil), testAESGCM(t, "k1", 0x01)))
	require.NoError(t, err)
	require.Len(t, sealed, 1)
}

func TestUploadChunksReportsSealFailure(t *testing.T) {
	_, err := collectChunks(uploadChunks(context.Background(), []byte("data"), nil, failingEncryptor{}))
	require.ErrorIs(t, err, errBrokenSource)
}

func TestUploadChunksReportsReaderFailureWhenSealing(t *testing.T) {
	reader := &brokenReader{data: []byte("partial")}

	_, err := collectChunks(uploadChunks(context.Background(), nil, reader, testAESGCM(t, "k1", 0x01)))
	require.ErrorIs(t, err, errBrokenSource)
}

// testChunkWindow is the chunk window of the chunk-stream unit tests.
const testChunkWindow = 4

// countAcks returns an ack function and a counter of its calls.
func countAcks() (func(), *atomic.Int32) {
	var acks atomic.Int32

	return func() { acks.Add(1) }, &acks
}

func TestChunkStreamReadsChunksThenEOF(t *testing.T) {
	ack, acks := countAcks()
	stream := newChunkStream(testChunkWindow, ack)
	require.True(t, stream.push([]byte("hello ")))
	require.True(t, stream.push([]byte("world")))
	stream.end(nil)

	read, err := io.ReadAll(stream)
	require.NoError(t, err)
	require.Equal(t, "hello world", string(read))
	require.EqualValues(t, 2, acks.Load(), "every chunk the reader takes is acknowledged")
}

func TestChunkStreamReportsEndError(t *testing.T) {
	ack, _ := countAcks()
	stream := newChunkStream(testChunkWindow, ack)
	require.True(t, stream.push([]byte("partial")))
	stream.end(errBrokenSource)

	_, err := io.ReadAll(stream)
	require.ErrorIs(t, err, errBrokenSource)
}

func TestChunkStreamNeverWaitsOnPush(t *testing.T) {
	ack, acks := countAcks()
	stream := newChunkStream(testChunkWindow, ack)

	for range testChunkWindow {
		require.True(t, stream.push([]byte("x")))
	}

	require.False(t, stream.push([]byte("over the window")), "a chunk beyond the window is a protocol violation")

	stream.abandon()
	require.True(t, stream.push([]byte("dropped")), "an abandoned stream drops chunks without waiting")
	require.Zero(t, acks.Load(), "unread chunks are never acknowledged")
}

func TestPayloadAssemblyBuffersUntilLast(t *testing.T) {
	var (
		completed []byte
		failure   error
	)

	ack, acks := countAcks()
	assembly := &payloadAssembly{ack: ack, complete: func(payload []byte, err error) { completed, failure = payload, err }}

	require.False(t, assembly.add(context.Background(), &conveyorv1.PayloadChunk{Data: []byte("ab")}))
	require.False(t, assembly.add(context.Background(), &conveyorv1.PayloadChunk{Data: []byte("cd")}))
	require.True(t, assembly.add(context.Background(), &conveyorv1.PayloadChunk{Last: true}))
	require.NoError(t, failure)
	require.Equal(t, "abcd", string(completed))
	require.EqualValues(t, 2, acks.Load(), "a buffered chunk is acknowledged on receipt")
}

func TestPayloadAssemblyReportsServerError(t *testing.T) {
	var failure error

	assembly := &payloadAssembly{complete: func(_ []byte, err error) { failure = err }}

	require.True(t, assembly.add(context.Background(), &conveyorv1.PayloadChunk{Last: true, Error: "store down"}))
	require.ErrorIs(t, failure, errServerPayloadRead)
	require.ErrorContains(t, failure, "store down")
}

func TestPayloadAssemblyDecryptsAndVerifiesChunks(t *testing.T) {
	encryptor := testAESGCM(t, "k1", 0x01)
	sealer := encryption.NewChunkSealer(encryptor)

	first, err := sealer.Seal(context.Background(), []byte("first"), false)
	require.NoError(t, err)

	var failure error

	ack, _ := countAcks()
	assembly := &payloadAssembly{
		opener:   encryption.NewChunkOpener(encryptor),
		ack:      ack,
		complete: func(_ []byte, err error) { failure = err },
	}

	require.False(t, assembly.add(context.Background(), &conveyorv1.PayloadChunk{Data: first}))
	require.True(t, assembly.add(context.Background(), &conveyorv1.PayloadChunk{Last: true}))
	require.ErrorIs(t, failure, encryption.ErrChunkTruncated)
}

func TestPayloadAssemblyReportsUndecryptableChunk(t *testing.T) {
	var failure error

	assembly := &payloadAssembly{
		opener:   encryption.NewChunkOpener(testAESGCM(t, "k1", 0x01)),
		complete: func(_ []byte, err error) { failure = err },
	}

	require.True(t, assembly.add(context.Background(), &conveyorv1.PayloadChunk{Data: []byte("not ciphertext")}))
	require.Error(t, failure)
}

func TestPayloadAssemblyFeedsStream(t *testing.T) {
	ack, _ := countAcks()
	stream := newChunkStream(testChunkWindow, ack)
	assembly := &payloadAssembly{stream: stream}

	require.False(t, assembly.add(context.Background(), &conveyorv1.PayloadChunk{Data: []byte("streamed")}))
	require.True(t, assembly.add(context.Background(), &conveyorv1.PayloadChunk{Last: true}))

	read, err := io.ReadAll(stream)
	require.NoError(t, err)
	require.Equal(t, "streamed", string(read))
}

func TestPayloadAssemblyFailsAStreamOverTheWindow(t *testing.T) {
	ack, _ := countAcks()
	stream := newChunkStream(1, ack)
	assembly := &payloadAssembly{stream: stream}

	require.False(t, assembly.add(context.Background(), &conveyorv1.PayloadChunk{Data: []byte("first")}))
	require.True(t, assembly.add(context.Background(), &conveyorv1.PayloadChunk{Data: []byte("second")}))

	_, err := io.ReadAll(stream)
	require.ErrorIs(t, err, errChunkWindowExceeded)
}

func TestChunkWindowFallsBackWhenUnadvertised(t *testing.T) {
	require.Equal(t, defaultPayloadChunkWindow, chunkWindow(&conveyorv1.Welcome{}))
	require.Equal(t, 8, chunkWindow(&conveyorv1.Welcome{PayloadChunkWindow: 8}))
}

func TestStreamPayloadRequiresReader(t *testing.T) {
	require.Error(t, Stream(nil, ContentTypeBytes).err)
}

func TestTaskPayloadReaderFallsBackToPayload(t *testing.T) {
	task := &Task{payload: []byte("inline")}

	read, err := io.ReadAll(task.PayloadReader())
	require.NoError(t, err)
	require.Equal(t, "inline", string(read))
}

func TestUniqueOnStreamPayloadNeedsKey(t *testing.T) {
	_, _, err := resolveEnqueueOptions(NewTask("t", Stream(bytes.NewReader(nil), ContentTypeBytes)), Unique(time.Hour))
	require.Error(t, err)

	_, key, err := resolveEnqueueOptions(NewTask("t", Stream(bytes.NewReader(nil), ContentTypeBytes)), Unique(time.Hour), UniqueKey("k"))
	require.NoError(t, err)
	require.Equal(t, "k", key)
}

func TestEnqueueTxRejectsStreamedPayload(t *testing.T) {
	client, err := NewClient("http://127.0.0.1:9")
	require.NoError(t, err)

	_, err = client.EnqueueTx(context.Background(), []TxTask{{Task: NewTask("t", Bytes(largePayload()))}})
	require.ErrorContains(t, err, "EnqueueTx")
}

// payloadRecorder records the payload digest each handled task saw.
type payloadRecorder struct {
	// mutex guards digests.
	mutex sync.Mutex
	// digests maps a task id to the SHA-256 of the payload its handler read.
	digests map[string][32]byte
}

// newPayloadRecorder returns an empty recorder.
func newPayloadRecorder() *payloadRecorder {
	return &payloadRecorder{digests: make(map[string][32]byte)}
}

// record stores the digest of payload for a task.
func (r *payloadRecorder) record(taskID string, payload []byte) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	r.digests[taskID] = sha256.Sum256(payload)
}

// digest returns the recorded digest for a task.
func (r *payloadRecorder) digest(taskID string) [32]byte {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	return r.digests[taskID]
}

func TestWorkerReceivesLargePayloadReassembled(t *testing.T) {
	baseURL := startTestServerWithPayloadCap(t, nil, streamTestPayloadCap)
	payload := largePayload()
	recorder := newPayloadRecorder()

	mux := NewMux()
	mux.HandleFunc("blob:process", func(_ context.Context, task *Task) error {
		recorder.record(task.ID(), task.Payload())

		return nil
	})

	runWorker(t, baseURL, mux)

	client, err := NewClient(baseURL)
	require.NoError(t, err)

	info, err := client.Enqueue(context.Background(), NewTask("blob:process", Bytes(payload)), Retention(time.Hour))
	require.NoError(t, err)
	require.True(t, info.PayloadChunked)
	require.EqualValues(t, len(payload), info.PayloadSize)

	awaitTaskState(t, client, info.ID, TaskStateCompleted)
	require.Equal(t, sha256.Sum256(payload), recorder.digest(info.ID))
}

func TestStreamingHandlerReadsStreamedPayload(t *testing.T) {
	baseURL := startTestServerWithPayloadCap(t, nil, streamTestPayloadCap)
	payload := largePayload()
	recorder := newPayloadRecorder()

	mux := NewMux()
	mux.HandleStreamFunc("blob:stream", func(_ context.Context, task *Task) error {
		read, err := io.ReadAll(task.PayloadReader())
		if err != nil {
			return err
		}

		recorder.record(task.ID(), read)

		return nil
	})

	runWorker(t, baseURL, mux)

	client, err := NewClient(baseURL)
	require.NoError(t, err)

	info, err := client.Enqueue(context.Background(), NewTask("blob:stream", Stream(bytes.NewReader(payload), ContentTypeBytes)), Retention(time.Hour))
	require.NoError(t, err)

	awaitTaskState(t, client, info.ID, TaskStateCompleted)
	require.Equal(t, sha256.Sum256(payload), recorder.digest(info.ID))
}

func TestStreamedPayloadEncryptedEndToEnd(t *testing.T) {
	baseURL := startTestServerWithPayloadCap(t, nil, streamTestPayloadCap)
	encryptor := testAESGCM(t, "k1", 0x01)
	payload := largePayload()
	recorder := newPayloadRecorder()

	mux := NewMux()
	mux.HandleFunc("blob:secret", func(_ context.Context, task *Task) error {
		recorder.record(task.ID(), task.Payload())

		return nil
	})
	mux.HandleStreamFunc("blob:secret-stream", func(_ context.Context, task *Task) error {
		read, err := io.ReadAll(task.PayloadReader())
		if err != nil {
			return err
		}

		recorder.record(task.ID(), read)

		return nil
	})

	runWorker(t, baseURL, mux, WithEncryption(encryptor))

	client, err := NewClient(baseURL, WithEncryption(encryptor))
	require.NoError(t, err)

	for _, taskType := range []string{"blob:secret", "blob:secret-stream"} {
		info, enqueueErr := client.Enqueue(context.Background(), NewTask(taskType, Bytes(payload)), Retention(time.Hour))
		require.NoError(t, enqueueErr)

		awaitTaskState(t, client, info.ID, TaskStateCompleted)
		require.Equal(t, sha256.Sum256(payload), recorder.digest(info.ID), "task type %s", taskType)
	}
}

func TestEncryptedStreamedPayloadWithoutWorkerKeyFails(t *testing.T) {
	baseURL := startTestServerWithPayloadCap(t, nil, streamTestPayloadCap)

	mux := NewMux()
	mux.HandleFunc("blob:secret", func(context.Context, *Task) error { return nil })
	runWorker(t, baseURL, mux)

	client, err := NewClient(baseURL, WithEncryption(testAESGCM(t, "k1", 0x01)))
	require.NoError(t, err)

	info, err := client.Enqueue(context.Background(), NewTask("blob:secret", Bytes(largePayload())), Retention(time.Hour))
	require.NoError(t, err)

	awaitTaskState(t, client, info.ID, TaskStateRetry)

	failed, err := client.GetTask(context.Background(), info.ID)
	require.NoError(t, err)
	require.Contains(t, failed.LastError, "no Encryptor")
}

func TestStreamedEnqueueFailsOnBrokenSource(t *testing.T) {
	baseURL := startTestServerWithPayloadCap(t, nil, streamTestPayloadCap)

	client, err := NewClient(baseURL)
	require.NoError(t, err)

	_, err = client.Enqueue(context.Background(), NewTask("blob:broken", Stream(&brokenReader{data: []byte("partial")}, ContentTypeBytes)), TaskID("broken-upload"))
	require.ErrorIs(t, err, errBrokenSource)

	_, err = client.GetTask(context.Background(), "broken-upload")
	require.Error(t, err, "a broken upload must commit nothing")
}

func TestStreamedEnqueueAboveCapIsRejected(t *testing.T) {
	baseURL := startTestServerWithPayloadCap(t, nil, 2*inlinePayloadBytes)

	client, err := NewClient(baseURL)
	require.NoError(t, err)

	_, err = client.Enqueue(context.Background(), NewTask("blob:huge", Bytes(largePayload())))
	require.Error(t, err)
}

func TestWorkerReceivesChunkedBatchMembers(t *testing.T) {
	baseURL := startTestServerWithPayloadCap(t, nil, streamTestPayloadCap)
	payload := largePayload()
	recorder := newPayloadRecorder()

	mux := NewMux()
	mux.HandleBatch("blob:batch", func(_ context.Context, batch []*Task) error {
		for _, task := range batch {
			recorder.record(task.ID(), task.Payload())
		}

		return nil
	})

	runWorker(t, baseURL, mux)

	client, err := NewClient(baseURL)
	require.NoError(t, err)

	var ids []string

	for range 2 {
		info, enqueueErr := client.Enqueue(context.Background(), NewTask("blob:batch", Bytes(payload)), Group("blobs"), Retention(time.Hour))
		require.NoError(t, enqueueErr)

		ids = append(ids, info.ID)
	}

	for _, id := range ids {
		awaitTaskState(t, client, id, TaskStateCompleted)
		require.Equal(t, sha256.Sum256(payload), recorder.digest(id))
	}
}

func TestStreamingHandlerReceivesPayloadLargerThanTheWindow(t *testing.T) {
	const (
		windowsCap   = 32 << 20
		chunkCount   = defaultPayloadChunkWindow + 4
		readInterval = time.Millisecond
	)

	baseURL := startTestServerWithPayloadCap(t, nil, windowsCap)
	payload := bytes.Repeat([]byte{0x5a}, chunkCount*payloadChunkBytes)
	recorder := newPayloadRecorder()

	mux := NewMux()
	mux.HandleStreamFunc("blob:windowed", func(_ context.Context, task *Task) error {
		var read bytes.Buffer

		buffer := make([]byte, payloadChunkBytes)

		for {
			n, err := task.PayloadReader().Read(buffer)
			read.Write(buffer[:n])

			if errors.Is(err, io.EOF) {
				break
			}

			if err != nil {
				return err
			}

			// A slow reader holds back only its own payload, through acks.
			time.Sleep(readInterval)
		}

		recorder.record(task.ID(), read.Bytes())

		return nil
	})

	runWorker(t, baseURL, mux)

	client, err := NewClient(baseURL)
	require.NoError(t, err)

	info, err := client.Enqueue(context.Background(), NewTask("blob:windowed", Bytes(payload)), Retention(time.Hour))
	require.NoError(t, err)

	awaitTaskState(t, client, info.ID, TaskStateCompleted)
	require.Equal(t, sha256.Sum256(payload), recorder.digest(info.ID))
}
