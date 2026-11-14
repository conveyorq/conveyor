# Copyright 2026 ConveyorQ
#
# SPDX-License-Identifier: Apache-2.0

import asyncio
import io
from datetime import timedelta

import grpc
import pytest

from conveyorq import (
    ChunkTruncatedError,
    Client,
    ContentType,
    ConveyorError,
    DuplicateTaskError,
    Key,
    PayloadStream,
    SyncClient,
    TxTask,
    binary,
    new_aes_gcm,
    new_task,
    stream,
)
from conveyorq.client import _resolve_unique_key
from conveyorq.encryption import ENCRYPTION_MARKER_KEY, ENCRYPTION_MARKER_VALUE, ChunkOpener, ChunkSealer
from conveyorq.gen.conveyor.v1 import service_pb2
from conveyorq.options import EnqueueOptions
from conveyorq.streaming import (
    DEFAULT_PAYLOAD_CHUNK_WINDOW,
    INLINE_PAYLOAD_BYTES,
    PAYLOAD_CHUNK_BYTES,
    ChunkStream,
    PayloadAssembly,
    upload_chunks,
)
from conveyorq.task import Task

# A payload above the inline limit, spanning several upload chunks with a
# partial last one.
LARGE_PAYLOAD_BYTES = 3 * PAYLOAD_CHUNK_BYTES + 12345

# How many upload chunks LARGE_PAYLOAD_BYTES splits into.
LARGE_PAYLOAD_CHUNKS = 4

# How long a test waits for something that must happen promptly.
PROMPT_SECONDS = 5


class BrokenSourceError(Exception):
    """The failure of a source that breaks mid-payload."""


def large_payload() -> bytes:
    return bytes(index % 251 for index in range(LARGE_PAYLOAD_BYTES))


def codec():
    return new_aes_gcm("k1", Key("k1", bytes([1]) * 32))


async def collect(chunks) -> list:
    return [chunk async for chunk in chunks]


async def pieces(*parts: bytes):
    for part in parts:
        yield part


async def broken_source():
    yield b"partial"
    raise BrokenSourceError("source broken")


class ShortReader:
    """A file-like object returning at most ``limit`` bytes per read."""

    def __init__(self, data: bytes, limit: int) -> None:
        self._buffer = io.BytesIO(data)
        self._limit = limit

    def read(self, size: int) -> bytes:
        return self._buffer.read(min(size, self._limit))


def open_all(opener: ChunkOpener, sealed: list) -> bytes:
    opened = b"".join(opener.open(chunk) for chunk in sealed)
    opener.finish()

    return opened


async def test_upload_chunks_splits_bytes():
    payload = large_payload()

    chunks = await collect(upload_chunks(payload, None, None))

    assert len(chunks) == LARGE_PAYLOAD_CHUNKS
    assert all(len(chunk) <= PAYLOAD_CHUNK_BYTES for chunk in chunks)
    assert b"".join(chunks) == payload


async def test_upload_chunks_reads_a_file_like_source_in_full_chunks():
    payload = large_payload()

    chunks = await collect(upload_chunks(b"", ShortReader(payload, 1000), None))

    assert [len(chunk) for chunk in chunks[:-1]] == [PAYLOAD_CHUNK_BYTES] * (LARGE_PAYLOAD_CHUNKS - 1)
    assert b"".join(chunks) == payload


async def test_upload_chunks_rechunks_an_async_iterable():
    payload = large_payload()
    parts = [payload[offset : offset + 300_000] for offset in range(0, len(payload), 300_000)]

    chunks = await collect(upload_chunks(b"", pieces(*parts), None))

    assert len(chunks) == LARGE_PAYLOAD_CHUNKS
    assert b"".join(chunks) == payload


async def test_upload_chunks_sends_nothing_for_an_empty_plaintext_payload():
    assert await collect(upload_chunks(b"", io.BytesIO(b""), None)) == []


async def test_upload_chunks_seals_each_chunk():
    encryptor = codec()
    payload = large_payload()

    sealed = await collect(upload_chunks(payload, None, encryptor))

    assert len(sealed) == LARGE_PAYLOAD_CHUNKS
    assert open_all(ChunkOpener(encryptor), sealed) == payload


async def test_upload_chunks_seals_an_empty_payload_as_one_last_chunk():
    encryptor = codec()

    sealed = await collect(upload_chunks(b"", pieces(), encryptor))

    assert len(sealed) == 1
    assert open_all(ChunkOpener(encryptor), sealed) == b""


@pytest.mark.parametrize("encrypted", [False, True])
async def test_upload_chunks_reports_a_source_failure(encrypted):
    with pytest.raises(BrokenSourceError):
        await collect(upload_chunks(b"", broken_source(), codec() if encrypted else None))


@pytest.mark.parametrize("source", [io.StringIO("text"), pieces(b"ok", "text")])  # type: ignore[arg-type]
async def test_upload_chunks_rejects_text(source):
    with pytest.raises(ConveyorError, match="must yield bytes"):
        await collect(upload_chunks(b"", source, None))


def test_stream_accepts_file_like_and_async_iterable_sources():
    reader = io.BytesIO(b"data")

    payload = stream(reader)

    assert payload.source is reader
    assert payload.data == b""
    assert payload.content_type == ContentType.OCTET_STREAM
    assert stream(pieces(), "text/csv").content_type == "text/csv"


def test_stream_rejects_a_value_that_is_not_a_source():
    with pytest.raises(ConveyorError):
        stream(b"bytes are not a stream")  # type: ignore[arg-type]


async def test_payload_stream_yields_an_inline_payload_once():
    assert [chunk async for chunk in PayloadStream(b"inline")] == [b"inline"]
    assert list(PayloadStream(b"inline")) == [b"inline"]
    assert await PayloadStream(b"inline").read() == b"inline"
    assert list(PayloadStream(b"")) == []


class AckCounter:
    """Counts the chunk acknowledgements a stream or assembly sends."""

    def __init__(self) -> None:
        self.count = 0

    def __call__(self) -> None:
        self.count += 1


def chunk_stream(window: int = DEFAULT_PAYLOAD_CHUNK_WINDOW) -> "tuple[ChunkStream, AckCounter]":
    acks = AckCounter()

    return ChunkStream(window, acks), acks


async def test_chunk_stream_reads_chunks_then_ends_acking_each():
    chunks, acks = chunk_stream()
    assert chunks.push(b"hello ")
    assert chunks.push(b"world")
    chunks.end(None)

    assert acks.count == 0
    assert await chunks.read() == b"hello world"
    assert acks.count == 2


async def test_chunk_stream_raises_the_end_error_after_its_chunks():
    chunks, _ = chunk_stream()
    chunks.push(b"partial")
    chunks.end(BrokenSourceError("broken"))

    assert await chunks.__anext__() == b"partial"

    with pytest.raises(BrokenSourceError):
        await chunks.__anext__()


async def test_chunk_stream_waits_for_a_chunk_that_has_not_arrived():
    chunks, _ = chunk_stream()
    reader = asyncio.ensure_future(chunks.read())
    await asyncio.sleep(0)

    chunks.push(b"late")
    chunks.end(None)

    assert await asyncio.wait_for(reader, PROMPT_SECONDS) == b"late"


async def test_chunk_stream_never_waits_on_push():
    chunks, acks = chunk_stream(window=2)

    assert chunks.push(b"one")
    assert chunks.push(b"two")
    assert not chunks.push(b"over the window")

    assert await chunks.__anext__() == b"one"
    assert acks.count == 1
    assert chunks.push(b"three")


async def test_chunk_stream_drops_chunks_once_abandoned():
    chunks, acks = chunk_stream(window=1)
    chunks.push(b"buffered")

    chunks.abandon()

    assert chunks.push(b"dropped")
    assert chunks.push(b"dropped too")
    assert acks.count == 0


async def test_chunk_stream_reads_from_a_handler_thread_acking_each():
    chunks, acks = chunk_stream()
    reader = asyncio.get_running_loop().run_in_executor(None, lambda: b"".join(chunks))

    chunks.push(b"from ")
    chunks.push(b"a thread")
    chunks.end(None)

    assert await asyncio.wait_for(reader, PROMPT_SECONDS) == b"from a thread"
    assert acks.count == 2


async def test_chunk_stream_refuses_a_blocking_read_on_the_event_loop():
    chunks, _ = chunk_stream()

    with pytest.raises(ConveyorError, match="async for"):
        next(iter(chunks))


def completion():
    """Return a recorder for a buffered payload's completion and its callback."""
    record: dict = {}

    def complete(payload, error):
        record["payload"] = payload
        record["error"] = error

    return record, complete


def test_payload_assembly_buffers_until_last_acking_each_chunk():
    record, complete = completion()
    acks = AckCounter()
    assembly = PayloadAssembly(None, complete=complete, ack=acks)

    assert assembly.add(service_pb2.PayloadChunk(data=b"ab")) is False
    assert assembly.add(service_pb2.PayloadChunk(data=b"cd")) is False
    assert acks.count == 2
    assert assembly.add(service_pb2.PayloadChunk(last=True)) is True
    assert record == {"payload": b"abcd", "error": None}
    assert acks.count == 2


def test_payload_assembly_reports_a_server_error():
    record, complete = completion()
    assembly = PayloadAssembly(None, complete=complete)

    assert assembly.add(service_pb2.PayloadChunk(last=True, error="store down")) is True
    assert record["payload"] == b""
    assert "could not read the payload: store down" in str(record["error"])


def test_payload_assembly_decrypts_chunks():
    encryptor = codec()
    sealer = ChunkSealer(encryptor)
    record, complete = completion()
    assembly = PayloadAssembly(ChunkOpener(encryptor), complete=complete)

    assembly.add(service_pb2.PayloadChunk(data=sealer.seal(b"first ", last=False)))
    assembly.add(service_pb2.PayloadChunk(data=sealer.seal(b"second", last=True)))
    assembly.add(service_pb2.PayloadChunk(last=True))

    assert record == {"payload": b"first second", "error": None}


def test_payload_assembly_detects_truncation():
    encryptor = codec()
    record, complete = completion()
    assembly = PayloadAssembly(ChunkOpener(encryptor), complete=complete)

    assembly.add(service_pb2.PayloadChunk(data=ChunkSealer(encryptor).seal(b"first", last=False)))

    assert assembly.add(service_pb2.PayloadChunk(last=True)) is True
    assert isinstance(record["error"], ChunkTruncatedError)
    assert record["payload"] == b""


def test_payload_assembly_reports_an_undecryptable_chunk():
    record, complete = completion()
    assembly = PayloadAssembly(ChunkOpener(codec()), complete=complete)

    assert assembly.add(service_pb2.PayloadChunk(data=b"not ciphertext")) is True
    assert record["error"] is not None


async def test_payload_assembly_feeds_a_stream_which_acks_on_read():
    chunks, acks = chunk_stream()
    assembly = PayloadAssembly(None, stream=chunks)

    assert assembly.add(service_pb2.PayloadChunk(data=b"streamed")) is False
    assert assembly.add(service_pb2.PayloadChunk(last=True)) is True
    assert acks.count == 0
    assert await chunks.read() == b"streamed"
    assert acks.count == 1


async def test_payload_assembly_fails_a_stream_over_the_window():
    chunks, _ = chunk_stream(window=1)
    assembly = PayloadAssembly(None, stream=chunks)

    assert assembly.add(service_pb2.PayloadChunk(data=b"within")) is False
    assert assembly.add(service_pb2.PayloadChunk(data=b"over")) is True

    assert await chunks.__anext__() == b"within"

    with pytest.raises(ConveyorError, match="chunk window"):
        await chunks.__anext__()


async def test_payload_assembly_abort_ends_the_payload_with_an_error():
    record, complete = completion()
    PayloadAssembly(None, complete=complete).abort()

    assert "session ended" in str(record["error"])

    chunks, _ = chunk_stream()
    PayloadAssembly(None, stream=chunks).abort()

    with pytest.raises(ConveyorError, match="session ended"):
        await chunks.read()


def test_task_payload_stream_falls_back_to_the_payload():
    task = Task(type="t", content_type=ContentType.OCTET_STREAM, data=b"inline")

    assert list(task.payload_stream) == [b"inline"]


def test_task_is_streamed_for_a_source_or_a_payload_above_the_inline_limit():
    assert new_task("t", stream(io.BytesIO(b"")))._streamed()
    assert new_task("t", binary(bytes(INLINE_PAYLOAD_BYTES + 1)))._streamed()
    assert not new_task("t", binary(bytes(INLINE_PAYLOAD_BYTES)))._streamed()


def test_unique_on_a_streamed_payload_needs_an_explicit_key():
    task = new_task("t", stream(io.BytesIO(b"")))

    with pytest.raises(ConveyorError, match="unique_key"):
        _resolve_unique_key(task, EnqueueOptions(unique=timedelta(hours=1)))

    assert _resolve_unique_key(task, EnqueueOptions(unique=timedelta(hours=1), unique_key="k")) == "k"


def test_unique_on_a_large_in_memory_payload_derives_its_key():
    task = new_task("t", binary(large_payload()))

    assert _resolve_unique_key(task, EnqueueOptions(unique=timedelta(hours=1))) != ""


@pytest.mark.parametrize(
    "payload", [lambda: binary(large_payload()), lambda: stream(io.BytesIO(b"small"))]
)
async def test_enqueue_tx_rejects_a_streamed_payload(payload):
    client = Client("http://127.0.0.1:9")

    with pytest.raises(ConveyorError, match="enqueue_tx"):
        await client.enqueue_tx([TxTask(new_task("t", payload()))])


class FakeUploadCall:
    """A stream-unary call recording the frames written to it.

    ``answer_after`` makes writes fail once that many frames were written, as a
    server that answered early does; ``error`` makes awaiting the call raise.
    """

    def __init__(self, answer_after=None, error=None) -> None:
        self.frames: list = []
        self.closed = False
        self.cancelled = False
        self._answer_after = answer_after
        self._error = error

    async def write(self, frame) -> None:
        if self._answer_after is not None and len(self.frames) >= self._answer_after:
            raise asyncio.InvalidStateError("rpc already finished")

        self.frames.append(frame)

    async def done_writing(self) -> None:
        self.closed = True

    def cancel(self) -> bool:
        self.cancelled = True

        return True

    def __await__(self):
        return self._response().__await__()

    async def _response(self):
        if self._error is not None:
            raise self._error

        header = self.frames[0].header
        size = sum(len(chunk) for chunk in self.chunks())

        return service_pb2.EnqueueResponse(
            task=service_pb2.TaskInfo(id="t1", type=header.type, payload_chunked=True, payload_size=size)
        )

    def chunks(self) -> list:
        return [frame.chunk for frame in self.frames if frame.WhichOneof("frame") == "chunk"]

    def commits(self) -> list:
        return [frame.commit for frame in self.frames if frame.WhichOneof("frame") == "commit"]


class FakeUploadStub:
    """A TaskService stub whose streaming enqueue returns one fake call."""

    def __init__(self, call: FakeUploadCall) -> None:
        self.call = call
        self.unary_requests: list = []

    def EnqueueStream(self, metadata=None):  # noqa: N802 - gRPC method name
        return self.call

    async def Enqueue(self, request, metadata=None):  # noqa: N802 - gRPC method name
        self.unary_requests.append(request)

        return service_pb2.EnqueueResponse(task=service_pb2.TaskInfo(id="inline"))


def client_with(call: FakeUploadCall, **kwargs) -> "tuple[Client, FakeUploadStub]":
    client = Client("http://127.0.0.1:9", **kwargs)
    stub = FakeUploadStub(call)
    client._stub = stub  # type: ignore[assignment]

    return client, stub


async def test_enqueue_uploads_a_large_payload_through_the_stream():
    payload = large_payload()
    call = FakeUploadCall()
    client, stub = client_with(call)

    info = await client.enqueue(new_task("blob", binary(payload)), queue="bulk")

    header = call.frames[0].header
    assert header.type == "blob"
    assert header.queue == "bulk"
    assert header.payload == b""
    assert b"".join(call.chunks()) == payload
    assert call.frames[-1].WhichOneof("frame") == "commit"
    assert call.frames[-1].commit.size == len(payload)
    assert call.closed
    assert stub.unary_requests == []
    assert info.payload_chunked
    assert info.payload_size == len(payload)


async def test_enqueue_sends_a_small_payload_inline():
    client, stub = client_with(FakeUploadCall())

    info = await client.enqueue(new_task("small", binary(b"tiny")))

    assert info.id == "inline"
    assert len(stub.unary_requests) == 1


async def test_enqueue_seals_each_uploaded_chunk_and_marks_the_header():
    encryptor = codec()
    payload = large_payload()
    call = FakeUploadCall()
    client, _ = client_with(call, encryptor=encryptor)

    await client.enqueue(new_task("blob", stream(io.BytesIO(payload))))

    assert call.frames[0].header.metadata[ENCRYPTION_MARKER_KEY] == ENCRYPTION_MARKER_VALUE
    assert open_all(ChunkOpener(encryptor), call.chunks()) == payload
    # The commit states the uploaded (sealed) size, not the plaintext size.
    assert [commit.size for commit in call.commits()] == [sum(len(chunk) for chunk in call.chunks())]


async def test_enqueue_cancels_the_upload_when_the_source_fails():
    call = FakeUploadCall()
    client, _ = client_with(call)

    with pytest.raises(BrokenSourceError):
        await client.enqueue(new_task("blob", stream(broken_source())))

    assert call.cancelled
    assert not call.closed
    assert call.commits() == []


async def test_enqueue_returns_the_answer_of_a_server_that_ended_the_upload_early():
    call = FakeUploadCall(answer_after=2)
    client, _ = client_with(call)

    info = await client.enqueue(new_task("blob", binary(large_payload())), task_id="t1")

    assert info.id == "t1"
    assert len(call.frames) == 2
    assert not call.cancelled


async def test_enqueue_maps_a_server_rejection_of_the_upload():
    rejection = grpc.aio.AioRpcError(
        grpc.StatusCode.ALREADY_EXISTS, grpc.aio.Metadata(), grpc.aio.Metadata(), "duplicate"
    )
    client, _ = client_with(FakeUploadCall(error=rejection))

    with pytest.raises(DuplicateTaskError):
        await client.enqueue(new_task("blob", binary(large_payload())))


def test_sync_client_uploads_a_file_like_stream():
    payload = large_payload()
    call = FakeUploadCall()

    with SyncClient("http://127.0.0.1:9") as client:
        client._client._stub = FakeUploadStub(call)  # type: ignore[assignment]
        info = client.enqueue(new_task("blob", stream(io.BytesIO(payload))))

    assert info.payload_size == len(payload)
    assert b"".join(call.chunks()) == payload


async def test_enqueue_commits_an_empty_streamed_payload_with_size_zero():
    call = FakeUploadCall()
    client, _ = client_with(call)

    await client.enqueue(new_task("blob", stream(io.BytesIO(b""))))

    assert call.chunks() == []
    assert [commit.size for commit in call.commits()] == [0]
