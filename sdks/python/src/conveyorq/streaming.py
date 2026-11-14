# Copyright 2026 ConveyorQ
#
# SPDX-License-Identifier: Apache-2.0

"""Large payload streaming: chunked upload on enqueue and chunked delivery to workers.

A payload above the server's inline limit, or one supplied as a stream, is
uploaded through the streaming enqueue in chunks, so it is never sent whole. A
worker receives such a payload as chunk frames after the task's dispatch, either
reassembled before the handler runs or fed to a streaming handler as it arrives.
"""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import AsyncIterable
from typing import AsyncGenerator, Callable, Deque, List, Optional, Protocol, Union

from .encryption import ChunkOpener, ChunkSealer, Encryptor
from .errors import ConveyorError
from .gen.conveyor.v1 import service_pb2

#: The largest payload the server accepts inline on enqueue. A larger payload,
#: or one supplied as a stream, is uploaded in chunks instead. It mirrors the
#: server's fixed inline limit.
INLINE_PAYLOAD_BYTES = 1 << 20

#: The plaintext size of one uploaded chunk. The server admits chunks up to
#: 2 MiB, which leaves room for encryption overhead.
PAYLOAD_CHUNK_BYTES = 1 << 20

#: The per-task chunk window assumed when the server's Welcome advertises none.
DEFAULT_PAYLOAD_CHUNK_WINDOW = 16

_SESSION_ENDED = "conveyor: session ended before the payload arrived"
_CHUNK_WINDOW_EXCEEDED = "conveyor: server exceeded the payload chunk window"
_SERVER_READ_FAILED = "conveyor: server could not read the payload"


class BinaryReader(Protocol):
    """A binary file-like object: ``read(size)`` returns up to ``size`` bytes,
    and an empty result at the end."""

    def read(self, size: int, /) -> bytes:
        """Read up to ``size`` bytes; return ``b""`` at the end."""
        ...


#: A streamed payload source: an async iterable of byte chunks of any size, or
#: a binary file-like object such as ``open(path, "rb")``.
PayloadSource = Union[AsyncIterable[bytes], BinaryReader]


def is_payload_source(source: object) -> bool:
    """Report whether ``source`` can supply a streamed payload."""
    return isinstance(source, AsyncIterable) or callable(getattr(source, "read", None))


class PayloadStream:
    """A task payload read as a sequence of byte chunks.

    A streaming handler (see :meth:`Mux.handle_stream`) reads a chunked payload
    here as it arrives; for any other task it yields the inline payload. Read it
    with ``async for chunk in stream`` or ``await stream.read()`` in an async
    handler, and with ``for chunk in stream`` in a sync handler, which runs on
    the worker's thread pool. A stream is read once.
    """

    __slots__ = ("_remaining",)

    def __init__(self, data: bytes = b"") -> None:
        self._remaining: Optional[bytes] = data or None

    def __aiter__(self) -> "PayloadStream":
        return self

    async def __anext__(self) -> bytes:
        chunk = await self._next()
        if chunk is None:
            raise StopAsyncIteration

        return chunk

    def __iter__(self) -> "PayloadStream":
        return self

    def __next__(self) -> bytes:
        chunk = self._next_blocking()
        if chunk is None:
            raise StopIteration

        return chunk

    async def read(self) -> bytes:
        """Read the rest of the payload into memory and return it."""
        return b"".join([chunk async for chunk in self])

    async def _next(self) -> Optional[bytes]:
        """Return the next chunk, or ``None`` at the end."""
        chunk, self._remaining = self._remaining, None

        return chunk

    def _next_blocking(self) -> Optional[bytes]:
        """Return the next chunk from a thread, or ``None`` at the end."""
        chunk, self._remaining = self._remaining, None

        return chunk


class ChunkStream(PayloadStream):
    """The :class:`PayloadStream` a streaming handler reads a chunked payload from.

    The session's receive loop pushes chunks as they arrive. The server sends at
    most the advertised chunk window of unacknowledged chunks per task and the
    buffer is sized to that window, so a push never waits: the session keeps
    reading its stream however slowly the handler reads. Each chunk the handler
    takes out is acknowledged through ``ack``, granting the server one more. A
    handler that returns without reading to the end abandons the stream, and
    later chunks are dropped. It must be built on the event loop that runs the
    session.
    """

    __slots__ = ("_loop", "_chunks", "_window", "_ack", "_arrived", "_ended", "_error", "_abandoned")

    def __init__(self, window: int, ack: Callable[[], None]) -> None:
        super().__init__()
        self._loop = asyncio.get_running_loop()
        self._chunks: Deque[bytes] = deque()
        self._window = window
        self._ack = ack
        self._arrived = asyncio.Event()
        self._ended = False
        self._error: Optional[BaseException] = None
        self._abandoned = False

    def push(self, data: bytes) -> bool:
        """Hand one chunk to the reader without waiting.

        Returns ``False`` when the buffer already holds a full window and the
        reader still wants chunks: the server sent more than its window allows.
        A chunk for an abandoned stream is dropped.
        """
        if self._abandoned:
            return True

        if len(self._chunks) >= self._window:
            return False

        self._chunks.append(data)
        self._arrived.set()

        return True

    def end(self, error: Optional[BaseException]) -> None:
        """End the stream: the reader raises ``error`` after the last chunk, or
        stops cleanly when it is ``None``."""
        self._ended = True
        self._error = error
        self._arrived.set()

    def abandon(self) -> None:
        """Mark the stream as no longer read, dropping buffered and later chunks."""
        self._abandoned = True
        self._chunks.clear()

    async def _next(self) -> Optional[bytes]:
        """Wait for the next chunk and acknowledge it; return ``None`` at a clean
        end and raise the failure that ended the stream otherwise."""
        while not self._chunks:
            if self._ended and self._error is not None:
                raise self._error

            if self._ended:
                return None

            self._arrived.clear()
            await self._arrived.wait()

        chunk = self._chunks.popleft()
        self._ack()

        return chunk

    def _next_blocking(self) -> Optional[bytes]:
        """Wait for the next chunk from a handler thread, through the session's loop."""
        if _running_loop() is self._loop:
            raise ConveyorError("conveyor: read a streamed payload with async for on the event loop")

        return asyncio.run_coroutine_threadsafe(self._next(), self._loop).result()


# Receives a buffered payload once it has arrived, or the failure that ended it
# with an empty payload.
PayloadComplete = Callable[[bytes, Optional[BaseException]], None]


class PayloadAssembly:
    """Collects one chunked payload as its chunk frames arrive, either into a
    buffer handed to ``complete`` or into a streaming handler's
    :class:`ChunkStream`. Only the session's receive loop touches it.
    """

    __slots__ = ("_opener", "_stream", "_complete", "_ack", "_buffer")

    def __init__(
        self,
        opener: Optional[ChunkOpener],
        *,
        stream: Optional[ChunkStream] = None,
        complete: Optional[PayloadComplete] = None,
        ack: Optional[Callable[[], None]] = None,
    ) -> None:
        self._opener = opener
        self._stream = stream
        self._complete = complete
        # Acknowledges one buffered chunk, so the server sends the next; a
        # streamed chunk is acknowledged by its reader instead.
        self._ack = ack
        self._buffer: List[bytes] = []

    def add(self, chunk: "service_pb2.PayloadChunk") -> bool:
        """Apply one chunk frame and report whether the payload is finished,
        successfully or not."""
        if chunk.error:
            self.finish(b"", ConveyorError(f"{_SERVER_READ_FAILED}: {chunk.error}"))

            return True

        if chunk.last:
            self._finish_last()

            return True

        data = chunk.data

        if self._opener is not None:
            try:
                data = self._opener.open(data)
            except Exception as error:  # noqa: BLE001 -- any decryption failure ends the payload
                self.finish(b"", error)

                return True

        if self._stream is not None and not self._stream.push(data):
            self.finish(b"", ConveyorError(_CHUNK_WINDOW_EXCEEDED))

            return True

        if self._stream is not None:
            return False

        self._buffer.append(data)

        if self._ack is not None:
            self._ack()

        return False

    def finish(self, payload: bytes, error: Optional[BaseException]) -> None:
        """End the payload: a streamed one through its reader, a buffered one
        through ``complete``."""
        if self._stream is not None:
            self._stream.end(error)

            return

        if self._complete is not None:
            self._complete(b"" if error is not None else payload, error)

    def abort(self) -> None:
        """End a payload still arriving when its session ends."""
        self.finish(b"", ConveyorError(_SESSION_ENDED))

    def _finish_last(self) -> None:
        """Finish on the frame marked last, checking an encrypted payload was
        not truncated."""
        if self._opener is not None:
            try:
                self._opener.finish()
            except Exception as error:  # noqa: BLE001 -- truncation fails the payload
                self.finish(b"", error)

                return

        self.finish(b"".join(self._buffer), None)


def upload_chunks(
    data: bytes, source: Optional[PayloadSource], encryptor: Optional[Encryptor]
) -> AsyncGenerator[bytes, None]:
    """Yield a payload as upload chunks of at most :data:`PAYLOAD_CHUNK_BYTES`,
    read from ``source`` when it is set and from ``data`` otherwise.

    With an encryptor each chunk is sealed on its own (see :class:`ChunkSealer`),
    and an empty payload is still sent as one sealed chunk so its end is
    authenticated.
    """
    plain = _bytes_chunks(data) if source is None else _source_chunks(source)

    if encryptor is None:
        return plain

    return _sealed_chunks(encryptor, plain)


async def _bytes_chunks(data: bytes) -> AsyncGenerator[bytes, None]:
    """Split in-memory bytes into upload chunks."""
    for offset in range(0, len(data), PAYLOAD_CHUNK_BYTES):
        yield data[offset : offset + PAYLOAD_CHUNK_BYTES]


def _source_chunks(source: PayloadSource) -> AsyncGenerator[bytes, None]:
    """Chunk a streamed source. An async iterable is preferred, so an async file
    object whose ``read`` is a coroutine is iterated rather than read."""
    if isinstance(source, AsyncIterable):
        return _iterable_chunks(source)

    return _reader_chunks(source)


async def _iterable_chunks(source: AsyncIterable[bytes]) -> AsyncGenerator[bytes, None]:
    """Re-chunk an async iterable of arbitrarily sized pieces into upload chunks."""
    pending = bytearray()

    async for piece in source:
        pending += _require_bytes(piece)

        while len(pending) >= PAYLOAD_CHUNK_BYTES:
            yield bytes(pending[:PAYLOAD_CHUNK_BYTES])
            del pending[:PAYLOAD_CHUNK_BYTES]

    if pending:
        yield bytes(pending)


async def _reader_chunks(reader: BinaryReader) -> AsyncGenerator[bytes, None]:
    """Read a file-like object to its end in upload chunks. Reads run on the
    loop's default executor so a blocking file never stalls the event loop."""
    loop = asyncio.get_running_loop()

    while True:
        chunk = await loop.run_in_executor(None, _read_full, reader, PAYLOAD_CHUNK_BYTES)

        if chunk:
            yield chunk

        if len(chunk) < PAYLOAD_CHUNK_BYTES:
            return


def _read_full(reader: BinaryReader, size: int) -> bytes:
    """Read until ``size`` bytes or the end, since one read may return fewer."""
    parts: List[bytes] = []
    remaining = size

    while remaining > 0:
        piece = _require_bytes(reader.read(remaining))
        if not piece:
            break

        parts.append(piece)
        remaining -= len(piece)

    return b"".join(parts)


def _require_bytes(piece: object) -> bytes:
    """Return a source piece as bytes, rejecting text so a file opened in text
    mode fails clearly."""
    if isinstance(piece, (bytes, bytearray, memoryview)):
        return bytes(piece)

    raise ConveyorError(f"conveyor: a streamed payload must yield bytes, got {type(piece).__name__}")


async def _sealed_chunks(
    encryptor: Encryptor, plain: AsyncGenerator[bytes, None]
) -> AsyncGenerator[bytes, None]:
    """Seal each plaintext chunk, looking one chunk ahead so the final chunk
    carries the last flag."""
    sealer = ChunkSealer(encryptor)
    pending: Optional[bytes] = None

    async for data in plain:
        if pending is not None:
            yield sealer.seal(pending, last=False)

        pending = data

    yield sealer.seal(pending or b"", last=True)


def _running_loop() -> Optional[asyncio.AbstractEventLoop]:
    """Return the event loop running on this thread, or ``None``."""
    try:
        return asyncio.get_running_loop()
    except RuntimeError:
        return None
