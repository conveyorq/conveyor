# Copyright 2026 ConveyorQ
#
# SPDX-License-Identifier: Apache-2.0

"""End-to-end tests against a live conveyord.

Start a dev server (``conveyord --dev``) and point the SDK at it with
``CONVEYOR_ADDR=http://localhost:8080``; an auth token, if the server requires
one, goes in ``CONVEYOR_TOKEN``. The whole module is skipped when no address is
set, so the default unit-test run needs no server.

The payloads above 1 MiB need a server whose payload cap admits them. Export
``CONVEYOR_BROKER__MAX_PAYLOAD_BYTES`` (16 MiB, say) to both the server and the
tests: the server reads it as its cap, and the tests skip those cases when it is
unset or too small.
"""

from __future__ import annotations

import asyncio
import hashlib
import io
import os
import uuid
from datetime import timedelta

import grpc
import pytest

from conveyorq import (
    Client,
    Key,
    Mux,
    SkipRetry,
    TaskState,
    Worker,
    binary,
    json,
    new_aes_gcm,
    new_task,
    stream,
)
from conveyorq.gen.conveyor.v1 import service_pb2

ADDR = os.environ.get("CONVEYOR_ADDR")
TOKEN = os.environ.get("CONVEYOR_TOKEN")

# The server's payload cap, as exported for it; 0 when the default applies.
MAX_PAYLOAD_BYTES = int(os.environ.get("CONVEYOR_BROKER__MAX_PAYLOAD_BYTES", "0"))

# A payload above the 1 MiB inline limit spanning several upload chunks.
LARGE_PAYLOAD_BYTES = 3 * (1 << 20) + 12345

# A streamed payload small enough for the server's default 1 MiB cap.
SMALL_STREAMED_BYTES = 512 * 1024

# A payload four chunks past the server's default chunk window of 16, so its
# delivery depends on the worker's chunk acknowledgements.
WINDOW_PAYLOAD_BYTES = (16 + 4) * (1 << 20) + 123

# The payload cap the window case needs.
WINDOW_PAYLOAD_CAP = 32 << 20

# How long a test waits for the worker to process a task.
PROCESS_TIMEOUT = 30

pytestmark = pytest.mark.skipif(not ADDR, reason="set CONVEYOR_ADDR to run integration tests")

requires_large_payloads = pytest.mark.skipif(
    MAX_PAYLOAD_BYTES < LARGE_PAYLOAD_BYTES,
    reason="export CONVEYOR_BROKER__MAX_PAYLOAD_BYTES (>= 4 MiB) to the server and the tests",
)

requires_window_payloads = pytest.mark.skipif(
    MAX_PAYLOAD_BYTES < WINDOW_PAYLOAD_CAP,
    reason="export CONVEYOR_BROKER__MAX_PAYLOAD_BYTES (>= 32 MiB) to the server and the tests",
)


async def _run_worker_until(
    mux: Mux, stop: asyncio.Event, *, concurrency: int = 4, encryptor=None
) -> asyncio.Task:
    worker = Worker(ADDR, queues={"default": 1}, concurrency=concurrency, token=TOKEN, encryptor=encryptor)
    task = asyncio.create_task(worker.run(mux, stop=stop, install_signal_handlers=False))

    return task


def _payload(size: int) -> bytes:
    return bytes(index % 251 for index in range(size))


def _codec():
    return new_aes_gcm("k1", Key("k1", bytes([1]) * 32))


async def _await_state(client: Client, task_id: str, state: TaskState) -> None:
    deadline = asyncio.get_event_loop().time() + PROCESS_TIMEOUT
    while asyncio.get_event_loop().time() < deadline:
        if (await client.get_task(task_id)).state == state:
            return

        await asyncio.sleep(0.1)

    raise AssertionError(f"task {task_id} never reached {state}")


class _DigestRecorder:
    """Records the SHA-256 of the payload each handled task read."""

    def __init__(self) -> None:
        self.digests: dict = {}

    async def buffered(self, task, ctx) -> None:
        self.digests[task.id] = hashlib.sha256(task.payload).hexdigest()

    async def streamed(self, task, ctx) -> None:
        digest = hashlib.sha256()

        async for chunk in task.payload_stream:
            digest.update(chunk)

        self.digests[task.id] = digest.hexdigest()


async def test_enqueue_then_process_round_trips():
    task_type = f"itest:echo:{uuid.uuid4()}"
    done = asyncio.Event()
    received: list[dict] = []

    mux = Mux()

    @mux.handler(task_type)
    async def handle(task, ctx):
        received.append(task.json())
        done.set()

    stop = asyncio.Event()
    worker_task = await _run_worker_until(mux, stop)

    try:
        async with Client(ADDR, token=TOKEN) as client:
            info = await client.enqueue(new_task(task_type, json({"n": 7})))
            assert info.id

            await asyncio.wait_for(done.wait(), timeout=15)
            assert received == [{"n": 7}]

            fetched = await client.get_task(info.id)
            assert fetched.id == info.id
    finally:
        stop.set()
        await asyncio.wait_for(worker_task, timeout=10)


async def test_handler_reports_progress():
    task_type = f"itest:progress:{uuid.uuid4()}"
    done = asyncio.Event()

    mux = Mux()

    @mux.handler(task_type)
    async def handle(task, ctx):
        ctx.report_progress(50, "halfway")
        done.set()

    stop = asyncio.Event()
    worker_task = await _run_worker_until(mux, stop)

    try:
        async with Client(ADDR, token=TOKEN) as client:
            info = await client.enqueue(new_task(task_type, json({})), retention=timedelta(hours=1))
            await asyncio.wait_for(done.wait(), timeout=15)

            async def progress_recorded() -> bool:
                fetched = await client.get_task(info.id)
                return fetched.progress == 50 and fetched.progress_message == "halfway"

            deadline = asyncio.get_event_loop().time() + 15
            while asyncio.get_event_loop().time() < deadline:
                if await progress_recorded():
                    break
                await asyncio.sleep(0.1)

            fetched = await client.get_task(info.id)
            assert fetched.progress == 50
            assert fetched.progress_message == "halfway"
    finally:
        stop.set()
        await asyncio.wait_for(worker_task, timeout=10)


async def test_run_returns_promptly_on_stop():
    # An idle worker must drain and return quickly when stopped (no hang on the
    # receive loop waiting for a server that has nothing to send).
    mux = Mux()
    stop = asyncio.Event()
    worker_task = await _run_worker_until(mux, stop)

    await asyncio.sleep(0.5)  # let the session establish
    stop.set()
    await asyncio.wait_for(worker_task, timeout=8)


async def test_sync_handler_processes_end_to_end():
    task_type = f"itest:sync:{uuid.uuid4()}"
    done = asyncio.Event()
    received: list[dict] = []

    mux = Mux()

    def handle(task, ctx):  # a plain sync handler runs on the worker's thread pool
        received.append(task.json())
        done.set()

    mux.handle(task_type, handle)
    stop = asyncio.Event()
    worker_task = await _run_worker_until(mux, stop)

    try:
        async with Client(ADDR, token=TOKEN) as client:
            await client.enqueue(new_task(task_type, json({"k": "v"})))
            await asyncio.wait_for(done.wait(), timeout=15)
            assert received == [{"k": "v"}]
    finally:
        stop.set()
        await asyncio.wait_for(worker_task, timeout=10)


async def test_skip_retry_archives_without_retrying():
    task_type = f"itest:skip:{uuid.uuid4()}"
    attempts = 0
    done = asyncio.Event()

    mux = Mux()

    @mux.handler(task_type)
    async def handle(task, ctx):
        nonlocal attempts
        attempts += 1
        done.set()
        raise SkipRetry("permanent failure")

    stop = asyncio.Event()
    worker_task = await _run_worker_until(mux, stop)

    try:
        async with Client(ADDR, token=TOKEN) as client:
            await client.enqueue(new_task(task_type, json({}), ), max_retry=5)
            await asyncio.wait_for(done.wait(), timeout=15)
            # Give the server a moment; a SkipRetry must not redeliver.
            await asyncio.sleep(2)
            assert attempts == 1
    finally:
        stop.set()
        await asyncio.wait_for(worker_task, timeout=10)


@requires_large_payloads
async def test_large_payload_is_uploaded_in_chunks_and_reassembled():
    task_type = f"itest:large:{uuid.uuid4()}"
    payload = _payload(LARGE_PAYLOAD_BYTES)
    recorder = _DigestRecorder()
    mux = Mux().handle(task_type, recorder.buffered)
    stop = asyncio.Event()
    worker_task = await _run_worker_until(mux, stop)

    try:
        async with Client(ADDR, token=TOKEN) as client:
            info = await client.enqueue(new_task(task_type, binary(payload)), retention=timedelta(hours=1))

            assert info.payload_chunked
            assert info.payload_size == len(payload)

            await _await_state(client, info.id, TaskState.COMPLETED)
            assert recorder.digests[info.id] == hashlib.sha256(payload).hexdigest()
    finally:
        stop.set()
        await asyncio.wait_for(worker_task, timeout=10)


async def test_streaming_handler_reads_a_streamed_payload():
    task_type = f"itest:stream:{uuid.uuid4()}"
    payload = _payload(SMALL_STREAMED_BYTES)
    recorder = _DigestRecorder()
    mux = Mux().handle_stream(task_type, recorder.streamed)
    stop = asyncio.Event()
    worker_task = await _run_worker_until(mux, stop)

    async def source():
        for offset in range(0, len(payload), 100_000):
            yield payload[offset : offset + 100_000]

    try:
        async with Client(ADDR, token=TOKEN) as client:
            info = await client.enqueue(new_task(task_type, stream(source())), retention=timedelta(hours=1))

            assert info.payload_chunked

            await _await_state(client, info.id, TaskState.COMPLETED)
            assert recorder.digests[info.id] == hashlib.sha256(payload).hexdigest()
    finally:
        stop.set()
        await asyncio.wait_for(worker_task, timeout=10)


@requires_large_payloads
async def test_large_payload_encrypted_end_to_end():
    buffered_type = f"itest:secret:{uuid.uuid4()}"
    streamed_type = f"itest:secret-stream:{uuid.uuid4()}"
    payload = _payload(LARGE_PAYLOAD_BYTES)
    recorder = _DigestRecorder()
    mux = Mux().handle(buffered_type, recorder.buffered).handle_stream(streamed_type, recorder.streamed)
    stop = asyncio.Event()
    worker_task = await _run_worker_until(mux, stop, encryptor=_codec())

    try:
        async with Client(ADDR, token=TOKEN, encryptor=_codec()) as client:
            for task_type in (buffered_type, streamed_type):
                info = await client.enqueue(
                    new_task(task_type, stream(io.BytesIO(payload))), retention=timedelta(hours=1)
                )

                await _await_state(client, info.id, TaskState.COMPLETED)
                assert recorder.digests[info.id] == hashlib.sha256(payload).hexdigest(), task_type
    finally:
        stop.set()
        await asyncio.wait_for(worker_task, timeout=10)


async def test_encrypted_streamed_payload_without_a_worker_key_retries():
    task_type = f"itest:no-key:{uuid.uuid4()}"
    mux = Mux().handle(task_type, lambda task, ctx: None)
    stop = asyncio.Event()
    worker_task = await _run_worker_until(mux, stop)

    try:
        async with Client(ADDR, token=TOKEN, encryptor=_codec()) as client:
            info = await client.enqueue(
                new_task(task_type, stream(io.BytesIO(b"sealed"))), retention=timedelta(hours=1)
            )

            await _await_state(client, info.id, TaskState.RETRY)
            assert "no encryptor" in (await client.get_task(info.id)).last_error
    finally:
        stop.set()
        await asyncio.wait_for(worker_task, timeout=10)


async def test_broken_streamed_upload_commits_nothing():
    task_id = f"itest-broken-{uuid.uuid4()}"

    async def broken():
        yield b"partial"
        raise OSError("source broken")

    async with Client(ADDR, token=TOKEN) as client:
        with pytest.raises(OSError):
            await client.enqueue(new_task("itest:broken", stream(broken())), task_id=task_id)

        with pytest.raises(grpc.aio.AioRpcError):
            await client.get_task(task_id)


async def test_streamed_retry_of_an_existing_task_returns_it():
    task_id = f"itest-idempotent-{uuid.uuid4()}"
    payload = _payload(SMALL_STREAMED_BYTES)

    def task():
        return new_task("itest:idempotent", stream(io.BytesIO(payload)))

    async with Client(ADDR, token=TOKEN) as client:
        first = await client.enqueue(task(), task_id=task_id)
        again = await client.enqueue(task(), task_id=task_id)

        assert again.id == first.id


async def test_batch_handler_receives_chunked_members_reassembled():
    task_type = f"itest:batch:{uuid.uuid4()}"
    payload = _payload(SMALL_STREAMED_BYTES)
    digests: dict = {}

    async def handle(tasks, ctx):
        for task in tasks:
            digests[task.id] = hashlib.sha256(task.payload).hexdigest()

    mux = Mux().handle_batch(task_type, handle)
    stop = asyncio.Event()
    worker_task = await _run_worker_until(mux, stop)

    try:
        async with Client(ADDR, token=TOKEN) as client:
            ids = []

            for _ in range(2):
                info = await client.enqueue(
                    new_task(task_type, stream(io.BytesIO(payload))),
                    group=f"blobs-{task_type}",
                    retention=timedelta(hours=1),
                )
                ids.append(info.id)

            for task_id in ids:
                await _await_state(client, task_id, TaskState.COMPLETED)
                assert digests[task_id] == hashlib.sha256(payload).hexdigest()
    finally:
        stop.set()
        await asyncio.wait_for(worker_task, timeout=10)


@requires_window_payloads
async def test_slow_streaming_handler_reads_a_payload_beyond_the_chunk_window():
    task_type = f"itest:window:{uuid.uuid4()}"
    payload = _payload(WINDOW_PAYLOAD_BYTES)
    digests: dict = {}

    async def handle(task, ctx):
        digest = hashlib.sha256()

        async for chunk in task.payload_stream:
            await asyncio.sleep(0.05)
            digest.update(chunk)

        digests[task.id] = digest.hexdigest()

    mux = Mux().handle_stream(task_type, handle)
    stop = asyncio.Event()
    worker_task = await _run_worker_until(mux, stop)

    try:
        async with Client(ADDR, token=TOKEN) as client:
            info = await client.enqueue(new_task(task_type, binary(payload)), retention=timedelta(hours=1))

            await _await_state(client, info.id, TaskState.COMPLETED)
            assert digests[info.id] == hashlib.sha256(payload).hexdigest()
    finally:
        stop.set()
        await asyncio.wait_for(worker_task, timeout=10)


async def test_upload_ended_without_a_commit_commits_nothing():
    task_id = f"itest-uncommitted-{uuid.uuid4()}"
    header = service_pb2.EnqueueRequest(task_id=task_id, queue="default", type="itest:uncommitted")

    async with Client(ADDR, token=TOKEN) as client:
        metadata = (("authorization", f"Bearer {TOKEN}"),) if TOKEN else ()
        call = client._connect().EnqueueStream(metadata=metadata)
        await call.write(service_pb2.EnqueueStreamRequest(header=header))
        await call.write(service_pb2.EnqueueStreamRequest(chunk=b"truncated"))
        await call.done_writing()

        with pytest.raises(grpc.aio.AioRpcError) as rejected:
            await call

        assert rejected.value.code() == grpc.StatusCode.INVALID_ARGUMENT

        with pytest.raises(grpc.aio.AioRpcError):
            await client.get_task(task_id)
