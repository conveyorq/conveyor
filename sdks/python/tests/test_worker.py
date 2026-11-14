# Copyright 2026 ConveyorQ
#
# SPDX-License-Identifier: Apache-2.0

import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from typing import List

import grpc
import pytest

from conveyorq import Key, _time, new_aes_gcm
from conveyorq.encryption import ENCRYPTION_MARKER_KEY, ENCRYPTION_MARKER_VALUE, ChunkSealer
from conveyorq.errors import ConveyorError
from conveyorq.gen.conveyor.v1 import service_pb2, task_pb2
from conveyorq.mux import Mux
from conveyorq.streaming import DEFAULT_PAYLOAD_CHUNK_WINDOW
from conveyorq.worker import (
    _FATAL_CODES,
    Worker,
    _chunk_window,
    _full_jitter,
    _hello,
    _Session,
    _SessionConfig,
)


# The worker validates its session contract at construction, so a Hello the
# server would reject fails fast instead of reconnecting forever against a
# permanent INVALID_ARGUMENT. The accepted queue-name shape is the server's own.
@pytest.mark.parametrize("name", ["my queue", "-leading", ".leading", "has/slash", "", "default\n"])
def test_worker_rejects_queue_names_the_server_refuses(name):
    with pytest.raises(ConveyorError):
        Worker("localhost:8080", queues={name: 1}, concurrency=1)


@pytest.mark.parametrize("name", ["default", "high-priority", "billing_v2", "a.b.c", "0queue"])
def test_worker_accepts_queue_names_the_server_allows(name):
    Worker("localhost:8080", queues={name: 1}, concurrency=1)


@pytest.mark.parametrize("weight", [0, -1])
def test_worker_rejects_non_positive_queue_weight(weight):
    with pytest.raises(ConveyorError):
        Worker("localhost:8080", queues={"default": weight}, concurrency=1)


def test_worker_rejects_non_positive_concurrency():
    with pytest.raises(ConveyorError):
        Worker("localhost:8080", queues={"default": 1}, concurrency=0)


def test_full_jitter_stays_within_the_attempt_ceiling():
    for attempt in range(0, 8):
        ceiling = min(30.0, 0.5 * (2 ** attempt))
        for _ in range(100):
            delay = _full_jitter(attempt)
            assert 0.0 <= delay < ceiling or delay == 0.0


def test_full_jitter_is_capped_at_max():
    assert _full_jitter(100) < 30.0


def test_full_jitter_survives_a_long_outage():
    # An attempt count from hours of reconnecting must not overflow the float
    # ceiling and crash the worker; the delay simply stays at the cap.
    assert _full_jitter(5000) < 30.0


def test_fatal_codes_are_the_non_recoverable_set():
    assert grpc.StatusCode.UNAUTHENTICATED in _FATAL_CODES
    assert grpc.StatusCode.PERMISSION_DENIED in _FATAL_CODES
    assert grpc.StatusCode.INVALID_ARGUMENT in _FATAL_CODES
    # Transient failures must reconnect, not stop the worker. The wire protocol
    # names exactly three fatal codes, so anything else, FAILED_PRECONDITION
    # included, reconnects; the Go and TypeScript SDKs draw the same line.
    assert grpc.StatusCode.FAILED_PRECONDITION not in _FATAL_CODES
    assert grpc.StatusCode.UNAVAILABLE not in _FATAL_CODES
    assert grpc.StatusCode.INTERNAL not in _FATAL_CODES


def test_hello_carries_the_declared_shape():
    config = _SessionConfig(
        queues={"default": 1, "critical": 5},
        concurrency=10,
        sdk_version="conveyor-py/test",
        min_server_version="v1.2.0",
        metadata=(),
    )
    message = _hello(config, ["digest"])
    hello = message.hello

    assert dict(hello.queues) == {"default": 1, "critical": 5}
    assert hello.concurrency == 10
    assert hello.sdk_version == "conveyor-py/test"
    assert hello.min_server_version == "v1.2.0"
    assert list(hello.batch_types) == ["digest"]
    assert hello.payload_streaming


def _envelope(task_id: str = "t1", task_type: str = "demo") -> "task_pb2.TaskEnvelope":
    return task_pb2.TaskEnvelope(
        id=task_id, type=task_type, queue="default", payload=b"{}", content_type="application/json"
    )


def _session(mux: Mux) -> _Session:
    config = _SessionConfig(
        queues={"default": 1}, concurrency=4, sdk_version="test", min_server_version="", metadata=()
    )
    return _Session(None, config, mux, None, ThreadPoolExecutor(max_workers=4))


def _result_outcomes(session: _Session) -> List["service_pb2.TaskOutcome"]:
    outcomes = []
    while not session._outbound.empty():
        message = session._outbound.get_nowait()
        if message is not None and message.WhichOneof("frame") == "result":
            outcomes.append(message.result.outcome)

    return outcomes


async def test_run_one_releases_a_task_waiting_when_drain_starts():
    # A task still queued behind the concurrency gate when the drain begins never
    # runs and is released with no retry penalty, not retried.
    mux = Mux()
    mux.handle("demo", lambda task, ctx: None)

    session = _session(mux)
    session._draining = True

    await session._run_one(_envelope(), None)

    assert _result_outcomes(session) == [service_pb2.TASK_OUTCOME_RELEASED]


async def test_run_one_maps_a_drain_interrupted_task_to_released():
    # A handler the drain aborted (its id marked released) reports RELEASED even
    # though it raised, so the redelivery does not burn a retry.
    def boom(task, ctx):
        raise RuntimeError("interrupted by drain")

    mux = Mux()
    mux.handle("demo", boom)

    session = _session(mux)
    session._released_ids.add("t1")

    await session._run_one(_envelope("t1"), None)

    assert _result_outcomes(session) == [service_pb2.TASK_OUTCOME_RELEASED]


async def test_run_one_retries_a_genuine_failure_when_not_draining():
    # Outside a drain, the same failure is a retry: RELEASED is reserved for drain.
    def boom(task, ctx):
        raise RuntimeError("genuine failure")

    mux = Mux()
    mux.handle("demo", boom)

    session = _session(mux)

    await session._run_one(_envelope("t1"), None)

    assert _result_outcomes(session) == [service_pb2.TASK_OUTCOME_RETRY]


def test_dispatch_during_drain_is_parked_and_heartbeated():
    # A task dispatched after the drain began is never started, but its lease
    # is kept alive by the heartbeat until the stream closes, so the server
    # releases it without a retry penalty instead of reaping it.
    mux = Mux()
    mux.handle("demo", lambda task, ctx: None)

    session = _session(mux)
    session._draining = True

    session._handle(service_pb2.ServerMessage(dispatch=service_pb2.Dispatch(task=_envelope("t1"))))
    session._handle(
        service_pb2.ServerMessage(
            batch_dispatch=service_pb2.BatchDispatch(tasks=[_envelope("t2"), _envelope("t3")], group="g")
        )
    )

    assert session._parked == {"t1", "t2", "t3"}
    assert session._tasks == set()
    assert _result_outcomes(session) == []
    assert sorted(session._active_task_ids()) == ["t1", "t2", "t3"]


class _CancelledCall:
    """A stream whose receive side is cancelled locally, as the drain's own
    teardown does when the server lingers past the half-close."""

    def __aiter__(self):
        return self

    async def __anext__(self):
        raise asyncio.CancelledError()

    async def write(self, message):
        pass

    async def done_writing(self):
        pass

    def cancel(self):
        pass


class _CancelledStub:
    def Session(self, metadata=()):
        return _CancelledCall()


def _stub_session(stub) -> _Session:
    config = _SessionConfig(
        queues={"default": 1}, concurrency=4, sdk_version="test", min_server_version="", metadata=()
    )
    return _Session(stub, config, Mux(), None, ThreadPoolExecutor(max_workers=4))


async def test_run_returns_when_the_drain_cancels_the_call():
    # The drain's fallback cancels the call itself; that cancellation ends the
    # session cleanly instead of escaping run() as an error.
    stop = asyncio.Event()
    stop.set()

    assert await _stub_session(_CancelledStub()).run(stop) is False


async def test_run_propagates_a_cancellation_that_is_not_the_drains():
    # A cancellation from the caller (the event loop shutting the worker down)
    # is theirs and must propagate.
    with pytest.raises(asyncio.CancelledError):
        await _stub_session(_CancelledStub()).run(asyncio.Event())


def test_duration_round_trips_through_proto():
    proto = _time.duration_proto(timedelta(milliseconds=1500))

    assert _time.duration_seconds(proto) == 1.5


def test_timestamp_round_trips_through_proto():
    when = datetime(2026, 6, 16, 10, 0, 0, tzinfo=timezone.utc)
    proto = _time.timestamp_proto(when)

    assert _time.datetime_from_timestamp(proto) == when


def test_unset_timestamp_is_none():
    from google.protobuf.timestamp_pb2 import Timestamp

    assert _time.datetime_from_timestamp(Timestamp()) is None


# How long a test waits for something that must happen promptly.
_PROMPT_SECONDS = 5


def _chunked_envelope(task_id: str = "t1", task_type: str = "demo", encrypted: bool = False):
    envelope = task_pb2.TaskEnvelope(
        id=task_id,
        type=task_type,
        queue="default",
        content_type="application/octet-stream",
        payload_chunked=True,
    )

    if encrypted:
        envelope.metadata[ENCRYPTION_MARKER_KEY] = ENCRYPTION_MARKER_VALUE

    return envelope


def _dispatch_message(envelope) -> "service_pb2.ServerMessage":
    return service_pb2.ServerMessage(dispatch=service_pb2.Dispatch(task=envelope))


def _batch_message(*envelopes) -> "service_pb2.ServerMessage":
    return service_pb2.ServerMessage(batch_dispatch=service_pb2.BatchDispatch(tasks=envelopes, group="g"))


def _chunk(task_id: str, data: bytes = b"", last: bool = False, error: str = "") -> service_pb2.PayloadChunk:
    return service_pb2.PayloadChunk(task_id=task_id, data=data, last=last, error=error)


def _encrypted_session(mux: Mux) -> _Session:
    config = _SessionConfig(
        queues={"default": 1}, concurrency=4, sdk_version="test", min_server_version="", metadata=()
    )
    return _Session(None, config, mux, _codec(), ThreadPoolExecutor(max_workers=4))


def _codec():
    return new_aes_gcm("k1", Key("k1", bytes([1]) * 32))


def _results(session: _Session) -> List["service_pb2.Result"]:
    results = []
    while not session._outbound.empty():
        message = session._outbound.get_nowait()
        if message is not None and message.WhichOneof("frame") == "result":
            results.append(message.result)

    return results


async def _settle(session: _Session) -> None:
    """Wait for every task the session spawned, including ones spawned meanwhile."""
    while session._tasks:
        await asyncio.wait_for(asyncio.gather(*list(session._tasks)), _PROMPT_SECONDS)


async def test_chunked_dispatch_runs_the_handler_once_the_payload_arrives():
    received = []

    async def handle(task, ctx):
        received.append(task.payload)

    session = _session(Mux().handle("demo", handle))
    session._handle(_dispatch_message(_chunked_envelope()))

    # The task stays heartbeated while its payload arrives.
    assert session._active_task_ids() == ["t1"]

    session._receive_chunk(_chunk("t1", b"ab"))
    session._receive_chunk(_chunk("t1", b"cd"))
    await asyncio.sleep(0)
    assert received == []

    session._receive_chunk(_chunk("t1", last=True))
    await _settle(session)

    assert received == [b"abcd"]
    assert _result_outcomes(session) == [service_pb2.TASK_OUTCOME_SUCCESS]
    assert session._payloads == {}
    assert session._inflight == {}


async def test_streaming_handler_starts_before_the_payload_arrives():
    started = asyncio.Event()
    received = []

    async def handle(task, ctx):
        started.set()
        received.append((task.payload, await task.payload_stream.read()))

    session = _session(Mux().handle_stream("demo", handle))
    session._handle(_dispatch_message(_chunked_envelope()))
    await asyncio.wait_for(started.wait(), _PROMPT_SECONDS)

    session._receive_chunk(_chunk("t1", b"streamed "))
    session._receive_chunk(_chunk("t1", b"payload"))
    session._receive_chunk(_chunk("t1", last=True))
    await _settle(session)

    assert received == [(b"", b"streamed payload")]
    assert _result_outcomes(session) == [service_pb2.TASK_OUTCOME_SUCCESS]


async def test_sync_streaming_handler_reads_on_its_thread():
    received = []

    def handle(task, ctx):
        received.append(b"".join(task.payload_stream))

    session = _session(Mux().handle_stream("demo", handle))
    session._handle(_dispatch_message(_chunked_envelope()))

    session._receive_chunk(_chunk("t1", b"sync "))
    session._receive_chunk(_chunk("t1", b"read"))
    session._receive_chunk(_chunk("t1", last=True))
    await _settle(session)

    assert received == [b"sync read"]


async def test_streaming_handler_that_returns_early_never_stalls_the_session():
    async def handle(task, ctx):
        pass

    session = _session(Mux().handle_stream("demo", handle))
    session._handle(_dispatch_message(_chunked_envelope()))
    await _settle(session)

    for _ in range(4 * DEFAULT_PAYLOAD_CHUNK_WINDOW):
        session._receive_chunk(_chunk("t1", b"unread"))

    session._receive_chunk(_chunk("t1", last=True))

    assert _result_outcomes(session) == [service_pb2.TASK_OUTCOME_SUCCESS]
    assert session._payloads == {}


async def test_encrypted_chunked_payload_is_opened_per_chunk():
    received = []
    sealer = ChunkSealer(_codec())

    async def handle(task, ctx):
        received.append((task.payload, dict(task.metadata)))

    session = _encrypted_session(Mux().handle("demo", handle))
    session._handle(_dispatch_message(_chunked_envelope(encrypted=True)))

    session._receive_chunk(_chunk("t1", sealer.seal(b"secret ", last=False)))
    session._receive_chunk(_chunk("t1", sealer.seal(b"payload", last=True)))
    session._receive_chunk(_chunk("t1", last=True))
    await _settle(session)

    assert received == [(b"secret payload", {})]


async def test_encrypted_chunked_payload_without_an_encryptor_retries():
    session = _session(Mux().handle("demo", lambda task, ctx: None))
    session._handle(_dispatch_message(_chunked_envelope(encrypted=True)))

    [result] = _results(session)
    assert result.outcome == service_pb2.TASK_OUTCOME_RETRY
    assert "no encryptor" in result.error_msg
    assert session._payloads == {}

    session._receive_chunk(_chunk("t1", b"dropped"))


async def test_payload_the_server_could_not_read_retries():
    session = _session(Mux().handle("demo", lambda task, ctx: None))
    session._handle(_dispatch_message(_chunked_envelope()))

    session._receive_chunk(_chunk("t1", last=True, error="store down"))

    [result] = _results(session)
    assert result.outcome == service_pb2.TASK_OUTCOME_RETRY
    assert "store down" in result.error_msg
    assert session._inflight == {}


async def test_batch_starts_once_every_chunked_member_arrived():
    received = []

    async def handle(tasks, ctx):
        received.append({task.id: task.payload for task in tasks})

    session = _session(Mux().handle_batch("demo", handle))
    session._handle(_batch_message(_chunked_envelope("t1"), _envelope("t2"), _chunked_envelope("t3")))

    assert sorted(session._active_task_ids()) == ["t1", "t2", "t3"]

    session._receive_chunk(_chunk("t1", b"one"))
    session._receive_chunk(_chunk("t3", b"three"))
    session._receive_chunk(_chunk("t1", last=True))
    await asyncio.sleep(0)
    assert received == []

    session._receive_chunk(_chunk("t3", last=True))
    await _settle(session)

    assert received == [{"t1": b"one", "t2": b"{}", "t3": b"three"}]
    assert _result_outcomes(session) == [service_pb2.TASK_OUTCOME_SUCCESS] * 3


async def test_batch_member_failure_retries_the_whole_batch():
    ran = []
    session = _session(Mux().handle_batch("demo", lambda tasks, ctx: ran.append(tasks)))
    session._handle(_batch_message(_chunked_envelope("t1"), _chunked_envelope("t2")))

    session._receive_chunk(_chunk("t1", last=True, error="store down"))
    session._receive_chunk(_chunk("t2", b"dropped"))
    session._receive_chunk(_chunk("t2", last=True))
    await _settle(session)

    assert ran == []
    assert _result_outcomes(session) == [service_pb2.TASK_OUTCOME_RETRY] * 2
    assert session._payloads == {}
    assert session._inflight == {}


async def test_batch_with_an_encrypted_member_and_no_encryptor_retries():
    session = _session(Mux().handle_batch("demo", lambda tasks, ctx: None))
    session._handle(_batch_message(_chunked_envelope("t1", encrypted=True), _envelope("t2")))

    assert _result_outcomes(session) == [service_pb2.TASK_OUTCOME_RETRY] * 2
    assert session._payloads == {}


async def test_session_end_fails_every_payload_still_arriving():
    failures = []

    async def stream_handle(task, ctx):
        try:
            await task.payload_stream.read()
        except ConveyorError as error:
            failures.append(str(error))
            raise

    mux = Mux().handle("buffered", lambda task, ctx: None).handle_stream("streamed", stream_handle)
    session = _session(mux)
    session._handle(_dispatch_message(_chunked_envelope("t1", "buffered")))
    session._handle(_dispatch_message(_chunked_envelope("t2", "streamed")))
    session._receive_chunk(_chunk("t2", b"partial"))

    session._abort_payloads()
    await _settle(session)

    assert session._payloads == {}
    assert failures and "session ended" in failures[0]
    assert sorted(result.task_id for result in _results(session)) == ["t1", "t2"]


class _ScriptedCall:
    """A worker stream that delivers a fixed script, then stays open until
    ``release`` is set, recording what the session writes."""

    def __init__(self, messages, release: asyncio.Event) -> None:
        self._messages = messages
        self._release = release
        self.written: list = []

    def __aiter__(self):
        return self._iterate()

    async def _iterate(self):
        for message in self._messages:
            yield message

        await self._release.wait()

    async def write(self, message):
        self.written.append(message)

    async def done_writing(self):
        pass

    def cancel(self):
        pass


class _ScriptedStub:
    def __init__(self, call: _ScriptedCall) -> None:
        self._call = call

    def Session(self, metadata=()):  # noqa: N802 - gRPC method name
        return self._call


async def test_run_routes_payload_chunks_to_their_task():
    done = asyncio.Event()
    received = []

    async def handle(task, ctx):
        received.append(await task.payload_stream.read())
        done.set()

    script = [
        _dispatch_message(_chunked_envelope()),
        service_pb2.ServerMessage(payload_chunk=_chunk("t1", b"over the wire")),
        service_pb2.ServerMessage(payload_chunk=_chunk("t1", last=True)),
    ]
    call = _ScriptedCall(script, done)
    config = _SessionConfig(
        queues={"default": 1}, concurrency=4, sdk_version="test", min_server_version="", metadata=()
    )
    mux = Mux().handle_stream("demo", handle)
    session = _Session(_ScriptedStub(call), config, mux, None, ThreadPoolExecutor(max_workers=4))

    await asyncio.wait_for(session.run(asyncio.Event()), _PROMPT_SECONDS)

    assert received == [b"over the wire"]
    assert call.written[0].hello.payload_streaming


async def test_inline_dispatches_start_at_once():
    ran = []
    mux = Mux().handle("demo", lambda task, ctx: ran.append(task.id))
    mux.handle_batch("group", lambda tasks, ctx: ran.extend(task.id for task in tasks))
    session = _session(mux)

    session._handle(_dispatch_message(_envelope("t1")))
    session._handle(_batch_message(_envelope("t2", "group"), _envelope("t3", "group")))
    await _settle(session)

    assert sorted(ran) == ["t1", "t2", "t3"]
    assert session._payloads == {}


def _acks(session: _Session) -> List[str]:
    """Drain the outbound queue and return the task id of every PayloadAck, one per chunk."""
    acked = []
    while not session._outbound.empty():
        message = session._outbound.get_nowait()
        if message is not None and message.WhichOneof("frame") == "payload_ack":
            acked.extend([message.payload_ack.task_id] * message.payload_ack.chunks)

    return acked


def test_chunk_window_falls_back_when_unadvertised():
    assert _chunk_window(service_pb2.Welcome()) == DEFAULT_PAYLOAD_CHUNK_WINDOW
    assert _chunk_window(service_pb2.Welcome(payload_chunk_window=4)) == 4


async def test_welcome_sets_the_session_chunk_window():
    session = _session(Mux())
    session._on_welcome(service_pb2.Welcome(payload_chunk_window=3))
    session._stop_heartbeat()

    assert session._chunk_window == 3


async def test_buffered_chunks_are_acked_on_receipt():
    session = _session(Mux().handle("demo", lambda task, ctx: None))
    session._handle(_dispatch_message(_chunked_envelope()))

    session._receive_chunk(_chunk("t1", b"ab"))
    session._receive_chunk(_chunk("t1", b"cd"))

    assert _acks(session) == ["t1", "t1"]

    session._receive_chunk(_chunk("t1", last=True))
    await _settle(session)


async def test_batch_member_chunks_are_acked_on_receipt():
    session = _session(Mux().handle_batch("demo", lambda tasks, ctx: None))
    session._handle(_batch_message(_chunked_envelope("t1"), _chunked_envelope("t2")))

    session._receive_chunk(_chunk("t1", b"one"))
    session._receive_chunk(_chunk("t2", b"two"))

    assert _acks(session) == ["t1", "t2"]

    session._receive_chunk(_chunk("t1", last=True))
    session._receive_chunk(_chunk("t2", last=True))
    await _settle(session)


async def test_streamed_chunks_are_acked_as_the_handler_reads_them():
    release = asyncio.Event()

    async def handle(task, ctx):
        await release.wait()
        await task.payload_stream.read()

    session = _session(Mux().handle_stream("demo", handle))
    session._handle(_dispatch_message(_chunked_envelope()))

    session._receive_chunk(_chunk("t1", b"a"))
    session._receive_chunk(_chunk("t1", b"b"))
    session._receive_chunk(_chunk("t1", last=True))
    await asyncio.sleep(0)
    assert _acks(session) == []

    release.set()
    await _settle(session)

    assert _acks(session) == ["t1", "t1"]


async def test_streaming_payload_over_the_window_fails_its_task():
    release = asyncio.Event()

    async def handle(task, ctx):
        await release.wait()
        await task.payload_stream.read()

    session = _session(Mux().handle_stream("demo", handle))
    session._chunk_window = 2
    session._handle(_dispatch_message(_chunked_envelope()))

    for data in (b"one", b"two", b"over the window"):
        session._receive_chunk(_chunk("t1", data))

    assert session._payloads == {}

    release.set()
    await _settle(session)

    [result] = _results(session)
    assert result.outcome == service_pb2.TASK_OUTCOME_RETRY
    assert "chunk window" in result.error_msg


class _FlowControlledCall:
    """A fake server that streams one task's chunks under a chunk window,
    sending more only as PayloadAck credit comes back."""

    def __init__(self, window: int, chunks: List[bytes], release: asyncio.Event) -> None:
        self._window = window
        self._chunks = chunks
        self._release = release
        self._credit = window
        self._credited = asyncio.Event()
        self.acked = 0

    def __aiter__(self):
        return self._iterate()

    async def _iterate(self):
        yield service_pb2.ServerMessage(welcome=service_pb2.Welcome(payload_chunk_window=self._window))
        yield _dispatch_message(_chunked_envelope())

        for data in self._chunks:
            while self._credit == 0:
                self._credited.clear()
                await self._credited.wait()

            self._credit -= 1
            yield service_pb2.ServerMessage(payload_chunk=_chunk("t1", data))

        yield service_pb2.ServerMessage(payload_chunk=_chunk("t1", last=True))
        await self._release.wait()

    async def write(self, message):
        if message.WhichOneof("frame") == "payload_ack":
            self._credit += message.payload_ack.chunks
            self.acked += message.payload_ack.chunks
            self._credited.set()

    async def done_writing(self):
        pass

    def cancel(self):
        pass


async def test_slow_streaming_handler_reads_a_payload_larger_than_the_window():
    window = 2
    chunks = [bytes([index]) * 8 for index in range(3 * window)]
    done = asyncio.Event()
    received = []

    async def handle(task, ctx):
        async for chunk in task.payload_stream:
            await asyncio.sleep(0.01)
            received.append(chunk)

        done.set()

    call = _FlowControlledCall(window, chunks, done)
    config = _SessionConfig(
        queues={"default": 1}, concurrency=4, sdk_version="test", min_server_version="", metadata=()
    )
    mux = Mux().handle_stream("demo", handle)
    session = _Session(_ScriptedStub(call), config, mux, None, ThreadPoolExecutor(max_workers=4))

    await asyncio.wait_for(session.run(asyncio.Event()), _PROMPT_SECONDS)

    assert received == chunks
    assert call.acked == len(chunks)
