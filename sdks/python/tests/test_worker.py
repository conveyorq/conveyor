# Copyright 2026 ConveyorQ
#
# SPDX-License-Identifier: Apache-2.0

import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from typing import List

import grpc
import pytest

from conveyorq import _time
from conveyorq.errors import ConveyorError
from conveyorq.gen.conveyor.v1 import service_pb2, task_pb2
from conveyorq.mux import Mux
from conveyorq.worker import _FATAL_CODES, Worker, _full_jitter, _hello, _Session, _SessionConfig


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
