# Copyright 2026 ConveyorQ
#
# SPDX-License-Identifier: Apache-2.0

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from typing import List

import grpc

from conveyorq import _time
from conveyorq.gen.conveyor.v1 import service_pb2, task_pb2
from conveyorq.mux import Mux
from conveyorq.worker import _FATAL_CODES, _full_jitter, _hello, _Session, _SessionConfig


def test_full_jitter_stays_within_the_attempt_ceiling():
    for attempt in range(0, 8):
        ceiling = min(30.0, 0.5 * (2 ** attempt))
        for _ in range(100):
            delay = _full_jitter(attempt)
            assert 0.0 <= delay < ceiling or delay == 0.0


def test_full_jitter_is_capped_at_max():
    assert _full_jitter(100) < 30.0


def test_fatal_codes_are_the_non_recoverable_set():
    assert grpc.StatusCode.UNAUTHENTICATED in _FATAL_CODES
    assert grpc.StatusCode.PERMISSION_DENIED in _FATAL_CODES
    assert grpc.StatusCode.INVALID_ARGUMENT in _FATAL_CODES
    # A rejected session contract (a FailedPrecondition Hello) is fatal too, and
    # the fatal set matches the Go and TypeScript SDKs.
    assert grpc.StatusCode.FAILED_PRECONDITION in _FATAL_CODES
    # Transient failures must reconnect, not stop the worker.
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
