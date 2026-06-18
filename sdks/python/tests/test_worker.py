# Copyright 2026 ConveyorQ
#
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime, timedelta, timezone

import grpc

from conveyorq import _time
from conveyorq.worker import _FATAL_CODES, _full_jitter, _hello, _SessionConfig


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
