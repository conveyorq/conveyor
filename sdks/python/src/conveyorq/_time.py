# Copyright 2026 ConveyorQ
#
# SPDX-License-Identifier: Apache-2.0

"""Conversions between Python time types and protobuf well-known types."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional

from google.protobuf.duration_pb2 import Duration
from google.protobuf.timestamp_pb2 import Timestamp


def duration_proto(value: timedelta) -> Duration:
    """Build a protobuf :class:`Duration` from a :class:`~datetime.timedelta`."""
    proto = Duration()
    proto.FromTimedelta(value)

    return proto


def timestamp_proto(value: datetime) -> Timestamp:
    """Build a protobuf :class:`Timestamp` from a :class:`~datetime.datetime`.

    A naive datetime is interpreted as UTC, matching protobuf's own convention.
    """
    proto = Timestamp()
    proto.FromDatetime(value)

    return proto


def duration_seconds(value: Optional[Duration]) -> float:
    """Read a protobuf :class:`Duration` as seconds; an unset duration is 0."""
    if value is None:
        return 0.0

    return value.seconds + value.nanos / 1_000_000_000


def datetime_from_timestamp(value: Optional[Timestamp]) -> Optional[datetime]:
    """Convert an optional protobuf :class:`Timestamp` to a timezone-aware UTC datetime."""
    if value is None or (value.seconds == 0 and value.nanos == 0):
        return None

    return value.ToDatetime(tzinfo=timezone.utc)
