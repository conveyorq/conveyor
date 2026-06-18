# Copyright 2026 ConveyorQ
#
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime, timedelta, timezone

import pytest

from conveyorq import Client, ConveyorError, json, new_task
from conveyorq.client import _derived_unique_key


def test_derived_unique_key_matches_cross_language_formula():
    # sha256(type + 0x00 + payload), hex -- identical in the Go and TS SDKs.
    assert (
        _derived_unique_key("t", b"x")
        == "6fca3fcfeb33651f070e9903370a070344d3659d7cc8190fc4e22838220b2128"
    )


def test_derived_unique_key_is_stable_and_payload_sensitive():
    assert _derived_unique_key("t", b"a") == _derived_unique_key("t", b"a")
    assert _derived_unique_key("t", b"a") != _derived_unique_key("t", b"b")


async def test_enqueue_rejects_conflicting_schedule_options():
    client = Client("http://localhost:9", token="x")

    try:
        with pytest.raises(ConveyorError):
            await client.enqueue(
                new_task("t", json(1)),
                process_at=datetime.now(timezone.utc),
                process_in=timedelta(seconds=5),
            )
    finally:
        await client.close()


async def test_enqueue_rejects_conflicting_expiry_options():
    client = Client("http://localhost:9", token="x")

    try:
        with pytest.raises(ConveyorError):
            await client.enqueue(
                new_task("t", json(1)),
                expires_at=datetime.now(timezone.utc),
                expires_in=timedelta(seconds=5),
            )
    finally:
        await client.close()


async def test_get_task_rejects_empty_id():
    client = Client("http://localhost:9", token="x")

    try:
        with pytest.raises(ConveyorError):
            await client.get_task("")
    finally:
        await client.close()


def test_worker_validates_configuration():
    from conveyorq import Worker

    with pytest.raises(ConveyorError):
        Worker("http://localhost:9", queues={}, concurrency=1)

    with pytest.raises(ConveyorError):
        Worker("http://localhost:9", queues={"default": 1}, concurrency=0)
