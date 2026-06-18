# Copyright 2026 ConveyorQ
#
# SPDX-License-Identifier: Apache-2.0

"""End-to-end tests against a live conveyord.

Start a dev server (``conveyord --dev``) and point the SDK at it with
``CONVEYOR_ADDR=http://localhost:8080``; an auth token, if the server requires
one, goes in ``CONVEYOR_TOKEN``. The whole module is skipped when no address is
set, so the default unit-test run needs no server.
"""

from __future__ import annotations

import asyncio
import os
import uuid

import pytest

from conveyorq import Client, Mux, SkipRetry, Worker, json, new_task

ADDR = os.environ.get("CONVEYOR_ADDR")
TOKEN = os.environ.get("CONVEYOR_TOKEN")

pytestmark = pytest.mark.skipif(not ADDR, reason="set CONVEYOR_ADDR to run integration tests")


async def _run_worker_until(mux: Mux, stop: asyncio.Event, *, concurrency: int = 4) -> asyncio.Task:
    worker = Worker(ADDR, queues={"default": 1}, concurrency=concurrency, token=TOKEN)
    task = asyncio.create_task(worker.run(mux, stop=stop, install_signal_handlers=False))

    return task


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
