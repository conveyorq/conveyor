# Copyright 2026 ConveyorQ
#
# SPDX-License-Identifier: Apache-2.0

"""Handler-invocation behaviour: sync/async dispatch and outcome mapping."""

from __future__ import annotations

import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor
from functools import partial

import pytest

from conveyorq import SkipRetry, json, new_task
from conveyorq.gen.conveyor.v1 import service_pb2
from conveyorq.mux import HandlerContext
from conveyorq.worker import _invoke, _run_handler


@pytest.fixture
def executor():
    pool = ThreadPoolExecutor(max_workers=2)
    yield pool
    pool.shutdown(wait=False)


def _ctx() -> HandlerContext:
    return HandlerContext(asyncio.Event(), None)


def _task():
    return new_task("t", json({"n": 1}))


async def test_async_handler_runs_on_the_loop(executor):
    loop_thread = threading.get_ident()
    seen = {}

    async def handler(task, ctx):
        seen["thread"] = threading.get_ident()

    outcome, msg = await _run_handler(handler, _task(), _ctx(), executor)

    assert outcome == service_pb2.TASK_OUTCOME_SUCCESS
    assert msg == ""
    assert seen["thread"] == loop_thread  # async handlers do not leave the loop


async def test_sync_handler_runs_off_the_loop(executor):
    loop_thread = threading.get_ident()
    seen = {}

    def handler(task, ctx):
        seen["thread"] = threading.get_ident()

    outcome, _ = await _run_handler(handler, _task(), _ctx(), executor)

    assert outcome == service_pb2.TASK_OUTCOME_SUCCESS
    assert seen["thread"] != loop_thread  # sync handlers run on the executor


async def test_partial_wrapped_coroutine_is_awaited_not_threaded(executor):
    # A functools.partial of a coroutine function must still be awaited, or its
    # coroutine would be created on a thread and never awaited (silent no-op).
    ran = asyncio.Event()

    async def handler(prefix, task, ctx):
        ran.set()

    wrapped = partial(handler, "prefix")
    outcome, _ = await _run_handler(wrapped, _task(), _ctx(), executor)

    assert outcome == service_pb2.TASK_OUTCOME_SUCCESS
    assert ran.is_set()


async def test_skip_retry_maps_to_skip_retry(executor):
    async def handler(task, ctx):
        raise SkipRetry("permanent")

    outcome, msg = await _run_handler(handler, _task(), _ctx(), executor)

    assert outcome == service_pb2.TASK_OUTCOME_SKIP_RETRY
    assert "permanent" in msg


async def test_any_other_error_maps_to_retry(executor):
    async def handler(task, ctx):
        raise RuntimeError("boom")

    outcome, msg = await _run_handler(handler, _task(), _ctx(), executor)

    assert outcome == service_pb2.TASK_OUTCOME_RETRY
    assert "boom" in msg


async def test_sync_handler_skip_retry_propagates(executor):
    def handler(task, ctx):
        raise SkipRetry("bad payload")

    outcome, msg = await _run_handler(handler, _task(), _ctx(), executor)

    assert outcome == service_pb2.TASK_OUTCOME_SKIP_RETRY
    assert "bad payload" in msg


async def test_invoke_passes_payload_and_ctx(executor):
    captured = {}

    async def handler(task, ctx):
        captured["task"] = task
        captured["ctx"] = ctx

    ctx = _ctx()
    task = _task()
    await _invoke(handler, task, ctx, executor)

    assert captured["task"] is task
    assert captured["ctx"] is ctx


def test_report_progress_forwards_to_the_reporter():
    sent: list[tuple[int, str]] = []
    ctx = HandlerContext(asyncio.Event(), None, lambda percent, message: sent.append((percent, message)))

    ctx.report_progress(42, "working")

    assert sent == [(42, "working")]


def test_report_progress_without_a_reporter_is_a_noop():
    ctx = HandlerContext(asyncio.Event(), None)

    ctx.report_progress(42, "working")  # no reporter wired: must not raise
