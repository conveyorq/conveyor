# Copyright 2026 ConveyorQ
#
# SPDX-License-Identifier: Apache-2.0

import pytest

from conveyorq import ConveyorError, Mux
from conveyorq.task import Task


def _task(task_type: str) -> Task:
    return Task(type=task_type, content_type="application/json", data=b"{}")


def test_handle_routes_by_type():
    mux = Mux()
    mux.handle("a", lambda task, ctx: None)

    assert mux.resolve("a") is not None
    assert mux.resolve("missing") is None


def test_handler_decorator_registers():
    mux = Mux()

    @mux.handler("a")
    async def handle(task, ctx):
        return None

    assert mux.resolve("a") is not None


def test_handle_rejects_empty_type():
    with pytest.raises(ConveyorError):
        Mux().handle("", lambda task, ctx: None)


def test_batch_types_lists_registered_batch_handlers():
    mux = Mux()
    mux.handle_batch("digest", lambda tasks, ctx: None)

    assert mux.batch_types() == ["digest"]
    assert mux.resolve_batch("digest") is not None


def test_middleware_wraps_outermost_first():
    order = []

    def outer(nxt):
        async def wrapped(task, ctx):
            order.append("outer")
            await nxt(task, ctx)

        return wrapped

    def inner(nxt):
        async def wrapped(task, ctx):
            order.append("inner")
            await nxt(task, ctx)

        return wrapped

    async def base(task, ctx):
        order.append("base")

    mux = Mux().use(outer, inner).handle("a", base)
    resolved = mux.resolve("a")

    import asyncio

    asyncio.run(resolved(_task("a"), None))

    assert order == ["outer", "inner", "base"]
