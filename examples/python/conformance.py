# Copyright 2026 ConveyorQ
#
# SPDX-License-Identifier: Apache-2.0

"""Conformance worker for the cross-SDK protocol suite (see conformance/ in the
repo root). It is not a usage example: the Go harness spawns it, drives it
through one scenario chosen by env vars, and asserts protocol behavior by
observing the server. Kept deliberately minimal.

    SCENARIO       "sleep" (default): run handlers that sleep SLEEP_MS then succeed.
                   "bad-hello": construct a worker with an invalid queue weight,
                   which must be rejected locally (exit 1), never a reconnect loop.
    SLEEP_MS       handler sleep in milliseconds (sleep scenario).
    CONVEYOR_ADDR  server base URL.
"""

from __future__ import annotations

import asyncio
import os
import sys

from conveyorq import ConveyorError, Mux, Worker

_ADDR = os.environ.get("CONVEYOR_ADDR", "http://localhost:8080")
_SCENARIO = os.environ.get("SCENARIO", "sleep")
_SLEEP_S = float(os.environ.get("SLEEP_MS", "0")) / 1000.0
_QUEUE = os.environ.get("QUEUE", "default")


async def main() -> None:
    if _SCENARIO == "bad-hello":
        try:
            # A zero weight is invalid; the constructor must reject it locally.
            Worker(_ADDR, queues={"default": 0}, concurrency=1)
        except ConveyorError:
            sys.exit(1)

        print("bad-hello: an invalid queue weight was accepted", file=sys.stderr)
        sys.exit(2)

    mux = Mux()

    # Sleep the full duration, ignoring cancellation on purpose: the suite checks
    # that a handler outliving the lease keeps its lease via heartbeats, and that
    # a drain waits for it rather than losing it.
    @mux.handler("conformance:task")
    async def _run(task, ctx) -> None:  # noqa: ANN001, ARG001
        await asyncio.sleep(_SLEEP_S)

    async def _run_batch(tasks, ctx) -> None:  # noqa: ANN001, ARG001
        await asyncio.sleep(_SLEEP_S)

    mux.handle_batch("conformance:batch", _run_batch)

    worker = Worker(_ADDR, queues={_QUEUE: 1}, concurrency=8)
    await worker.run(mux)


if __name__ == "__main__":
    asyncio.run(main())
