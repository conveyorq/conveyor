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
    EXPECTED_SHA256
                   hex SHA-256 the conformance:large (buffered) and
                   conformance:large-stream (streaming) handlers require of the
                   payload they read; a mismatch fails the task without retry.
    ENCRYPTION_KEY_HEX
                   optional 64 hex chars (a 32-byte AES-256 key); with
                   ENCRYPTION_KEY_ID set too, the worker opens end-to-end
                   encrypted payloads with that single key.
    ENCRYPTION_KEY_ID
                   the id of ENCRYPTION_KEY_HEX.
    CONVEYOR_ADDR  server base URL.
"""

from __future__ import annotations

import asyncio
import hashlib
import os
import sys
from typing import Optional

from conveyorq import ConveyorError, Encryptor, Key, Mux, Worker, new_aes_gcm, skip_retry

_ADDR = os.environ.get("CONVEYOR_ADDR", "http://localhost:8080")
_SCENARIO = os.environ.get("SCENARIO", "sleep")
_SLEEP_S = float(os.environ.get("SLEEP_MS", "0")) / 1000.0
_QUEUE = os.environ.get("QUEUE", "default")
_EXPECTED_SHA256 = os.environ.get("EXPECTED_SHA256", "")
_ENCRYPTION_KEY_HEX = os.environ.get("ENCRYPTION_KEY_HEX", "")
_ENCRYPTION_KEY_ID = os.environ.get("ENCRYPTION_KEY_ID", "")


def _encryptor() -> Optional[Encryptor]:
    """Build the worker's encryptor from the key env vars, or None when unset."""
    if not _ENCRYPTION_KEY_HEX or not _ENCRYPTION_KEY_ID:
        return None

    return new_aes_gcm(_ENCRYPTION_KEY_ID, Key(_ENCRYPTION_KEY_ID, bytes.fromhex(_ENCRYPTION_KEY_HEX)))


def _check_digest(digest: str) -> None:
    """Fail the task without retry when the payload digest is not the expected one."""
    if digest != _EXPECTED_SHA256:
        raise skip_retry(f"payload sha256 {digest}, want {_EXPECTED_SHA256}")


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

    @mux.handler("conformance:large")
    async def _large(task, ctx) -> None:  # noqa: ANN001, ARG001
        _check_digest(hashlib.sha256(task.payload).hexdigest())

    @mux.stream_handler("conformance:large-stream")
    async def _large_stream(task, ctx) -> None:  # noqa: ANN001, ARG001
        digest = hashlib.sha256()

        async for chunk in task.payload_stream:
            digest.update(chunk)

        _check_digest(digest.hexdigest())

    worker = Worker(_ADDR, queues={_QUEUE: 1}, concurrency=8, encryptor=_encryptor())
    await worker.run(mux)


if __name__ == "__main__":
    asyncio.run(main())
