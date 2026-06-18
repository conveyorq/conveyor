# Copyright 2026 ConveyorQ
#
# SPDX-License-Identifier: Apache-2.0

"""A worker that processes the email queue.

Run it after starting a Conveyor server; it drains gracefully on SIGTERM/SIGINT:

    python worker.py
"""

from __future__ import annotations

import asyncio

from conveyorq import Mux, SkipRetry, Worker

from tasks import EMAIL_QUEUE, REMINDER_EMAIL, WELCOME_EMAIL, load_config


async def main() -> None:
    config = load_config()
    mux = Mux()

    @mux.handler(WELCOME_EMAIL)
    async def send_welcome(task, ctx) -> None:
        email = task.json()
        _require(email.get("to"), "missing recipient")
        print(f"sending welcome to {email['to']} ({email['name']})")
        await _deliver(email["to"], f"Welcome, {email['name']}!")

    @mux.handler(REMINDER_EMAIL)
    async def send_reminder(task, ctx) -> None:
        email = task.json()
        _require(email.get("to"), "missing recipient")
        print(f"sending reminder to {email['to']}: {email['subject']}")
        await _deliver(email["to"], email["body"])

    worker = Worker(
        config.addr,
        queues={EMAIL_QUEUE: 1},
        concurrency=8,
        token=config.token or None,
    )

    print(f"worker started against {config.addr}; serving queue {EMAIL_QUEUE!r}")
    await worker.run(mux)
    print("worker stopped")


async def _deliver(to: str, body: str) -> None:
    """Stand in for a real email send (an SMTP/API call)."""
    await asyncio.sleep(0.1)


def _require(value: object, message: str) -> None:
    """Raise SkipRetry for a permanently bad payload -- retrying cannot fix it."""
    if not value:
        raise SkipRetry(f"conveyor-example: {message}")


if __name__ == "__main__":
    asyncio.run(main())
