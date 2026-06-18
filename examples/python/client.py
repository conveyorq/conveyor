# Copyright 2026 ConveyorQ
#
# SPDX-License-Identifier: Apache-2.0

"""A small CLI producer that enqueues email work.

Usage:
    python client.py welcome <to> <name>
    python client.py reminder <to> <subject> [delay_minutes]
"""

from __future__ import annotations

import asyncio
import dataclasses
import sys
from datetime import timedelta

from conveyorq import Client, DuplicateTaskError, json, new_task

from tasks import (
    EMAIL_QUEUE,
    REMINDER_EMAIL,
    WELCOME_EMAIL,
    ReminderEmail,
    WelcomeEmail,
    load_config,
)

ONE_DAY = timedelta(days=1)


async def main() -> None:
    config = load_config()
    args = sys.argv[1:]
    command = args[0] if args else ""

    async with Client(config.addr, token=config.token or None) as client:
        if command == "welcome":
            await _enqueue_welcome(client, config.max_retry, args[1:])
        elif command == "reminder":
            await _enqueue_reminder(client, config.max_retry, args[1:])
        else:
            _usage()


async def _enqueue_welcome(client: Client, max_retry: int, args: list[str]) -> None:
    if len(args) < 2:
        _usage()
        return

    to, name = args[0], args[1]
    payload = WelcomeEmail(user_id=to, to=to, name=name)

    try:
        # A unique key plus a 24h TTL makes re-running this a no-op: at most one
        # pending welcome per recipient, so a retry or a double click never
        # sends two.
        info = await client.enqueue(
            new_task(WELCOME_EMAIL, json(dataclasses.asdict(payload))),
            queue=EMAIL_QUEUE,
            max_retry=max_retry,
            unique_key=f"welcome:{to}",
            unique=ONE_DAY,
        )
        print(f"enqueued welcome {info.id} ({info.state.value})")
    except DuplicateTaskError:
        print(f"welcome for {to} already pending — skipped")


async def _enqueue_reminder(client: Client, max_retry: int, args: list[str]) -> None:
    if len(args) < 2:
        _usage()
        return

    to, subject = args[0], args[1]
    delay_minutes = int(args[2]) if len(args) > 2 else 0
    payload = ReminderEmail(user_id=to, to=to, subject=subject, body=f"Reminder: {subject}")

    info = await client.enqueue(
        new_task(REMINDER_EMAIL, json(dataclasses.asdict(payload))),
        queue=EMAIL_QUEUE,
        max_retry=max_retry,
        process_in=timedelta(minutes=delay_minutes),
    )
    print(f"enqueued reminder {info.id} ({info.state.value}), due in {delay_minutes}m")


def _usage() -> None:
    print(__doc__)
    sys.exit(2)


if __name__ == "__main__":
    asyncio.run(main())
