# Copyright 2026 ConveyorQ
#
# SPDX-License-Identifier: Apache-2.0

"""The task contract shared by the producer and the worker.

Both sides import these names and payload shapes, so they agree on the JSON
encoding of each task type -- the only thing the queue requires of two peers.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

#: The queue all email work flows through.
EMAIL_QUEUE = "email"

#: Task type: a one-time welcome email.
WELCOME_EMAIL = "email:welcome"

#: Task type: a scheduled reminder email.
REMINDER_EMAIL = "email:reminder"


@dataclass
class WelcomeEmail:
    """Payload of a :data:`WELCOME_EMAIL` task."""

    user_id: str
    to: str
    name: str


@dataclass
class ReminderEmail:
    """Payload of a :data:`REMINDER_EMAIL` task."""

    user_id: str
    to: str
    subject: str
    body: str


@dataclass
class Config:
    """Connection and policy settings read from the environment."""

    addr: str
    token: str
    max_retry: int


def load_config() -> Config:
    """Read the server address, token, and retry budget from the environment."""
    return Config(
        addr=os.environ.get("CONVEYOR_ADDR", "http://localhost:8080"),
        token=os.environ.get("CONVEYOR_TOKEN", ""),
        max_retry=int(os.environ.get("CONVEYOR_MAX_RETRY", "10")),
    )
