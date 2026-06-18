# Copyright 2026 ConveyorQ
#
# SPDX-License-Identifier: Apache-2.0

"""The Task type a producer enqueues and a worker receives."""

from __future__ import annotations

from typing import Any, Mapping, Optional

from . import codec
from .codec import ContentType, Payload


class Task:
    """Both what a producer enqueues and what a handler receives.

    A producer builds one with :func:`new_task`; a worker is handed one per
    dispatch. The payload is opaque bytes plus a content type -- decode it with
    :meth:`json` (or read :meth:`payload` for the raw bytes).
    """

    __slots__ = (
        "type",
        "content_type",
        "metadata",
        "id",
        "queue",
        "retried",
        "max_retry",
        "_data",
    )

    def __init__(
        self,
        *,
        type: str,
        content_type: str,
        data: bytes,
        metadata: Optional[Mapping[str, str]] = None,
        id: str = "",
        queue: str = "",
        retried: int = 0,
        max_retry: int = 0,
    ) -> None:
        #: The handler routing key, e.g. ``"email:welcome"``.
        self.type = type
        #: How the payload is encoded, e.g. ``"application/json"``.
        self.content_type = content_type
        #: User tags and trace propagation carried with the task.
        self.metadata: dict[str, str] = dict(metadata or {})
        #: The task id; assigned by the server, set on dispatched tasks.
        self.id = id
        #: The queue the task belongs to; set on dispatched tasks.
        self.queue = queue
        #: How many times this task has already been retried.
        self.retried = retried
        #: The retry budget before the task is dead-lettered.
        self.max_retry = max_retry
        self._data = data

    @property
    def payload(self) -> bytes:
        """The raw payload bytes."""
        return self._data

    def json(self) -> Any:
        """Decode a JSON payload into a value; raises if the content type is not JSON."""
        return codec.decode_json(self._data, self.content_type)

    def text(self) -> str:
        """Decode the payload bytes as a UTF-8 string."""
        return codec.decode_text(self._data)


def new_task(type: str, payload: Payload, *, metadata: Optional[Mapping[str, str]] = None) -> Task:
    """Build a task to enqueue from a routing type and an encoded payload.

    Construct the payload with a codec -- ``conveyorq.json(value)`` (the default)
    or ``conveyorq.binary(data)``::

        client.enqueue(new_task("email:welcome", conveyorq.json({"user_id": 42})))
    """
    return Task(
        type=type,
        content_type=payload.content_type or ContentType.JSON,
        data=payload.data,
        metadata=metadata,
    )
