# Copyright 2026 ConveyorQ
#
# SPDX-License-Identifier: Apache-2.0

"""Error types raised across the Conveyor SDK."""

from __future__ import annotations

from typing import Optional


class ConveyorError(Exception):
    """Base class for SDK-level failures that are not a server error.

    Covers configuration, codec, and protocol violations surfaced by the SDK
    itself rather than returned by the server.
    """


class SkipRetry(ConveyorError):
    """Raise from a handler for a permanent failure that retrying cannot fix.

    The server archives (dead-letters) the task immediately instead of retrying
    it -- use it for a malformed payload or a rejected business rule. Any other
    exception raised from a handler is treated as a transient failure and the
    task is retried.

    The optional ``cause`` is the underlying error, preserved for logging.
    """

    def __init__(self, message: str, cause: Optional[BaseException] = None) -> None:
        super().__init__(message)
        self.__cause__ = cause


def skip_retry(message: str, cause: Optional[BaseException] = None) -> SkipRetry:
    """Build a :class:`SkipRetry`; ``raise skip_retry("bad payload")`` dead-letters the task."""
    return SkipRetry(message, cause)


class DuplicateTaskError(ConveyorError):
    """Raised by :meth:`Client.enqueue` when a unique task already exists.

    Reported when a task carrying the same uniqueness key is still incomplete,
    so the enqueue was rejected by the server with ``already_exists``.
    """

    def __init__(self, message: str = "conveyor: duplicate task") -> None:
        super().__init__(message)
