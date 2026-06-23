# Copyright 2026 ConveyorQ
#
# SPDX-License-Identifier: Apache-2.0

"""Routing of dispatched tasks to the handler registered for their type."""

from __future__ import annotations

import asyncio
from typing import Awaitable, Callable, Dict, List, Mapping, Optional, Sequence, Union

from .errors import ConveyorError
from .task import Task


class HandlerContext:
    """Passed to every handler; carries cancellation and the effective deadline.

    :attr:`cancelled` is set when the task's deadline passes or the server
    cancels it. A well-behaved handler observes it -- ``ctx.cancelled.is_set()``
    in sync code, ``await ctx.cancelled.wait()`` raced against work in async
    code -- to stop early. Cancellation is best-effort: a handler that ignores
    it simply runs to completion.
    """

    __slots__ = ("cancelled", "deadline", "_report_progress")

    def __init__(
        self,
        cancelled: asyncio.Event,
        deadline: Optional[float],
        report_progress: Optional[Callable[[int, str], None]] = None,
    ) -> None:
        #: An :class:`asyncio.Event` set on deadline or operator cancel.
        self.cancelled = cancelled
        #: The effective deadline as a UNIX timestamp (seconds), or ``None``.
        self.deadline = deadline
        self._report_progress = report_progress

    def report_progress(self, percent: int, message: str = "") -> None:
        """Record how far the running task has advanced.

        ``percent`` is a completion estimate from 0 to 100 (clamped) and
        ``message`` an optional human-readable status. Progress is advisory: it
        surfaces on the task's status for inspection and never affects
        execution. Consecutive identical reports are coalesced. It is a no-op
        for a batch handler, where progress is per single task.
        """
        if self._report_progress is not None:
            self._report_progress(percent, message)

    def is_cancelled(self) -> bool:
        """Whether the task has been cancelled or its deadline has passed."""
        return self.cancelled.is_set()


# A handler processes one task. Returning marks it completed; raising SkipRetry
# dead-letters it; any other exception retries it. It may be sync or async.
Handler = Callable[[Task, HandlerContext], Union[None, Awaitable[None]]]

# A batch handler processes an aggregation group's members as one delivery.
BatchHandler = Callable[[Sequence[Task], HandlerContext], Union[None, Awaitable[None]]]

# Middleware decorates a handler, outermost first.
Middleware = Callable[[Handler], Handler]

# BatchMiddleware decorates a batch handler, outermost first.
BatchMiddleware = Callable[[BatchHandler], BatchHandler]


class BatchError(Exception):
    """Reports per-member failures from a :class:`BatchHandler`.

    Maps a member task id to its failure; members not listed are treated as
    succeeded. Raise it from a batch handler to fail individual members while
    completing the rest. A member mapped to a :class:`SkipRetry` is archived;
    any other exception retries that member.
    """

    def __init__(self, failures: Mapping[str, BaseException]) -> None:
        super().__init__(f"conveyor: {len(failures)} batch member(s) failed")
        #: Map of member task id to its failure.
        self.failures: dict[str, BaseException] = dict(failures)


class Mux:
    """Routes a task to the handler registered for its type.

    Register single-task handlers with :meth:`handle` (or the :meth:`handler`
    decorator) and batch handlers for aggregation groups with
    :meth:`handle_batch`; decorate them with :meth:`use` / :meth:`use_batch`.
    """

    def __init__(self) -> None:
        self._handlers: Dict[str, Handler] = {}
        self._batch_handlers: Dict[str, BatchHandler] = {}
        self._middleware: List[Middleware] = []
        self._batch_middleware: List[BatchMiddleware] = []

    def handle(self, type: str, handler: Handler) -> "Mux":
        """Register a single-task handler for a task type and return self."""
        if type == "":
            raise ConveyorError("conveyor: handler type is required")

        self._handlers[type] = handler

        return self

    def handler(self, type: str) -> Callable[[Handler], Handler]:
        """Decorator form of :meth:`handle`::

        @mux.handler("email:welcome")
        async def send_welcome(task, ctx): ...
        """

        def register(fn: Handler) -> Handler:
            self.handle(type, fn)

            return fn

        return register

    def handle_batch(self, type: str, handler: BatchHandler) -> "Mux":
        """Register a batch handler for an aggregation-group task type and return self."""
        if type == "":
            raise ConveyorError("conveyor: batch handler type is required")

        self._batch_handlers[type] = handler

        return self

    def batch_handler(self, type: str) -> Callable[[BatchHandler], BatchHandler]:
        """Decorator form of :meth:`handle_batch`."""

        def register(fn: BatchHandler) -> BatchHandler:
            self.handle_batch(type, fn)

            return fn

        return register

    def use(self, *middleware: Middleware) -> "Mux":
        """Append single-task middleware, applied outermost first; returns self."""
        self._middleware.extend(middleware)

        return self

    def use_batch(self, *middleware: BatchMiddleware) -> "Mux":
        """Append batch middleware, applied outermost first; returns self."""
        self._batch_middleware.extend(middleware)

        return self

    def batch_types(self) -> List[str]:
        """The task types registered as batch handlers, advertised to the server."""
        return list(self._batch_handlers.keys())

    def resolve(self, type: str) -> Optional[Handler]:
        """Return the middleware-wrapped single-task handler for a type, if any."""
        handler = self._handlers.get(type)
        if handler is None:
            return None

        for middleware in reversed(self._middleware):
            handler = middleware(handler)

        return handler

    def resolve_batch(self, type: str) -> Optional[BatchHandler]:
        """Return the middleware-wrapped batch handler for a type, if any."""
        handler = self._batch_handlers.get(type)
        if handler is None:
            return None

        for middleware in reversed(self._batch_middleware):
            handler = middleware(handler)

        return handler
