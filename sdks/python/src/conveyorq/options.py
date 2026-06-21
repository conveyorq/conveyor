# Copyright 2026 ConveyorQ
#
# SPDX-License-Identifier: Apache-2.0

"""Producer-side option and result types."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Awaitable, Callable, Optional

from .task import Task


class TaskState(str, Enum):
    """The lifecycle state of a task, as reported by :meth:`Client.get_task`."""

    UNSPECIFIED = "unspecified"
    SCHEDULED = "scheduled"
    PENDING = "pending"
    ACTIVE = "active"
    RETRY = "retry"
    COMPLETED = "completed"
    ARCHIVED = "archived"
    CANCELED = "canceled"
    AGGREGATING = "aggregating"
    BLOCKED = "blocked"


class DependencyFailure(str, Enum):
    """Decides a dependent task's fate when a task it depends on fails terminally
    (retries exhausted, skipped, or canceled) instead of succeeding."""

    #: Keep the dependent blocked indefinitely (the default).
    BLOCK = "block"
    #: Cancel the dependent and, in turn, its own dependents.
    CASCADE_CANCEL = "cascade-cancel"
    #: Treat the failed dependency as satisfied so the dependent proceeds.
    CONTINUE = "continue"


@dataclass(frozen=True)
class Dependency:
    """One task a task waits for. The dependent stays blocked until the
    referenced task reaches a terminal success; ``on_failure`` decides what
    happens if it fails terminally instead."""

    #: The id of the task that must finish first.
    task_id: str
    #: Policy applied when the dependency fails terminally.
    on_failure: DependencyFailure = DependencyFailure.BLOCK


@dataclass(frozen=True)
class TaskInfo:
    """The external view of a task returned by the producer API."""

    id: str
    queue: str
    type: str
    state: TaskState
    priority: int
    retried: int
    max_retry: int
    last_error: str
    enqueued_at: Optional[datetime] = None
    process_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    started_at: Optional[datetime] = None


@dataclass
class EnqueueOptions:
    """Per-enqueue settings. Durations are :class:`~datetime.timedelta`; absolute
    times are :class:`~datetime.datetime`. Every field is optional; an unset
    field takes the server default.
    """

    #: A client-chosen task id, making enqueue retries idempotent.
    task_id: Optional[str] = None
    #: Target queue; defaults to ``"default"``.
    queue: Optional[str] = None
    #: Retries before the task is dead-lettered; ``None`` selects the server default.
    max_retry: Optional[int] = None
    #: Dispatch priority within a queue, 1 (lowest) to 9 (highest).
    priority: Optional[int] = None
    #: Per-attempt timeout; the handler's context is cancelled after it.
    timeout: Optional[timedelta] = None
    #: Absolute time after which the task must not run.
    deadline: Optional[datetime] = None
    #: Delay execution until this absolute time. Exclusive with ``process_in``.
    process_at: Optional[datetime] = None
    #: Delay execution by this duration. Exclusive with ``process_at``.
    process_in: Optional[timedelta] = None
    #: Keep the completed task visible for this long before purge.
    retention: Optional[timedelta] = None
    #: Reject duplicates of this task for this long (uniqueness TTL).
    unique: Optional[timedelta] = None
    #: Explicit uniqueness key; defaults to a hash of type + payload.
    unique_key: Optional[str] = None
    #: Make the task a member of the named aggregation group.
    group: Optional[str] = None
    #: Archive the task if it is not dispatched within this duration. Exclusive with ``expires_at``.
    expires_in: Optional[timedelta] = None
    #: Archive the task if it is not dispatched by this absolute time. Exclusive with ``expires_in``.
    expires_at: Optional[datetime] = None
    #: Tasks this task waits for, building a workflow: it stays blocked until each
    #: reaches a terminal success. A plain string is a dependency with the default
    #: block-on-failure policy; a :class:`Dependency` sets an explicit policy.
    #: Dependencies must be acyclic.
    depends_on: list[str | Dependency] = field(default_factory=list)
    #: Extra metadata merged onto the task before commit.
    metadata: dict[str, str] = field(default_factory=dict)


# Commits a task and returns its info; the unit an enqueue middleware wraps.
EnqueueFn = Callable[[Task, EnqueueOptions], Awaitable[TaskInfo]]

# Decorates the enqueue path, outermost first -- the client-side counterpart of
# mux middleware: inject metadata, enforce policy, or record metrics.
EnqueueMiddleware = Callable[[EnqueueFn], EnqueueFn]
