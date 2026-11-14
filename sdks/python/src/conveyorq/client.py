# Copyright 2026 ConveyorQ
#
# SPDX-License-Identifier: Apache-2.0

"""The producer side of Conveyor: commit tasks to the server."""

from __future__ import annotations

import asyncio
import hashlib
from dataclasses import dataclass, field
from typing import AsyncGenerator, Mapping, Optional, Sequence

import grpc

from . import _time
from ._transport import auth_metadata, client_channel_options, create_channel
from .encryption import ENCRYPTION_MARKER_KEY, ENCRYPTION_MARKER_VALUE, Encryptor
from .errors import ConveyorError, DuplicateTaskError
from .gen.conveyor.v1 import service_pb2, service_pb2_grpc, task_pb2
from .options import (
    Dependency,
    DependencyFailure,
    EnqueueFn,
    EnqueueMiddleware,
    EnqueueOptions,
    RetryStrategy,
    TaskInfo,
    TaskState,
)
from .streaming import upload_chunks
from .task import Task

_STATE_NAMES = {
    task_pb2.TASK_STATE_UNSPECIFIED: TaskState.UNSPECIFIED,
    task_pb2.TASK_STATE_SCHEDULED: TaskState.SCHEDULED,
    task_pb2.TASK_STATE_PENDING: TaskState.PENDING,
    task_pb2.TASK_STATE_ACTIVE: TaskState.ACTIVE,
    task_pb2.TASK_STATE_RETRY: TaskState.RETRY,
    task_pb2.TASK_STATE_COMPLETED: TaskState.COMPLETED,
    task_pb2.TASK_STATE_ARCHIVED: TaskState.ARCHIVED,
    task_pb2.TASK_STATE_CANCELED: TaskState.CANCELED,
    task_pb2.TASK_STATE_AGGREGATING: TaskState.AGGREGATING,
    task_pb2.TASK_STATE_BLOCKED: TaskState.BLOCKED,
}

_FAILURE_POLICIES = {
    DependencyFailure.BLOCK: task_pb2.DEPENDENCY_FAILURE_POLICY_BLOCK,
    DependencyFailure.CASCADE_CANCEL: task_pb2.DEPENDENCY_FAILURE_POLICY_CASCADE_CANCEL,
    DependencyFailure.CONTINUE: task_pb2.DEPENDENCY_FAILURE_POLICY_CONTINUE,
}

_RETRY_STRATEGIES = {
    RetryStrategy.DEFAULT: task_pb2.RETRY_STRATEGY_UNSPECIFIED,
    RetryStrategy.EXPONENTIAL: task_pb2.RETRY_STRATEGY_EXPONENTIAL,
    RetryStrategy.LINEAR: task_pb2.RETRY_STRATEGY_LINEAR,
    RetryStrategy.FIXED: task_pb2.RETRY_STRATEGY_FIXED,
}


@dataclass
class TxTask:
    """Pairs a task with its per-task enqueue options for :meth:`Client.enqueue_tx`.

    ``options`` accepts the same keyword fields as :meth:`Client.enqueue`, e.g.
    ``TxTask(new_task("a", json(1)), {"queue": "billing"})``. A single atomic
    enqueue may therefore span queues, priorities, and schedules.
    """

    task: "Task"
    options: Mapping[str, object] = field(default_factory=dict)


class Client:
    """The producer side of Conveyor: commits tasks to the server.

    Build one with a base URL and options, then :meth:`enqueue`::

        async with Client("http://localhost:8080", token=token) as client:
            await client.enqueue(
                new_task("email:welcome", json({"user_id": 42})),
                queue="critical",
            )

    It is also usable without the context manager; call :meth:`close` when done.
    """

    def __init__(
        self,
        base_url: str,
        *,
        token: Optional[str] = None,
        encryptor: Optional[Encryptor] = None,
        enqueue_middleware: Optional[Sequence[EnqueueMiddleware]] = None,
    ) -> None:
        self._base_url = base_url
        self._metadata = auth_metadata(token)
        self._encryptor = encryptor
        self._middleware = list(enqueue_middleware or [])
        self._channel: Optional[grpc.aio.Channel] = None
        self._stub: Optional[service_pb2_grpc.TaskServiceStub] = None

    async def __aenter__(self) -> "Client":
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    async def close(self) -> None:
        """Close the underlying gRPC channel if one was opened."""
        if self._channel is not None:
            await self._channel.close()

    def _connect(self) -> "service_pb2_grpc.TaskServiceStub":
        """Open the channel lazily, inside the running loop that will use it.

        gRPC binds a channel to the event loop active when it is created, so the
        channel is built on first use rather than in ``__init__`` (which may run
        on a different loop, or none).
        """
        if self._stub is None:
            self._channel = create_channel(self._base_url, client_channel_options())
            self._stub = service_pb2_grpc.TaskServiceStub(self._channel)

        return self._stub

    async def enqueue(self, task: "Task", **kwargs: object) -> TaskInfo:
        """Commit one task and return its server-assigned info.

        Accepts every field of :class:`EnqueueOptions` as a keyword argument,
        e.g. ``queue="critical"``, ``max_retry=5``, ``unique=timedelta(hours=1)``.
        Raises :class:`DuplicateTaskError` when a unique task with the same key
        is still incomplete.

        A payload larger than the server's 1 MiB inline limit, or one built with
        :func:`stream`, is uploaded in chunks through the streaming enqueue; the
        task is committed only once the whole payload is uploaded.
        """
        options = EnqueueOptions(**kwargs)  # type: ignore[arg-type]
        unique_key = _resolve_unique_key(task, options)

        enqueue: EnqueueFn = lambda decorated, settings: self._commit(decorated, settings, unique_key)

        for middleware in reversed(self._middleware):
            enqueue = middleware(enqueue)

        return await enqueue(task, options)

    async def enqueue_tx(self, tasks: "Sequence[TxTask]") -> "list[TaskInfo]":
        """Commit every task atomically: either all are enqueued or none are.

        If any task fails (a duplicate unique key, a unique-key collision between
        two tasks in the call, or an invalid task), no task is committed and the
        offending task's error is raised. On success it returns the committed
        tasks in the order given. Re-committing an existing task id is a no-op
        success that does not abort the call.

        Unlike :meth:`enqueue`, it does not run the enqueue middleware: the
        middleware decorates a single-task commit, which the all-or-nothing path
        does not model. It carries inline payloads only, so a streamed or
        larger-than-inline payload is rejected; enqueue such a task alone.
        """
        if not tasks:
            raise ConveyorError("conveyor: at least one task is required")

        stub = self._connect()
        requests = []

        for index, item in enumerate(tasks):
            if item.task._streamed():
                raise ConveyorError(
                    f"conveyor: task {index} has a streamed or larger-than-inline payload, "
                    "which enqueue_tx cannot carry; use enqueue"
                )

            options = EnqueueOptions(**item.options)  # type: ignore[arg-type]
            unique_key = _resolve_unique_key(item.task, options)
            requests.append(self._build_request(item.task, options, unique_key))

        request = service_pb2.EnqueueTxRequest(tasks=requests)

        try:
            response = await stub.EnqueueTx(request, metadata=self._metadata)
        except grpc.aio.AioRpcError as error:
            raise _map_client_error(error) from error

        return [_task_info_from_proto(task) for task in response.tasks]

    async def get_task(self, task_id: str) -> TaskInfo:
        """Return the current state of one task, or raise if the id is unknown."""
        if task_id == "":
            raise ConveyorError("conveyor: task id is required")

        request = service_pb2.GetTaskRequest(id=task_id)
        stub = self._connect()

        try:
            response = await stub.GetTask(request, metadata=self._metadata)
        except grpc.aio.AioRpcError as error:
            raise _map_client_error(error) from error

        return _task_info_from_proto(response.task)

    async def _commit(self, task: "Task", options: EnqueueOptions, unique_key: str) -> TaskInfo:
        if task._streamed():
            return await self._enqueue_stream(task, options, unique_key)

        stub = self._connect()
        request = self._build_request(task, options, unique_key)

        try:
            response = await stub.Enqueue(request, metadata=self._metadata)
        except grpc.aio.AioRpcError as error:
            raise _map_client_error(error) from error

        return _task_info_from_proto(response.task)

    async def _enqueue_stream(self, task: "Task", options: EnqueueOptions, unique_key: str) -> TaskInfo:
        """Upload a task whose payload is a stream or exceeds the inline limit
        through the streaming enqueue. With encryption configured each chunk is
        sealed on its own, so the payload is never held whole."""
        header_task = Task(type=task.type, content_type=task.content_type, data=b"", metadata=task.metadata)
        header = self._build_request(header_task, options, unique_key)

        if self._encryptor is not None:
            header.metadata[ENCRYPTION_MARKER_KEY] = ENCRYPTION_MARKER_VALUE

        chunks = upload_chunks(task.payload, task._source, self._encryptor)
        call = self._connect().EnqueueStream(metadata=self._metadata)

        try:
            await _send_upload(call, header, chunks)
            response = await call
        except grpc.aio.AioRpcError as error:
            raise _map_client_error(error) from error

        return _task_info_from_proto(response.task)

    def _build_request(
        self, task: "Task", options: EnqueueOptions, unique_key: str
    ) -> "service_pb2.EnqueueRequest":
        """Build the wire request from a task and its resolved options, sealing
        the payload when encryption is configured. It is the single source of
        truth shared by :meth:`enqueue` and :meth:`enqueue_tx`.
        """
        payload = task.payload
        metadata = {**task.metadata, **options.metadata}

        if self._encryptor is not None and len(payload) > 0:
            payload = self._encryptor.encrypt(payload)
            metadata[ENCRYPTION_MARKER_KEY] = ENCRYPTION_MARKER_VALUE

        request = service_pb2.EnqueueRequest(
            task_id=options.task_id or "",
            queue=options.queue or "",
            type=task.type,
            payload=payload,
            content_type=task.content_type,
            metadata=metadata,
            max_retry=options.max_retry or 0,
            priority=options.priority or 0,
            unique_key=unique_key,
            group=options.group or "",
            concurrency_key=options.concurrency_key or "",
        )

        if options.timeout is not None:
            request.timeout.CopyFrom(_time.duration_proto(options.timeout))

        if options.deadline is not None:
            request.deadline.CopyFrom(_time.timestamp_proto(options.deadline))

        if options.process_at is not None:
            request.process_at.CopyFrom(_time.timestamp_proto(options.process_at))

        if options.process_in is not None:
            request.process_in.CopyFrom(_time.duration_proto(options.process_in))

        if options.retention is not None:
            request.retention.CopyFrom(_time.duration_proto(options.retention))

        if options.unique is not None:
            request.unique_ttl.CopyFrom(_time.duration_proto(options.unique))

        if options.expires_in is not None:
            request.expires_in.CopyFrom(_time.duration_proto(options.expires_in))

        if options.expires_at is not None:
            request.expires_at.CopyFrom(_time.timestamp_proto(options.expires_at))

        for dependency in options.depends_on:
            edge = dependency if isinstance(dependency, Dependency) else Dependency(task_id=dependency)
            request.depends_on.add(
                task_id=edge.task_id,
                on_failure=_FAILURE_POLICIES[edge.on_failure],
            )

        if options.retry_policy is not None:
            policy = options.retry_policy
            request.retry_policy.strategy = _RETRY_STRATEGIES[policy.strategy]
            if policy.base is not None:
                request.retry_policy.base.CopyFrom(_time.duration_proto(policy.base))
            if policy.max is not None:
                request.retry_policy.max.CopyFrom(_time.duration_proto(policy.max))

        return request


def _resolve_unique_key(task: "Task", options: EnqueueOptions) -> str:
    """Validate an enqueue's mutually exclusive options and return the effective
    unique key, derived over the plaintext payload (before any encryption) so
    identical work still collides under uniqueness while the server only ever
    sees ciphertext.
    """
    if options.process_at is not None and options.process_in is not None:
        raise ConveyorError("conveyor: process_at and process_in are mutually exclusive")

    if options.expires_at is not None and options.expires_in is not None:
        raise ConveyorError("conveyor: expires_at and expires_in are mutually exclusive")

    unique_key = options.unique_key or ""
    if unique_key != "" or options.unique is None:
        return unique_key

    if task._source is not None:
        raise ConveyorError("conveyor: unique on a streamed payload needs an explicit unique_key")

    return _derived_unique_key(task.type, task.payload)


async def _send_upload(
    call: "grpc.aio.StreamUnaryCall",
    header: "service_pb2.EnqueueRequest",
    chunks: AsyncGenerator[bytes, None],
) -> None:
    """Send the header, every chunk, then the commit stating their total size,
    and half-close the upload.

    The server commits the task only on a commit whose size matches what it
    received, so the commit is sent only once the source is exhausted. A
    failing chunk source cancels the call instead, and its error propagates.
    When the server ends the upload early (an idempotent retry, or a rejection)
    the remaining chunks are not read, and awaiting the call yields its answer.
    """
    try:
        await call.write(service_pb2.EnqueueStreamRequest(header=header))
        size = 0

        async for data in chunks:
            await call.write(service_pb2.EnqueueStreamRequest(chunk=data))
            size += len(data)

        await call.write(service_pb2.EnqueueStreamRequest(commit=service_pb2.PayloadCommit(size=size)))
        await call.done_writing()
    except (asyncio.InvalidStateError, grpc.aio.AioRpcError):
        return
    except BaseException:
        call.cancel()
        raise
    finally:
        await chunks.aclose()


def _derived_unique_key(task_type: str, payload: bytes) -> str:
    """Compute the default uniqueness key: SHA-256 of type, a 0x00 separator, and
    the payload -- matching the Go and TypeScript SDKs so the same logical task
    dedups across languages.
    """
    digest = hashlib.sha256()
    digest.update(task_type.encode("utf-8"))
    digest.update(b"\x00")
    digest.update(payload)

    return digest.hexdigest()


def _map_client_error(error: "grpc.aio.AioRpcError") -> Exception:
    """Map a duplicate-task server error to :class:`DuplicateTaskError`."""
    if error.code() == grpc.StatusCode.ALREADY_EXISTS:
        return DuplicateTaskError(error.details() or "conveyor: duplicate task")

    return error


def _task_info_from_proto(info: "service_pb2.TaskInfo") -> TaskInfo:
    return TaskInfo(
        id=info.id,
        queue=info.queue,
        type=info.type,
        state=_STATE_NAMES.get(info.state, TaskState.UNSPECIFIED),
        priority=info.priority,
        retried=info.retried,
        max_retry=info.max_retry,
        last_error=info.last_error,
        enqueued_at=_time.datetime_from_timestamp(info.enqueued_at),
        process_at=_time.datetime_from_timestamp(info.process_at),
        completed_at=_time.datetime_from_timestamp(info.completed_at),
        started_at=_time.datetime_from_timestamp(info.started_at),
        progress=info.progress,
        progress_message=info.progress_message,
        payload_size=info.payload_size,
        payload_chunked=info.payload_chunked,
    )
