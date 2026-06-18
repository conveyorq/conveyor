# Copyright 2026 ConveyorQ
#
# SPDX-License-Identifier: Apache-2.0

"""Synchronous wrappers over the asyncio client and worker.

These let code that is not itself async use Conveyor. :class:`SyncClient` runs a
private event loop on a background thread and exposes blocking ``enqueue`` /
``get_task`` calls. :class:`SyncWorker` owns the event loop for the duration of a
blocking ``run`` and accepts plain (non-``async``) handlers, which it runs on a
thread pool. Both wrap the same async core.
"""

from __future__ import annotations

import asyncio
import threading
from concurrent.futures import Future
from typing import Awaitable, Mapping, Optional, Sequence, TypeVar

from .client import Client
from .encryption import Encryptor
from .mux import Mux
from .options import EnqueueMiddleware, TaskInfo
from .task import Task
from .worker import Worker

_T = TypeVar("_T")


class SyncClient:
    """A blocking producer client wrapping :class:`Client`.

    It runs an event loop on a daemon thread; every method submits its coroutine
    to that loop and blocks for the result::

        with SyncClient("http://localhost:8080", token=token) as client:
            client.enqueue(new_task("email:welcome", json({"user_id": 42})))
    """

    def __init__(
        self,
        base_url: str,
        *,
        token: Optional[str] = None,
        encryptor: Optional[Encryptor] = None,
        enqueue_middleware: Optional[Sequence[EnqueueMiddleware]] = None,
    ) -> None:
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._loop.run_forever, name="conveyorq-client", daemon=True)
        self._thread.start()

        # The client opens its channel lazily on first use, which runs on the
        # background loop via _submit, so the channel binds to that loop.
        self._client = Client(
            base_url,
            token=token,
            encryptor=encryptor,
            enqueue_middleware=enqueue_middleware,
        )

    def __enter__(self) -> "SyncClient":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def enqueue(self, task: Task, **kwargs: object) -> TaskInfo:
        """Commit one task and return its info; see :meth:`Client.enqueue`."""
        return self._submit(self._client.enqueue(task, **kwargs))

    def get_task(self, task_id: str) -> TaskInfo:
        """Return the current state of one task; see :meth:`Client.get_task`."""
        return self._submit(self._client.get_task(task_id))

    def close(self) -> None:
        """Close the client and stop the background event loop."""
        self._submit(self._client.close())
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join()
        self._loop.close()

    def _submit(self, coro: Awaitable[_T]) -> _T:
        future: Future[_T] = asyncio.run_coroutine_threadsafe(coro, self._loop)  # type: ignore[arg-type]

        return future.result()


class SyncWorker:
    """A blocking worker wrapping :class:`Worker`.

    :meth:`run` owns the event loop and blocks until SIGTERM/SIGINT triggers a
    graceful drain. Handlers registered on the mux may be plain functions (run on
    a thread pool) or ``async def``::

        worker = SyncWorker("http://localhost:8080", queues={"default": 1}, concurrency=8, token=token)
        mux = Mux().handle("email:welcome", send_welcome)
        worker.run(mux)
    """

    def __init__(
        self,
        base_url: str,
        *,
        queues: Mapping[str, int],
        concurrency: int,
        token: Optional[str] = None,
        sdk_version: Optional[str] = None,
        min_server_version: Optional[str] = None,
        encryptor: Optional[Encryptor] = None,
    ) -> None:
        self._base_url = base_url
        self._queues = dict(queues)
        self._concurrency = concurrency
        self._token = token
        self._sdk_version = sdk_version
        self._min_server_version = min_server_version
        self._encryptor = encryptor

    def run(self, mux: Mux) -> None:
        """Run the worker until SIGTERM/SIGINT; blocks the calling thread."""
        asyncio.run(self._run(mux))

    async def _run(self, mux: Mux) -> None:
        worker = Worker(
            self._base_url,
            queues=self._queues,
            concurrency=self._concurrency,
            token=self._token,
            sdk_version=self._sdk_version,
            min_server_version=self._min_server_version,
            encryptor=self._encryptor,
        )
        await worker.run(mux)
