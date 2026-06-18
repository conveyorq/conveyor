# Copyright 2026 ConveyorQ
#
# SPDX-License-Identifier: Apache-2.0

"""The consumer side of Conveyor: open a session, run handlers, report outcomes."""

from __future__ import annotations

import asyncio
import contextlib
import random
import signal
import time
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from typing import Awaitable, Callable, Dict, List, Mapping, Optional, Sequence, Set, Tuple

import grpc

from . import _time
from ._transport import auth_metadata, create_channel, worker_channel_options
from .encryption import ENCRYPTION_MARKER_KEY, Encryptor
from .errors import ConveyorError, SkipRetry
from .gen.conveyor.v1 import service_pb2, service_pb2_grpc, task_pb2
from .mux import BatchError, BatchHandler, Handler, HandlerContext, Mux
from .task import Task

#: The SDK version reported in Hello.
SDK_VERSION = "conveyor-py/0.1.0"

# Reconnection backoff (full jitter), per the wire protocol section 5.9.
_RECONNECT_BASE = 0.5
_RECONNECT_MAX = 30.0

# Default lease/heartbeat fallbacks when Welcome omits them (seconds).
_DEFAULT_LEASE_TTL = 60.0
_DEFAULT_HEARTBEAT = 20.0

# How long graceful drain waits for in-flight tasks before closing the stream.
_DRAIN_GRACE = 25.0

# After half-closing on drain, how long to let the server end the stream cleanly
# before forcing the call down so run() cannot hang.
_CLOSE_GRACE = 5.0

# gRPC status codes that must stop the worker rather than trigger a reconnect:
# bad auth, or a server-side rejection of the session contract (an outdated SDK
# version, a malformed Hello, an unmet minimum server version). gRPC does not
# synthesize INVALID_ARGUMENT for a severed connection (that surfaces as
# UNAVAILABLE), so an INVALID_ARGUMENT here always came from the server.
_FATAL_CODES = frozenset(
    {
        grpc.StatusCode.UNAUTHENTICATED,
        grpc.StatusCode.PERMISSION_DENIED,
        grpc.StatusCode.INVALID_ARGUMENT,
    }
)


class Worker:
    """The consumer side of Conveyor: opens a session, receives dispatched tasks,
    runs the matching handler, and reports each outcome.

    It implements the full worker session protocol -- concurrency-bounded
    dispatch, heartbeats, best-effort cancellation, full-jitter reconnect, and
    graceful drain::

        worker = Worker("http://localhost:8080", queues={"default": 1}, concurrency=10, token=token)
        mux = Mux()

        @mux.handler("email:welcome")
        async def send_welcome(task, ctx):
            ...

        await worker.run(mux)
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
        if not queues:
            raise ConveyorError("conveyor: a worker must declare at least one queue")

        if concurrency <= 0:
            raise ConveyorError("conveyor: worker concurrency must be positive")

        self._base_url = base_url
        self._metadata = auth_metadata(token)
        self._queues = dict(queues)
        self._concurrency = concurrency
        self._sdk_version = sdk_version or SDK_VERSION
        self._min_server_version = min_server_version or ""
        self._encryptor = encryptor

    async def run(
        self,
        mux: Mux,
        *,
        stop: Optional[asyncio.Event] = None,
        install_signal_handlers: bool = True,
    ) -> None:
        """Drive the worker until ``stop`` is set (or SIGTERM/SIGINT), reconnecting
        with full-jitter backoff across transient stream failures.

        Returns once the worker has stopped; raises only on a fatal error (bad
        auth, an unmet minimum server version, a rejected Hello). Pass a
        :class:`asyncio.Event` as ``stop`` to control shutdown yourself, or leave
        ``install_signal_handlers`` enabled to drain on SIGTERM/SIGINT.

        Create the worker and call ``run`` on the same event loop: gRPC binds a
        channel to the loop active when it is created, which is the loop entered
        here (not necessarily the one the constructor ran on).
        """
        stop = stop or asyncio.Event()

        # Build the channel inside the running loop so it is bound to the loop
        # that drives it. A dedicated, concurrency-sized executor runs sync
        # handlers off the event loop without contending on the shared default.
        channel = create_channel(self._base_url, worker_channel_options())
        stub = service_pb2_grpc.WorkerServiceStub(channel)
        executor = ThreadPoolExecutor(max_workers=self._concurrency, thread_name_prefix="conveyorq-handler")
        removers = self._install_signals(stop) if install_signal_handlers else []

        try:
            attempt = 0
            while not stop.is_set():
                session = _Session(stub, self._session_config(), mux, self._encryptor, executor)
                established = await session.run(stop)

                if stop.is_set():
                    break

                attempt = 0 if established else attempt + 1
                await _sleep_until(_full_jitter(attempt), stop)
        finally:
            for remove in removers:
                remove()

            executor.shutdown(wait=False, cancel_futures=True)
            await channel.close()

    def _session_config(self) -> "_SessionConfig":
        return _SessionConfig(
            queues=self._queues,
            concurrency=self._concurrency,
            sdk_version=self._sdk_version,
            min_server_version=self._min_server_version,
            metadata=self._metadata,
        )

    @staticmethod
    def _install_signals(stop: asyncio.Event) -> List[Callable[[], object]]:
        loop = asyncio.get_running_loop()
        removers: List[Callable[[], object]] = []

        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, stop.set)
                removers.append(partial(loop.remove_signal_handler, sig))
            except (NotImplementedError, ValueError):
                # add_signal_handler is unavailable off the main thread or on
                # some platforms; the caller can still pass an explicit stop.
                pass

        return removers


class _SessionConfig:
    """The immutable per-session inputs derived from the worker's options."""

    __slots__ = ("queues", "concurrency", "sdk_version", "min_server_version", "metadata")

    def __init__(
        self,
        *,
        queues: Dict[str, int],
        concurrency: int,
        sdk_version: str,
        min_server_version: str,
        metadata: Sequence[Tuple[str, str]],
    ) -> None:
        self.queues = queues
        self.concurrency = concurrency
        self.sdk_version = sdk_version
        self.min_server_version = min_server_version
        self.metadata = metadata


class _Session:
    """Owns the state of one connected worker stream."""

    def __init__(
        self,
        stub: service_pb2_grpc.WorkerServiceStub,
        config: _SessionConfig,
        mux: Mux,
        encryptor: Optional[Encryptor],
        executor: ThreadPoolExecutor,
    ) -> None:
        self._stub = stub
        self._config = config
        self._mux = mux
        self._encryptor = encryptor
        self._executor = executor

        self._outbound: "asyncio.Queue[Optional[service_pb2.WorkerMessage]]" = asyncio.Queue()
        self._inflight: Dict[str, asyncio.Event] = {}
        self._tasks: Set[asyncio.Task[None]] = set()
        self._sem = asyncio.Semaphore(config.concurrency)
        self._call: Optional["grpc.aio.StreamStreamCall"] = None
        self._established = False
        self._draining = False
        self._heartbeat_task: Optional[asyncio.Task[None]] = None
        self._lease_ttl = _DEFAULT_LEASE_TTL
        self._heartbeat_interval = _DEFAULT_HEARTBEAT

    async def run(self, stop: asyncio.Event) -> bool:
        """Run one session to completion; return whether it reached Welcome.

        Raises the underlying :class:`grpc.aio.AioRpcError` on a fatal error.
        """
        call = self._stub.Session(metadata=self._config.metadata)
        self._call = call
        writer = asyncio.create_task(self._writer(call))
        drainer = asyncio.create_task(self._drain_on_stop(stop))

        self._send(_hello(self._config, self._mux.batch_types()))

        try:
            async for message in call:
                self._handle(message)

            return self._established
        except grpc.aio.AioRpcError as error:
            if not self._draining and not stop.is_set() and error.code() in _FATAL_CODES:
                raise

            return self._established
        finally:
            drainer.cancel()
            self._stop_heartbeat()
            self._cancel_inflight()
            self._send(None)  # end the request stream if drain did not
            with contextlib.suppress(Exception):
                await writer

            call.cancel()  # tear the RPC down even if it never started cleanly

    async def _writer(self, call: "grpc.aio.StreamStreamCall") -> None:
        """Serialize all outbound frames onto the single stream writer."""
        while True:
            message = await self._outbound.get()
            if message is None:
                with contextlib.suppress(Exception):
                    await call.done_writing()

                return

            with contextlib.suppress(Exception):
                await call.write(message)

    def _handle(self, message: "service_pb2.ServerMessage") -> None:
        which = message.WhichOneof("frame")

        if which == "welcome":
            self._on_welcome(message.welcome)
        elif which == "dispatch":
            if not self._draining and message.dispatch.HasField("task"):
                self._spawn(self._run_one(message.dispatch.task, _deadline(message.dispatch)))
        elif which == "batch_dispatch":
            if not self._draining:
                batch = message.batch_dispatch
                self._spawn(self._run_batch(list(batch.tasks), batch.group, _deadline(batch)))
        elif which == "cancel":
            event = self._inflight.get(message.cancel.task_id)
            if event is not None:
                event.set()
        # "ping" and an unset frame are tolerated without a reply.

    def _on_welcome(self, welcome: "service_pb2.Welcome") -> None:
        self._established = True
        self._lease_ttl = _time.duration_seconds(welcome.lease_ttl) or _DEFAULT_LEASE_TTL
        self._heartbeat_interval = _time.duration_seconds(welcome.heartbeat_interval) or self._lease_ttl / 3
        self._start_heartbeat()

    async def _run_one(self, envelope: "task_pb2.TaskEnvelope", deadline: Optional[float]) -> None:
        """Execute a single dispatched task and report exactly one outcome."""
        cancelled = asyncio.Event()
        self._inflight[envelope.id] = cancelled
        timer = _arm_deadline(cancelled, deadline)

        try:
            async with self._sem:
                if self._draining:
                    return

                task = self._open_task(envelope)
                handler = self._mux.resolve(envelope.type)

                if handler is None:
                    self._report(envelope.id, service_pb2.TASK_OUTCOME_RETRY,
                                 f"conveyor: no handler registered for type {envelope.type}")
                    return

                ctx = HandlerContext(cancelled, deadline)
                outcome, error_msg = await _run_handler(handler, task, ctx, self._executor)
                self._report(envelope.id, outcome, error_msg)
        except Exception as error:  # noqa: BLE001 -- undecryptable/decoding failure → retryable
            self._report(envelope.id, service_pb2.TASK_OUTCOME_RETRY, str(error))
        finally:
            if timer is not None:
                timer.cancel()

            self._inflight.pop(envelope.id, None)

    async def _run_batch(
        self, envelopes: "List[task_pb2.TaskEnvelope]", group: str, deadline: Optional[float]
    ) -> None:
        """Execute an aggregation group's members as one delivery."""
        cancelled = asyncio.Event()
        ids = [envelope.id for envelope in envelopes]

        # Register every member id so each member's lease is heartbeated
        # individually and a per-member Cancel reaches this batch.
        for task_id in ids:
            self._inflight[task_id] = cancelled

        timer = _arm_deadline(cancelled, deadline)

        try:
            async with self._sem:
                if self._draining:
                    return

                handler = self._mux.resolve_batch(envelopes[0].type if envelopes else "")
                if handler is None:
                    self._report_each(
                        ids, service_pb2.TASK_OUTCOME_RETRY, "conveyor: no batch handler registered"
                    )
                    return

                tasks = [self._open_task(envelope) for envelope in envelopes]
                await self._run_batch_handler(handler, tasks, ids, HandlerContext(cancelled, deadline))
        except Exception as error:  # noqa: BLE001
            self._report_each(ids, service_pb2.TASK_OUTCOME_RETRY, str(error))
        finally:
            if timer is not None:
                timer.cancel()

            for task_id in ids:
                self._inflight.pop(task_id, None)

    async def _run_batch_handler(
        self, handler: BatchHandler, tasks: List[Task], ids: List[str], ctx: HandlerContext
    ) -> None:
        try:
            await _invoke(handler, tasks, ctx, self._executor)
            self._report_each(ids, service_pb2.TASK_OUTCOME_SUCCESS, "")
        except BatchError as batch_error:
            for task_id in ids:
                failure = batch_error.failures.get(task_id)

                if failure is None:
                    self._report(task_id, service_pb2.TASK_OUTCOME_SUCCESS, "")
                elif isinstance(failure, SkipRetry):
                    self._report(task_id, service_pb2.TASK_OUTCOME_SKIP_RETRY, str(failure))
                else:
                    self._report(task_id, service_pb2.TASK_OUTCOME_RETRY, str(failure))
        except Exception as error:  # noqa: BLE001 -- whole-batch failure retries each member
            self._report_each(ids, service_pb2.TASK_OUTCOME_RETRY, str(error))

    def _open_task(self, envelope: "task_pb2.TaskEnvelope") -> Task:
        """Decode a dispatched envelope into a Task, decrypting if it is marked."""
        payload = envelope.payload
        metadata = dict(envelope.metadata)

        if metadata.get(ENCRYPTION_MARKER_KEY):
            if self._encryptor is None:
                raise ConveyorError(
                    f"conveyor: task {envelope.id} is encrypted but the worker has no encryptor"
                )

            payload = self._encryptor.decrypt(payload)
            metadata.pop(ENCRYPTION_MARKER_KEY, None)

        return Task(
            type=envelope.type,
            content_type=envelope.content_type,
            data=payload,
            metadata=metadata,
            id=envelope.id,
            queue=envelope.queue,
            retried=envelope.retried,
            max_retry=envelope.options.max_retry,
        )

    def _spawn(self, coro: "Awaitable[None]") -> None:
        task = asyncio.ensure_future(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    def _report(self, task_id: str, outcome: "service_pb2.TaskOutcome", error_msg: str) -> None:
        self._send(
            service_pb2.WorkerMessage(
                result=service_pb2.Result(task_id=task_id, outcome=outcome, error_msg=error_msg)
            )
        )

    def _report_each(self, ids: List[str], outcome: "service_pb2.TaskOutcome", error_msg: str) -> None:
        for task_id in ids:
            self._report(task_id, outcome, error_msg)

    def _send(self, message: "Optional[service_pb2.WorkerMessage]") -> None:
        self._outbound.put_nowait(message)

    def _start_heartbeat(self) -> None:
        self._stop_heartbeat()
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    def _stop_heartbeat(self) -> None:
        if self._heartbeat_task is not None:
            self._heartbeat_task.cancel()
            self._heartbeat_task = None

    async def _heartbeat_loop(self) -> None:
        while True:
            await asyncio.sleep(self._heartbeat_interval)

            if not self._inflight:
                continue

            self._send(
                service_pb2.WorkerMessage(
                    heartbeat=service_pb2.Heartbeat(active_task_ids=list(self._inflight.keys()))
                )
            )

    async def _drain_on_stop(self, stop: asyncio.Event) -> None:
        """Wait for the stop signal, then drain in-flight work and close the stream."""
        await stop.wait()

        self._draining = True
        self._stop_heartbeat()

        deadline = time.monotonic() + _DRAIN_GRACE
        while self._inflight and time.monotonic() < deadline:
            await asyncio.sleep(0.05)

        # End the request stream; the server releases any still-held leases with
        # no retry penalty (a deploy is therefore free).
        self._send(None)

        # Give the writer and server a brief window to half-close cleanly, then
        # force the call down so the receive loop in run() can never hang waiting
        # on a server that lingers after our half-close. On a clean close run()
        # has already returned and cancelled this task before the sleep elapses.
        await asyncio.sleep(_CLOSE_GRACE)
        if self._call is not None:
            self._call.cancel()

    def _cancel_inflight(self) -> None:
        for event in self._inflight.values():
            event.set()

        for task in list(self._tasks):
            task.cancel()


async def _run_handler(
    handler: Handler, task: Task, ctx: HandlerContext, executor: ThreadPoolExecutor
) -> Tuple["service_pb2.TaskOutcome", str]:
    """Run a single-task handler and map its result to an outcome."""
    try:
        await _invoke(handler, task, ctx, executor)

        return service_pb2.TASK_OUTCOME_SUCCESS, ""
    except SkipRetry as skip:
        return service_pb2.TASK_OUTCOME_SKIP_RETRY, str(skip)
    except Exception as error:  # noqa: BLE001 -- any other failure (or raise) is retryable
        return service_pb2.TASK_OUTCOME_RETRY, str(error)


async def _invoke(
    handler: object, payload: object, ctx: HandlerContext, executor: ThreadPoolExecutor
) -> None:
    """Call a handler, awaiting an async one and running a sync one on the executor.

    ``asyncio.iscoroutinefunction`` is used (not ``inspect``) so a coroutine
    function wrapped in ``functools.partial`` is still awaited rather than run on
    a thread, where its coroutine would never be awaited.
    """
    if asyncio.iscoroutinefunction(handler):
        await handler(payload, ctx)  # type: ignore[operator]

        return

    loop = asyncio.get_running_loop()
    await loop.run_in_executor(executor, handler, payload, ctx)  # type: ignore[arg-type]


def _hello(config: _SessionConfig, batch_types: List[str]) -> "service_pb2.WorkerMessage":
    return service_pb2.WorkerMessage(
        hello=service_pb2.Hello(
            queues=config.queues,
            concurrency=config.concurrency,
            sdk_version=config.sdk_version,
            min_server_version=config.min_server_version,
            batch_types=batch_types,
        )
    )


def _deadline(frame: object) -> Optional[float]:
    """Read an optional dispatch deadline Timestamp as a UNIX timestamp (seconds)."""
    if not frame.HasField("deadline"):  # type: ignore[attr-defined]
        return None

    deadline = frame.deadline  # type: ignore[attr-defined]

    return deadline.seconds + deadline.nanos / 1_000_000_000


def _arm_deadline(cancelled: asyncio.Event, deadline: Optional[float]) -> Optional[asyncio.TimerHandle]:
    if deadline is None:
        return None

    remaining = max(0.0, deadline - time.time())

    return asyncio.get_running_loop().call_later(remaining, cancelled.set)


def _full_jitter(attempt: int) -> float:
    ceiling = min(_RECONNECT_MAX, _RECONNECT_BASE * (2 ** attempt))

    return random.random() * ceiling


async def _sleep_until(delay: float, stop: asyncio.Event) -> None:
    """Sleep for ``delay`` seconds, returning early if ``stop`` is set."""
    with contextlib.suppress(asyncio.TimeoutError):
        await asyncio.wait_for(stop.wait(), timeout=delay)
