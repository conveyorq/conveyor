# Copyright 2026 ConveyorQ
#
# SPDX-License-Identifier: Apache-2.0

"""The Conveyor Python SDK.

A producer :class:`Client` for enqueuing tasks and a :class:`Worker` for
processing them, over the same wire protocol as the Go and TypeScript SDKs.
Synchronous wrappers (:class:`SyncClient`, :class:`SyncWorker`) are available for
code that is not itself async.

    import conveyorq
    from conveyorq import Client, Worker, Mux, new_task, json

    async with Client("http://localhost:8080", token=token) as client:
        await client.enqueue(new_task("email:welcome", json({"user_id": 42})))
"""

from __future__ import annotations

from .client import Client
from .codec import ContentType, Payload, binary, decode_json, decode_text, json, text
from .encryption import (
    AESGCM,
    AuthenticationError,
    Encryptor,
    InvalidKeyError,
    Key,
    MalformedCiphertextError,
    UnknownKeyIdError,
    new_aes_gcm,
)
from .errors import ConveyorError, DuplicateTaskError, SkipRetry, skip_retry
from .mux import (
    BatchError,
    BatchHandler,
    BatchMiddleware,
    Handler,
    HandlerContext,
    Middleware,
    Mux,
)
from .options import EnqueueFn, EnqueueMiddleware, EnqueueOptions, TaskInfo, TaskState
from .sync import SyncClient, SyncWorker
from .task import Task, new_task
from .worker import SDK_VERSION, Worker

__version__ = "0.1.0"

__all__ = [
    "AESGCM",
    "AuthenticationError",
    "BatchError",
    "BatchHandler",
    "BatchMiddleware",
    "Client",
    "ContentType",
    "ConveyorError",
    "DuplicateTaskError",
    "Encryptor",
    "EnqueueFn",
    "EnqueueMiddleware",
    "EnqueueOptions",
    "Handler",
    "HandlerContext",
    "InvalidKeyError",
    "Key",
    "MalformedCiphertextError",
    "Middleware",
    "Mux",
    "Payload",
    "SDK_VERSION",
    "SkipRetry",
    "SyncClient",
    "SyncWorker",
    "Task",
    "TaskInfo",
    "TaskState",
    "UnknownKeyIdError",
    "Worker",
    "__version__",
    "binary",
    "decode_json",
    "decode_text",
    "json",
    "new_aes_gcm",
    "new_task",
    "skip_retry",
    "text",
]
