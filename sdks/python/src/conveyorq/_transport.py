# Copyright 2026 ConveyorQ
#
# SPDX-License-Identifier: Apache-2.0

"""gRPC channel construction shared by the client and the worker.

A plaintext ``http://`` base URL uses HTTP/2 cleartext (h2c); an ``https://``
URL negotiates HTTP/2 with TLS. gRPC is used because the worker session is a
bidirectional stream, which needs full-duplex HTTP/2. The bearer token is
attached per call as ``authorization`` metadata, not on the channel.
"""

from __future__ import annotations

from typing import List, Optional, Sequence, Tuple
from urllib.parse import urlparse

import grpc

from .errors import ConveyorError

# Metadata is a sequence of (key, value) pairs; gRPC keys must be lower-case.
Metadata = Sequence[Tuple[str, str]]

# A gRPC channel option is a (name, value) pair.
ChannelOption = Tuple[str, object]

_DEFAULT_HTTP_PORT = 80
_DEFAULT_HTTPS_PORT = 443

# A BatchDispatch may carry several tasks, each with a payload the server caps at
# 1 MiB, so the worker's receive limit must comfortably exceed gRPC's 4 MiB
# default. 64 MiB holds a large batch with headroom.
WORKER_MESSAGE_LIMIT = 64 * 1024 * 1024


def create_channel(base_url: str, options: Optional[Sequence[ChannelOption]] = None) -> grpc.aio.Channel:
    """Build an async gRPC channel for the Conveyor server at ``base_url``.

    ``http://`` yields an insecure (h2c) channel; ``https://`` yields a TLS
    channel. The URL must name a host, e.g. ``http://localhost:8080``. The
    channel MUST be created inside the running event loop that will use it; gRPC
    binds a channel to the loop active at construction.
    """
    parsed = urlparse(base_url)

    if parsed.hostname is None:
        raise ConveyorError(f"conveyor: invalid server url {base_url!r}")

    secure = parsed.scheme == "https"
    port = parsed.port or (_DEFAULT_HTTPS_PORT if secure else _DEFAULT_HTTP_PORT)
    target = f"{parsed.hostname}:{port}"
    channel_options: List[ChannelOption] = list(options or [])

    if secure:
        return grpc.aio.secure_channel(target, grpc.ssl_channel_credentials(), options=channel_options)

    return grpc.aio.insecure_channel(target, options=channel_options)


def worker_channel_options() -> List[ChannelOption]:
    """Channel options for a worker: receive/send limits sized for batch dispatch."""
    return [
        ("grpc.max_receive_message_length", WORKER_MESSAGE_LIMIT),
        ("grpc.max_send_message_length", WORKER_MESSAGE_LIMIT),
    ]


def auth_metadata(token: Optional[str]) -> Metadata:
    """Return the call metadata carrying the bearer token, or empty when unset."""
    if not token:
        return ()

    return (("authorization", f"Bearer {token}"),)
