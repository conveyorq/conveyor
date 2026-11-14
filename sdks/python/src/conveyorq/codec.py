# Copyright 2026 ConveyorQ
#
# SPDX-License-Identifier: Apache-2.0

"""Payload codecs: build and decode the opaque bytes a task carries.

A payload is opaque bytes tagged with a content type; the server never decodes
it, so a producer and a consumer interoperate on a task only if they agree on
the bytes for its content type (see the wire protocol section 3).
"""

from __future__ import annotations

import json as _json
from dataclasses import dataclass
from typing import Any, Optional

from .errors import ConveyorError
from .streaming import PayloadSource, is_payload_source


class ContentType:
    """The built-in payload content types.

    These name how a payload's bytes are encoded. ``JSON`` is the default and
    the only codec a worker must support to be useful with typical producers.
    """

    #: UTF-8 JSON document; the default codec.
    JSON = "application/json"
    #: Opaque binary, used verbatim.
    OCTET_STREAM = "application/octet-stream"
    #: Protobuf wire encoding.
    PROTOBUF = "application/x-protobuf"


@dataclass(frozen=True)
class Payload:
    """The encoded body of a task: opaque bytes plus the content type to decode them."""

    #: The content type, e.g. :attr:`ContentType.JSON`.
    content_type: str
    #: The encoded bytes; empty for a streamed payload.
    data: bytes
    #: Supplies a streamed payload (see :func:`stream`); ``None`` for in-memory bytes.
    source: Optional[PayloadSource] = None


def json(value: Any) -> Payload:
    """Encode ``value`` as a JSON payload (the default codec).

    Producer and consumer must agree on the JSON shape of a given task type.
    The bytes match Go's ``encoding/json`` and JavaScript's ``JSON.stringify``
    for plain values, so a task enqueued from any SDK decodes in the others.
    """
    return Payload(ContentType.JSON, _json.dumps(value, separators=(",", ":")).encode("utf-8"))


def binary(data: bytes) -> Payload:
    """Wrap raw bytes as an opaque binary payload, stored and delivered verbatim."""
    return Payload(ContentType.OCTET_STREAM, data)


def stream(source: PayloadSource, content_type: str = ContentType.OCTET_STREAM) -> Payload:
    """Supply a payload from ``source`` for one too large to hold in memory.

    ``source`` is an async iterable of byte chunks of any size, or a binary
    file-like object such as ``open(path, "rb")``, read on a background thread.
    :meth:`Client.enqueue` uploads it in chunks through the server's streaming
    enqueue, reading it to its end, and a streaming handler receives it as it
    arrives (see :meth:`Mux.handle_stream`). A streamed payload cannot be
    enqueued with :meth:`Client.enqueue_tx`, and ``unique`` needs an explicit
    ``unique_key`` since the payload is never held whole to hash.
    """
    if not is_payload_source(source):
        raise ConveyorError("conveyor: stream needs an async iterable of bytes or a binary file-like object")

    return Payload(content_type, b"", source)


def text(value: str) -> Payload:
    """Encode a string as a JSON-string payload, so a JSON consumer binds it as a string."""
    return json(value)


def decode_json(data: bytes, content_type: str) -> Any:
    """Decode a JSON payload's bytes into a value.

    Raises :class:`ConveyorError` when the content type is not JSON, so a worker
    never silently mis-decodes a payload it has no codec for. An empty content
    type is accepted as JSON for compatibility with minimally-tagged producers.
    """
    if content_type not in (ContentType.JSON, ""):
        raise ConveyorError(f"conveyor: cannot JSON-decode payload with content type {content_type}")

    return _json.loads(data.decode("utf-8"))


def decode_text(data: bytes) -> str:
    """Decode a payload's bytes to a UTF-8 string."""
    return data.decode("utf-8")
