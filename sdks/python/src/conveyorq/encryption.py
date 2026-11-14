# Copyright 2026 ConveyorQ
#
# SPDX-License-Identifier: Apache-2.0

"""End-to-end payload encryption, byte-compatible with the Go and TypeScript SDKs.

A client seals a payload before enqueue and a worker opens it on dispatch, so
the server stores and relays ciphertext only and holds no keys. The built-in
:class:`AESGCM` codec frames each ciphertext as: a version byte, a key-id length
byte, the key id, a 12-byte nonce, the ciphertext, and the 16-byte GCM tag. The
version + key-id header is authenticated as additional data, binding the key id
to the result. This layout matches the other SDKs, so a task sealed by one opens
in any other.
"""

from __future__ import annotations

import os
import struct
from typing import Dict, Protocol, runtime_checkable

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM as _AESGCM

# Framing parameters -- must stay identical across SDKs.
_SCHEME_VERSION = 1
_KEY_BYTES = 32
_NONCE_BYTES = 12
_TAG_BYTES = 16
_MAX_KEY_ID_BYTES = 255
_HEADER_FIXED_BYTES = 2

# Chunk framing sealed into every chunk of a streamed payload: an 8-byte
# big-endian sequence number followed by a 1-byte last-chunk flag. Sealing the
# header with the data lets the opener detect reordered, dropped, or appended
# chunks, which per-chunk authentication alone would not catch.
_CHUNK_HEADER = struct.Struct(">QB")
_CHUNK_LAST_FLAG = 1

#: Metadata key set on an encrypted payload so a worker knows to decrypt it.
ENCRYPTION_MARKER_KEY = "conveyor.encryption"
#: Metadata marker value identifying the framing version.
ENCRYPTION_MARKER_VALUE = "1"


@runtime_checkable
class Encryptor(Protocol):
    """Seals and opens opaque byte strings end to end.

    A client encrypts a payload before enqueue and a worker decrypts it on
    dispatch. :meth:`decrypt` must accept any ciphertext a prior :meth:`encrypt`
    produced -- including ciphertext sealed under a now-retired key the
    implementation still holds -- so key rotation never strands stored data.
    """

    def encrypt(self, plaintext: bytes) -> bytes:
        """Seal plaintext into ciphertext that :meth:`decrypt` can open."""
        ...

    def decrypt(self, ciphertext: bytes) -> bytes:
        """Open ciphertext produced by :meth:`encrypt`."""
        ...


class Key:
    """A named AES-256 secret.

    The ``id`` labels ciphertext so the matching secret can be found after the
    active key rotates; it travels in the clear and must not itself be sensitive.
    The ``secret`` is 32 bytes of AES-256 key material.
    """

    __slots__ = ("id", "secret")

    def __init__(self, id: str, secret: bytes) -> None:
        self.id = id
        self.secret = secret


class InvalidKeyError(Exception):
    """Raised when a key cannot be used to build an :class:`AESGCM`."""

    def __init__(self, message: str) -> None:
        super().__init__(f"encryption: invalid key: {message}")


class UnknownKeyIdError(Exception):
    """Raised when decrypting ciphertext sealed under a key the codec does not hold."""

    def __init__(self, key_id: str) -> None:
        super().__init__(f"encryption: unknown key id {key_id!r}")
        self.key_id = key_id


class MalformedCiphertextError(Exception):
    """Raised when ciphertext is truncated or does not match the framing."""

    def __init__(self, message: str) -> None:
        super().__init__(f"encryption: malformed ciphertext: {message}")


class AuthenticationError(Exception):
    """Raised when ciphertext fails its authentication tag (tampered or wrong key)."""

    def __init__(self) -> None:
        super().__init__("encryption: authentication failed")


class AESGCM:
    """The built-in :class:`Encryptor`: AES-256-GCM with a per-call random nonce.

    It holds a keyring so a retired key still opens data sealed under it, and
    seals new data under the active key. Construct one with :func:`new_aes_gcm`.
    """

    __slots__ = ("_ring", "_active_id", "_active")

    def __init__(self, ring: Dict[str, bytes], active_id: str) -> None:
        self._ring = ring
        self._active_id = active_id
        self._active = _AESGCM(ring[active_id])

    def encrypt(self, plaintext: bytes) -> bytes:
        """Seal ``plaintext`` under the active key with a fresh random nonce."""
        header = _encode_header(self._active_id)
        nonce = os.urandom(_NONCE_BYTES)
        body = self._active.encrypt(nonce, plaintext, header)

        return header + nonce + body

    def decrypt(self, ciphertext: bytes) -> bytes:
        """Open ``ciphertext``, selecting the key named in its header."""
        key_id, header_length = _decode_header(ciphertext)

        secret = self._ring.get(key_id)
        if secret is None:
            raise UnknownKeyIdError(key_id)

        rest = ciphertext[header_length:]
        if len(rest) < _NONCE_BYTES + _TAG_BYTES:
            raise MalformedCiphertextError("missing nonce or tag")

        nonce = rest[:_NONCE_BYTES]
        body = rest[_NONCE_BYTES:]
        header = ciphertext[:header_length]

        try:
            return _AESGCM(secret).decrypt(nonce, body, header)
        except InvalidTag:
            raise AuthenticationError() from None


class ChunkOrderError(Exception):
    """Raised when a chunk is not the next in sequence or follows the chunk marked last."""

    def __init__(self, message: str) -> None:
        super().__init__(f"encryption: chunk out of order: {message}")


class ChunkTruncatedError(Exception):
    """Raised when a chunked payload ends before the chunk marked last."""

    def __init__(self) -> None:
        super().__init__("encryption: chunked payload truncated")


class ChunkSealer:
    """Seals a streamed payload one chunk at a time, numbering chunks from zero.

    Each chunk is sealed as ``encrypt(sequence || last || data)``, the framing
    every SDK shares, so a payload sealed here opens in any of them. Use one
    sealer per payload.
    """

    __slots__ = ("_encryptor", "_next")

    def __init__(self, encryptor: Encryptor) -> None:
        self._encryptor = encryptor
        self._next = 0

    def seal(self, data: bytes, last: bool) -> bytes:
        """Frame and encrypt the next chunk.

        ``last`` must be true on the final chunk, and only on it; an empty
        payload is sealed as one empty chunk marked last.
        """
        flag = _CHUNK_LAST_FLAG if last else 0
        sealed = self._encryptor.encrypt(_CHUNK_HEADER.pack(self._next, flag) + data)
        self._next += 1

        return sealed


class ChunkOpener:
    """Opens a payload sealed by :class:`ChunkSealer`, enforcing that chunks
    arrive in order and end with the chunk marked last. Use one opener per
    payload.
    """

    __slots__ = ("_encryptor", "_next", "_done")

    def __init__(self, encryptor: Encryptor) -> None:
        self._encryptor = encryptor
        self._next = 0
        self._done = False

    def open(self, sealed: bytes) -> bytes:
        """Decrypt the next chunk and return its data.

        Raises :class:`ChunkOrderError` for a chunk out of sequence or after the
        chunk marked last, and :class:`MalformedCiphertextError` when the opened
        chunk is too short to carry the framing header.
        """
        if self._done:
            raise ChunkOrderError("chunk after the last chunk")

        framed = self._encryptor.decrypt(sealed)
        if len(framed) < _CHUNK_HEADER.size:
            raise MalformedCiphertextError("chunk shorter than its header")

        sequence, flag = _CHUNK_HEADER.unpack_from(framed)
        if sequence != self._next:
            raise ChunkOrderError(f"got chunk {sequence}, want {self._next}")

        self._next += 1
        self._done = flag == _CHUNK_LAST_FLAG

        return framed[_CHUNK_HEADER.size :]

    def finish(self) -> None:
        """Check the payload ended after the chunk marked last.

        Raises :class:`ChunkTruncatedError` otherwise. A payload of no chunks at
        all was not chunk-sealed and passes.
        """
        if self._next > 0 and not self._done:
            raise ChunkTruncatedError()


def new_aes_gcm(active_key_id: str, *keys: Key) -> AESGCM:
    """Build an :class:`AESGCM` sealing under ``active_key_id`` and opening any of ``keys``.

    Pass several keys to support rotation: keep an old key to decrypt existing
    data while a new active key seals fresh data.
    """
    if active_key_id == "":
        raise InvalidKeyError("active key id is empty")

    if not keys:
        raise InvalidKeyError("no keys provided")

    ring: Dict[str, bytes] = {}

    for key in keys:
        if key.id == "":
            raise InvalidKeyError("empty key id")

        if len(key.id.encode("utf-8")) > _MAX_KEY_ID_BYTES:
            raise InvalidKeyError(f"key id {key.id!r} exceeds {_MAX_KEY_ID_BYTES} bytes")

        if len(key.secret) != _KEY_BYTES:
            raise InvalidKeyError(f"key {key.id!r} secret is {len(key.secret)} bytes, want {_KEY_BYTES}")

        if key.id in ring:
            raise InvalidKeyError(f"duplicate key id {key.id!r}")

        ring[key.id] = key.secret

    if active_key_id not in ring:
        raise InvalidKeyError(f"active key id {active_key_id!r} not among keys")

    return AESGCM(ring, active_key_id)


def _encode_header(key_id: str) -> bytes:
    id_bytes = key_id.encode("utf-8")

    return bytes((_SCHEME_VERSION, len(id_bytes))) + id_bytes


def _decode_header(ciphertext: bytes) -> tuple[str, int]:
    if len(ciphertext) < _HEADER_FIXED_BYTES:
        raise MalformedCiphertextError("shorter than header")

    if ciphertext[0] != _SCHEME_VERSION:
        raise MalformedCiphertextError(f"unsupported version {ciphertext[0]}")

    id_length = ciphertext[1]
    header_length = _HEADER_FIXED_BYTES + id_length

    if len(ciphertext) < header_length:
        raise MalformedCiphertextError("truncated key id")

    key_id = ciphertext[_HEADER_FIXED_BYTES:header_length].decode("utf-8")

    return key_id, header_length
