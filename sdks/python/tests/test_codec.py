# Copyright 2026 ConveyorQ
#
# SPDX-License-Identifier: Apache-2.0

import pytest

from conveyorq import ContentType, binary, json, new_task, text
from conveyorq.codec import decode_json, decode_text
from conveyorq.errors import ConveyorError


def test_json_encodes_compactly_and_round_trips():
    payload = json({"user_id": 42, "name": "ada"})

    assert payload.content_type == ContentType.JSON
    assert payload.data == b'{"user_id":42,"name":"ada"}'
    assert decode_json(payload.data, payload.content_type) == {"user_id": 42, "name": "ada"}


def test_binary_is_verbatim():
    payload = binary(b"\x00\x01\x02")

    assert payload.content_type == ContentType.OCTET_STREAM
    assert payload.data == b"\x00\x01\x02"


def test_text_is_a_json_string():
    payload = text("hello")

    assert payload.content_type == ContentType.JSON
    assert payload.data == b'"hello"'


def test_decode_json_rejects_non_json_content_type():
    with pytest.raises(ConveyorError):
        decode_json(b"{}", ContentType.OCTET_STREAM)


def test_decode_json_accepts_empty_content_type():
    assert decode_json(b"[1,2]", "") == [1, 2]


def test_decode_text_reads_utf8():
    assert decode_text("héllo".encode("utf-8")) == "héllo"


def test_new_task_defaults_to_json_when_payload_untagged():
    task = new_task("t", json(1))

    assert task.content_type == ContentType.JSON
    assert task.json() == 1


def test_task_text_reads_raw_bytes():
    task = new_task("t", binary(b"raw"))

    assert task.text() == "raw"
    assert task.payload == b"raw"
