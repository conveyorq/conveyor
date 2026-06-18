# Copyright 2026 ConveyorQ
#
# SPDX-License-Identifier: Apache-2.0

import pytest

from conveyorq import (
    AuthenticationError,
    InvalidKeyError,
    Key,
    MalformedCiphertextError,
    UnknownKeyIdError,
    new_aes_gcm,
)


def secret(fill: int) -> bytes:
    return bytes([fill]) * 32


def test_round_trips_a_payload():
    codec = new_aes_gcm("k1", Key("k1", secret(1)))

    assert codec.decrypt(codec.encrypt(b"top secret")) == b"top secret"


def test_frames_version_key_id_and_a_fresh_nonce():
    codec = new_aes_gcm("k1", Key("k1", secret(1)))
    sealed = codec.encrypt(b"x")

    assert sealed[0] == 1  # scheme version
    assert sealed[1] == 2  # key id length of "k1"
    assert sealed[2:4] == b"k1"
    # header(2) + key id(2) + nonce(12) + ciphertext(1) + GCM tag(16)
    assert len(sealed) == 2 + 2 + 12 + 1 + 16

    again = codec.encrypt(b"x")
    assert again[4:16] != sealed[4:16]  # distinct nonces


def test_decrypts_a_ciphertext_sealed_by_the_go_sdk():
    # Produced by the Go encryption.AESGCM under key "k1" = 32 bytes of 0x01.
    go_sealed = bytes.fromhex(
        "01026b31fa2631bc8ec32f8344f3319d4fed95d45187a2599c8ec57922b1de221cdf147ddcfed37d1cae62"
    )
    codec = new_aes_gcm("k1", Key("k1", secret(1)))

    assert codec.decrypt(go_sealed) == b"launch-code"


def test_rejects_a_wrong_key_with_an_authentication_error():
    sealed = new_aes_gcm("k1", Key("k1", secret(1))).encrypt(b"secret")
    wrong = new_aes_gcm("k1", Key("k1", secret(2)))

    with pytest.raises(AuthenticationError):
        wrong.decrypt(sealed)


def test_detects_a_tampered_byte():
    sealed = bytearray(new_aes_gcm("k1", Key("k1", secret(1))).encrypt(b"secret"))
    sealed[-1] ^= 0xFF

    with pytest.raises(AuthenticationError):
        new_aes_gcm("k1", Key("k1", secret(1))).decrypt(bytes(sealed))


def test_opens_data_under_a_retired_key_after_the_active_key_rotates():
    sealed = new_aes_gcm("a", Key("a", secret(1))).encrypt(b"old")
    rotated = new_aes_gcm("b", Key("b", secret(2)), Key("a", secret(1)))

    assert rotated.decrypt(sealed) == b"old"


def test_fails_on_a_ciphertext_sealed_under_an_unheld_key_id():
    sealed = new_aes_gcm("a", Key("a", secret(1))).encrypt(b"x")
    other = new_aes_gcm("b", Key("b", secret(2)))

    with pytest.raises(UnknownKeyIdError):
        other.decrypt(sealed)


def test_rejects_truncated_ciphertext():
    codec = new_aes_gcm("k1", Key("k1", secret(1)))

    with pytest.raises(MalformedCiphertextError):
        codec.decrypt(b"\x01")


@pytest.mark.parametrize(
    "active, keys",
    [
        ("", [Key("k1", secret(1))]),
        ("k1", []),
        ("k1", [Key("k1", secret(1)[:16])]),
        ("missing", [Key("k1", secret(1))]),
        ("k1", [Key("k1", secret(1)), Key("k1", secret(2))]),
    ],
)
def test_rejects_invalid_keyrings(active, keys):
    with pytest.raises(InvalidKeyError):
        new_aes_gcm(active, *keys)
