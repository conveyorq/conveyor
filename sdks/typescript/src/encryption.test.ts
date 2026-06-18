// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from "vitest";

import { AuthenticationError, newAESGCM, UnknownKeyIdError } from "./encryption.js";

const secret = (fill: number): Uint8Array => new Uint8Array(32).fill(fill);
const enc = () => new TextEncoder();
const dec = () => new TextDecoder();

describe("AESGCM", () => {
  it("round-trips a payload", () => {
    const codec = newAESGCM("k1", { id: "k1", secret: secret(1) });
    const plaintext = enc().encode("launch-code");

    expect(Uint8Array.from(codec.decrypt(codec.encrypt(plaintext)))).toEqual(plaintext);
  });

  it("frames the version, key id, and a fresh nonce", () => {
    const sealed = newAESGCM("k1", { id: "k1", secret: secret(1) }).encrypt(enc().encode("x"));

    expect(sealed[0]).toBe(1); // scheme version
    expect(sealed[1]).toBe(2); // key id length of "k1"
    expect(dec().decode(sealed.subarray(2, 4))).toBe("k1");
    // header(2) + key id(2) + nonce(12) + ciphertext(1) + GCM tag(16)
    expect(sealed.length).toBe(2 + 2 + 12 + 1 + 16);

    const again = newAESGCM("k1", { id: "k1", secret: secret(1) }).encrypt(enc().encode("x"));
    expect(again.subarray(4, 16)).not.toEqual(sealed.subarray(4, 16)); // distinct nonces
  });

  it("decrypts a ciphertext sealed by the Go SDK (cross-SDK interop)", () => {
    // Produced by the Go encryption.AESGCM under key "k1" = 32 bytes of 0x01.
    const goSealed = Buffer.from(
      "01026b31fa2631bc8ec32f8344f3319d4fed95d45187a2599c8ec57922b1de221cdf147ddcfed37d1cae62",
      "hex",
    );
    const codec = newAESGCM("k1", { id: "k1", secret: secret(1) });

    expect(dec().decode(codec.decrypt(goSealed))).toBe("launch-code");
  });

  it("rejects a wrong key with an authentication error", () => {
    const sealed = newAESGCM("k1", { id: "k1", secret: secret(1) }).encrypt(enc().encode("secret"));
    const wrong = newAESGCM("k1", { id: "k1", secret: secret(2) });

    expect(() => wrong.decrypt(sealed)).toThrow(AuthenticationError);
  });

  it("detects a tampered byte", () => {
    const sealed = newAESGCM("k1", { id: "k1", secret: secret(1) }).encrypt(enc().encode("secret"));
    const last = sealed.length - 1;
    sealed[last] = (sealed[last] ?? 0) ^ 0xff;

    expect(() => newAESGCM("k1", { id: "k1", secret: secret(1) }).decrypt(sealed)).toThrow(AuthenticationError);
  });

  it("opens data under a retired key after the active key rotates", () => {
    const sealed = newAESGCM("a", { id: "a", secret: secret(1) }).encrypt(enc().encode("old"));
    const rotated = newAESGCM("b", { id: "b", secret: secret(2) }, { id: "a", secret: secret(1) });

    expect(dec().decode(rotated.decrypt(sealed))).toBe("old");
  });

  it("fails on a ciphertext sealed under an unheld key id", () => {
    const sealed = newAESGCM("a", { id: "a", secret: secret(1) }).encrypt(enc().encode("x"));
    const other = newAESGCM("b", { id: "b", secret: secret(2) });

    expect(() => other.decrypt(sealed)).toThrow(UnknownKeyIdError);
  });
});
