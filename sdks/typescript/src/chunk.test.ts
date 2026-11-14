// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from "vitest";

import { ChunkOpener, ChunkOrderError, ChunkSealer, ChunkTruncatedError } from "./chunk.js";
import { type Encryptor, MalformedCiphertextError, newAESGCM } from "./encryption.js";

const encoder = new TextEncoder();
const decoder = new TextDecoder();

/** codec returns the AES-256-GCM encryptor keyed "k1" with 32 bytes of 0x01. */
const codec = () => newAESGCM("k1", { id: "k1", secret: new Uint8Array(32).fill(1) });

/** sealAll seals each part as one chunk, marking the final part last. */
async function sealAll(encryptor: Encryptor, parts: string[]): Promise<Uint8Array[]> {
  const sealer = new ChunkSealer(encryptor);
  const sealed: Uint8Array[] = [];

  for (const [index, part] of parts.entries()) {
    sealed.push(await sealer.seal(encoder.encode(part), index === parts.length - 1));
  }

  return sealed;
}

describe("ChunkSealer and ChunkOpener", () => {
  it("round-trip a chunked payload", async () => {
    const encryptor = codec();
    const opener = new ChunkOpener(encryptor);
    let opened = "";

    for (const chunk of await sealAll(encryptor, ["hello ", "chunked ", "world"])) {
      opened += decoder.decode(await opener.open(chunk));
    }

    expect(() => opener.finish()).not.toThrow();
    expect(opened).toBe("hello chunked world");
  });

  it("open chunks sealed by the Go SDK (cross-SDK interop)", async () => {
    // Produced by Go encryption.ChunkSealer over AESGCM key "k1" = 32 bytes of 0x01.
    const goSealed = [
      "01026b31b1754d697109b376cb515a14e561d2a8f8a2f93476229ea51ff3be9634a1a4841e03e8f54124cddb3c2764",
      "01026b31182ea7f5f24e82244fe353e9529dca9f4526af5f00b9b26c8581fb1e05324404c0dfeaebe447dca3abcf570234",
      "01026b318dfadf4ccdd6105bffb1958b7cb6e017686b5fd73aea350dee86330c467576b1e2df7c7defad2cec5465",
    ];
    const opener = new ChunkOpener(codec());
    let opened = "";

    for (const chunk of goSealed) {
      opened += decoder.decode(await opener.open(Buffer.from(chunk, "hex")));
    }

    expect(() => opener.finish()).not.toThrow();
    expect(opened).toBe("hello chunked world");
  });

  it("frame the sequence number and last flag inside the ciphertext", async () => {
    const encryptor = codec();
    const [first, last] = await sealAll(encryptor, ["a", "b"]);

    const framedFirst = await encryptor.decrypt(first!);
    const framedLast = await encryptor.decrypt(last!);

    expect([...framedFirst]).toEqual([0, 0, 0, 0, 0, 0, 0, 0, 0, "a".charCodeAt(0)]);
    expect([...framedLast]).toEqual([0, 0, 0, 0, 0, 0, 0, 1, 1, "b".charCodeAt(0)]);
  });

  it("reject a chunk out of sequence", async () => {
    const [, second] = await sealAll(codec(), ["a", "b", "c"]);

    await expect(new ChunkOpener(codec()).open(second!)).rejects.toThrow(ChunkOrderError);
  });

  it("reject a chunk after the last chunk", async () => {
    const encryptor = codec();
    const [only] = await sealAll(encryptor, ["a"]);
    const opener = new ChunkOpener(encryptor);

    await opener.open(only!);

    await expect(opener.open(only!)).rejects.toThrow(/after the last chunk/);
  });

  it("report a payload truncated before its last chunk", async () => {
    const encryptor = codec();
    const [first] = await sealAll(encryptor, ["a", "b"]);
    const opener = new ChunkOpener(encryptor);

    await opener.open(first!);

    expect(() => opener.finish()).toThrow(ChunkTruncatedError);
  });

  it("accept a payload of no chunks at all", () => {
    expect(() => new ChunkOpener(codec()).finish()).not.toThrow();
  });

  it("reject a decrypted chunk too short for its header", async () => {
    const encryptor = codec();
    const short = encryptor.encrypt(encoder.encode("short"));

    await expect(new ChunkOpener(encryptor).open(short)).rejects.toThrow(MalformedCiphertextError);
  });
});
