// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from "vitest";

import { bytes, ContentType, decodeJson, decodeText, json, text } from "./codec.js";
import { ConveyorError } from "./errors.js";

describe("codec", () => {
  it("json encodes and decodes a value", () => {
    const payload = json({ userId: 42, name: "Ada" });

    expect(payload.contentType).toBe(ContentType.JSON);
    expect(decodeJson<{ userId: number; name: string }>(payload.data, payload.contentType)).toEqual({
      userId: 42,
      name: "Ada",
    });
  });

  it("bytes wraps raw data verbatim as octet-stream", () => {
    const raw = new Uint8Array([1, 2, 3]);
    const payload = bytes(raw);

    expect(payload.contentType).toBe(ContentType.OctetStream);
    expect(payload.data).toEqual(raw);
  });

  it("text round-trips through the JSON codec", () => {
    const payload = text("hello");

    expect(decodeText(payload.data)).toBe('"hello"');
    expect(decodeJson<string>(payload.data, payload.contentType)).toBe("hello");
  });

  it("refuses to JSON-decode a non-JSON content type", () => {
    expect(() => decodeJson(new Uint8Array([1]), ContentType.OctetStream)).toThrow(ConveyorError);
  });
});
