// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from "vitest";

import { bytes, ContentType, json, stream } from "./codec.js";
import { INLINE_PAYLOAD_BYTES } from "./stream.js";
import { newTask, Task } from "./task.js";

const encoder = new TextEncoder();
const decoder = new TextDecoder();

/** collect drains an async iterable of bytes into a string. */
async function collect(iterable: AsyncIterable<Uint8Array>): Promise<string> {
  let text = "";
  for await (const chunk of iterable) {
    text += decoder.decode(chunk);
  }

  return text;
}

describe("Task", () => {
  it("builds a task from a payload with its metadata", () => {
    const task = newTask("email:welcome", json({ userId: 42 }), { metadata: { tenant: "acme" } });

    expect(task.type).toBe("email:welcome");
    expect(task.contentType).toBe(ContentType.JSON);
    expect(task.metadata).toEqual({ tenant: "acme" });
    expect(task.json()).toEqual({ userId: 42 });
    expect(task.text()).toBe('{"userId":42}');
  });

  it("defaults the content type to JSON", () => {
    expect(newTask("t", { contentType: "", data: new Uint8Array(0) }).contentType).toBe(ContentType.JSON);
  });

  it("reads an in-memory payload as a stream", async () => {
    expect(await collect(newTask("t", bytes(encoder.encode("inline"))).payloadStream())).toBe("inline");
    expect(await collect(newTask("t", bytes(new Uint8Array(0))).payloadStream())).toBe("");
  });

  it("reads a streamed payload from its source", async () => {
    const source = (async function* () {
      yield encoder.encode("from ");
      yield encoder.encode("source");
    })();

    const task = newTask("t", stream(source));

    expect(task.payload()).toHaveLength(0);
    expect(task.hasPayloadSource()).toBe(true);
    expect(await collect(task.payloadStream())).toBe("from source");
  });

  it("needs the streaming upload for a stream or a payload above the inline limit", () => {
    const empty = (async function* () {})();

    expect(newTask("t", stream(empty)).needsStreamUpload()).toBe(true);
    expect(newTask("t", bytes(new Uint8Array(INLINE_PAYLOAD_BYTES + 1))).needsStreamUpload()).toBe(true);
    expect(newTask("t", bytes(new Uint8Array(INLINE_PAYLOAD_BYTES))).needsStreamUpload()).toBe(false);
  });

  it("defaults the dispatch fields of a task built directly", () => {
    const task = new Task({ type: "t", contentType: ContentType.OctetStream, data: new Uint8Array(0) });

    expect([task.id, task.queue, task.retried, task.maxRetry]).toEqual(["", "", 0, 0]);
    expect(task.metadata).toEqual({});
    expect(task.hasPayloadSource()).toBe(false);
  });
});
