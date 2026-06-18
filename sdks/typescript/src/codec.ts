// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

import { ConveyorError } from "./errors.js";

/**
 * Built-in content types. A payload is opaque bytes plus a content type; the
 * server never decodes it, so a producer and a consumer interoperate on a task
 * only if they agree on the bytes for its content type (see the wire protocol
 * §3).
 */
export const ContentType = {
  /** UTF-8 JSON document; the default. */
  JSON: "application/json",
  /** Opaque binary, used verbatim. */
  OctetStream: "application/octet-stream",
  /** Protobuf wire encoding. */
  Protobuf: "application/x-protobuf",
} as const;

/**
 * Payload is the encoded body of a task: opaque bytes tagged with the content
 * type that says how to decode them.
 */
export interface Payload {
  /** The content type, e.g. {@link ContentType.JSON}. */
  readonly contentType: string;
  /** The encoded bytes. */
  readonly data: Uint8Array;
}

const encoder = new TextEncoder();
const decoder = new TextDecoder();

/**
 * json encodes a value as a JSON payload (the default codec). Producer and
 * consumer must agree on the JSON shape of a given task type.
 */
export function json(value: unknown): Payload {
  return { contentType: ContentType.JSON, data: encoder.encode(JSON.stringify(value)) };
}

/**
 * bytes wraps raw bytes as an opaque binary payload. The bytes are stored and
 * delivered verbatim.
 */
export function bytes(data: Uint8Array): Payload {
  return { contentType: ContentType.OctetStream, data };
}

/**
 * text encodes a string as a UTF-8 JSON-string payload, so a JSON consumer
 * binds it as a string.
 */
export function text(value: string): Payload {
  return json(value);
}

/**
 * decodeJson decodes a JSON payload's bytes into a value. It throws a
 * {@link ConveyorError} when the content type is not JSON, so a worker never
 * silently mis-decodes a payload it has no codec for.
 */
export function decodeJson<T>(data: Uint8Array, contentType: string): T {
  if (contentType !== ContentType.JSON && contentType !== "") {
    throw new ConveyorError(`conveyor: cannot JSON-decode payload with content type ${contentType}`);
  }

  return JSON.parse(decoder.decode(data)) as T;
}

/** decodeText decodes a payload's bytes to a UTF-8 string. */
export function decodeText(data: Uint8Array): string {
  return decoder.decode(data);
}
