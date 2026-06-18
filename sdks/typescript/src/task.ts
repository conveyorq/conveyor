// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

import { ContentType, decodeJson, decodeText, type Payload } from "./codec.js";

/** Fields used to construct a {@link Task}. */
export interface TaskInit {
  type: string;
  contentType: string;
  data: Uint8Array;
  metadata?: Record<string, string>;
  id?: string;
  queue?: string;
  retried?: number;
  maxRetry?: number;
}

/**
 * Task is both what a producer enqueues and what a handler receives. A producer
 * builds one with {@link newTask}; a worker is handed one per dispatch. The
 * payload is opaque bytes plus a content type — decode it with {@link Task.json}
 * (or {@link Task.payload} for raw bytes).
 */
export class Task {
  /** The handler routing key, e.g. "email:welcome". */
  readonly type: string;
  /** How the payload is encoded, e.g. "application/json". */
  readonly contentType: string;
  /** User tags and trace propagation carried with the task. */
  readonly metadata: Record<string, string>;
  /** The task id; assigned by the server, set on dispatched tasks. */
  readonly id: string;
  /** The queue the task belongs to; set on dispatched tasks. */
  readonly queue: string;
  /** How many times this task has already been retried. */
  readonly retried: number;
  /** The retry budget before the task is dead-lettered. */
  readonly maxRetry: number;

  private readonly data: Uint8Array;

  constructor(init: TaskInit) {
    this.type = init.type;
    this.contentType = init.contentType;
    this.metadata = init.metadata ?? {};
    this.data = init.data;
    this.id = init.id ?? "";
    this.queue = init.queue ?? "";
    this.retried = init.retried ?? 0;
    this.maxRetry = init.maxRetry ?? 0;
  }

  /** The raw payload bytes. */
  payload(): Uint8Array {
    return this.data;
  }

  /** Decode a JSON payload into a value. Throws if the content type is not JSON. */
  json<T>(): T {
    return decodeJson<T>(this.data, this.contentType);
  }

  /** Decode the payload bytes as a UTF-8 string. */
  text(): string {
    return decodeText(this.data);
  }
}

/**
 * newTask builds a task to enqueue. Construct the payload with a codec —
 * `json(value)` (the default) or `bytes(data)`:
 *
 * ```ts
 * client.enqueue(newTask("email:welcome", json({ userId: 42 })));
 * ```
 */
export function newTask(type: string, payload: Payload, options?: { metadata?: Record<string, string> }): Task {
  const init: TaskInit = { type, contentType: payload.contentType || ContentType.JSON, data: payload.data };
  if (options?.metadata !== undefined) {
    init.metadata = options.metadata;
  }

  return new Task(init);
}
