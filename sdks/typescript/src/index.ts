// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

/**
 * The Conveyor TypeScript SDK: a producer {@link Client} for enqueuing tasks
 * and a {@link Worker} for processing them, over the same wire protocol as the
 * Go SDK.
 *
 * @packageDocumentation
 */

export { Client } from "./client.js";
export type { EnqueueTxItem } from "./client.js";
export { Worker } from "./worker.js";
export { Mux, BatchError } from "./mux.js";
export type { Handler, BatchHandler, HandlerContext, Middleware, BatchMiddleware } from "./mux.js";
export { Task, newTask } from "./task.js";
export { json, bytes, text, ContentType } from "./codec.js";
export type { Payload } from "./codec.js";
export { SkipRetry, skipRetry, DuplicateTaskError, ConveyorError } from "./errors.js";
export {
  newAESGCM,
  AESGCM,
  InvalidKeyError,
  UnknownKeyIdError,
  MalformedCiphertextError,
  AuthenticationError,
} from "./encryption.js";
export type { Encryptor, Key } from "./encryption.js";
export type {
  ClientOptions,
  WorkerOptions,
  EnqueueOptions,
  EnqueueMiddleware,
  EnqueueFn,
  TaskInfo,
  TaskState,
  Dependency,
  DependencyFailure,
  RetryPolicy,
  RetryStrategy,
} from "./options.js";
