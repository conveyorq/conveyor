// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

import type { Encryptor } from "./encryption.js";
import type { Task } from "./task.js";

/**
 * TaskState is the lifecycle state of a task, as reported by
 * {@link Client.getTask}.
 */
export type TaskState =
  | "unspecified"
  | "scheduled"
  | "pending"
  | "active"
  | "retry"
  | "completed"
  | "archived"
  | "canceled"
  | "aggregating";

/** TaskInfo is the external view of a task returned by the producer API. */
export interface TaskInfo {
  id: string;
  queue: string;
  type: string;
  state: TaskState;
  priority: number;
  retried: number;
  maxRetry: number;
  lastError: string;
  enqueuedAt?: Date;
  processAt?: Date;
  completedAt?: Date;
  startedAt?: Date;
}

/**
 * EnqueueOptions configures one enqueue. Durations are in **milliseconds**;
 * absolute times are `Date`s. Every field is optional; unset fields take the
 * server default.
 */
export interface EnqueueOptions {
  /** A client-chosen task id, making enqueue retries idempotent. */
  taskId?: string;
  /** Target queue; defaults to "default". */
  queue?: string;
  /** Retries before the task is dead-lettered; 0 selects the server default. */
  maxRetry?: number;
  /** Dispatch priority within a queue, 1 (lowest) to 9 (highest). */
  priority?: number;
  /** Per-attempt timeout in milliseconds; the handler's signal aborts after it. */
  timeout?: number;
  /** Absolute time after which the task must not run. */
  deadline?: Date;
  /** Delay execution until this absolute time. */
  processAt?: Date;
  /** Delay execution by this many milliseconds. */
  processIn?: number;
  /** Keep the completed task visible for this many milliseconds before purge. */
  retention?: number;
  /** Reject duplicates of this task for this many milliseconds (uniqueness TTL). */
  unique?: number;
  /** Explicit uniqueness key; defaults to a hash of type + payload. */
  uniqueKey?: string;
  /** Make the task a member of the named aggregation group. */
  group?: string;
  /** Archive the task if it is not dispatched within this many milliseconds. */
  expiresIn?: number;
  /** Archive the task if it is not dispatched by this absolute time. */
  expiresAt?: Date;
}

/** EnqueueFn commits a task and returns its info; the unit enqueue middleware wraps. */
export type EnqueueFn = (task: Task, options: EnqueueOptions) => Promise<TaskInfo>;

/**
 * EnqueueMiddleware decorates the enqueue path, outermost first. It is the
 * client-side counterpart of {@link Mux.use}: inject metadata, enforce policy,
 * or record metrics before a task is committed.
 */
export type EnqueueMiddleware = (next: EnqueueFn) => EnqueueFn;

/** ClientOptions configures a {@link Client}. */
export interface ClientOptions {
  /** Bearer token presented to the server. */
  token?: string | undefined;
  /** Encrypt task payloads end to end before enqueue (see {@link newAESGCM}). */
  encryptor?: Encryptor | undefined;
  /** Middleware applied to every enqueue, outermost first. */
  enqueueMiddleware?: EnqueueMiddleware[] | undefined;
}

/** WorkerOptions configures a {@link Worker}. */
export interface WorkerOptions {
  /** Bearer token presented to the server. */
  token?: string | undefined;
  /** Queues this worker serves, mapping queue name to dispatch weight (> 0). */
  queues: Record<string, number>;
  /** Total simultaneous executions across all queues (> 0). */
  concurrency: number;
  /** Reported in Hello; defaults to the SDK's own version. */
  sdkVersion?: string | undefined;
  /** Minimum server version required, as semver (e.g. "v1.2.0"). */
  minServerVersion?: string | undefined;
  /** Decrypt encrypted payloads on dispatch (see {@link newAESGCM}). */
  encryptor?: Encryptor | undefined;
}
