// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

import { createHash } from "node:crypto";

import { create } from "@bufbuild/protobuf";
import { type Client as ConnectClient, Code, ConnectError, createClient } from "@connectrpc/connect";

import { ConveyorError, DuplicateTaskError } from "./errors.js";
import { ENCRYPTION_MARKER_KEY, ENCRYPTION_MARKER_VALUE, type Encryptor } from "./encryption.js";
import {
  type EnqueueRequest,
  EnqueueRequestSchema,
  GetTaskRequestSchema,
  type TaskInfo as ProtoTaskInfo,
  TaskService,
} from "./gen/conveyor/v1/service_pb.js";
import {
  DependencyFailurePolicy as ProtoDependencyFailurePolicy,
  RetryPolicySchema,
  RetryStrategy as ProtoRetryStrategy,
  TaskState as ProtoTaskState,
} from "./gen/conveyor/v1/task_pb.js";
import type {
  ClientOptions,
  Dependency,
  DependencyFailure,
  EnqueueFn,
  EnqueueMiddleware,
  EnqueueOptions,
  RetryPolicy,
  RetryStrategy,
  TaskInfo,
  TaskState,
} from "./options.js";
import type { Task } from "./task.js";
import { dateFromTimestamp, durationFromMs, timestampFromDate } from "./time.js";
import { createTransport } from "./transport.js";

/**
 * EnqueueTxItem pairs a task with its per-task options for {@link Client.enqueueTx},
 * so a single atomic enqueue may span queues, priorities, and schedules.
 */
export interface EnqueueTxItem {
  /** task is the task to commit. */
  task: Task;
  /** options are the per-task enqueue options applied to {@link EnqueueTxItem.task}. */
  options?: EnqueueOptions;
}

/**
 * Client is the producer side of Conveyor: it commits tasks to the server.
 * Build one with a base URL and options, then call {@link Client.enqueue}:
 *
 * ```ts
 * const client = new Client("http://localhost:8080", { token });
 * await client.enqueue(newTask("email:welcome", json({ userId: 42 })), { queue: "critical" });
 * ```
 */
export class Client {
  private readonly rpc: ConnectClient<typeof TaskService>;
  private readonly encryptor: Encryptor | undefined;
  private readonly middleware: EnqueueMiddleware[];

  /**
   * Build a client for the server at `baseUrl` (e.g. "http://localhost:8080").
   */
  constructor(baseUrl: string, options: ClientOptions = {}) {
    this.rpc = createClient(TaskService, createTransport(baseUrl, options.token));
    this.encryptor = options.encryptor;
    this.middleware = options.enqueueMiddleware ?? [];
  }

  /**
   * enqueue commits one task and returns its server-assigned info. It rejects
   * with {@link DuplicateTaskError} when a unique task with the same key is
   * still incomplete.
   */
  async enqueue(task: Task, options: EnqueueOptions = {}): Promise<TaskInfo> {
    const uniqueKey = resolveUniqueKey(task, options);

    let enqueue: EnqueueFn = (decorated, settings) => this.commit(decorated, settings, uniqueKey);

    for (let i = this.middleware.length - 1; i >= 0; i--) {
      enqueue = this.middleware[i]!(enqueue);
    }

    return enqueue(task, options);
  }

  /**
   * enqueueTx commits every task atomically: either all are enqueued or none
   * are. If any task fails (a duplicate unique key, a unique-key collision
   * between two tasks in the call, or an invalid task), no task is committed and
   * it rejects with the offending task's error. On success it returns the
   * committed tasks in the order given.
   *
   * Unlike {@link Client.enqueue}, it does not run the enqueue middleware: the
   * middleware decorates a single-task commit, which the all-or-nothing path
   * does not model.
   */
  async enqueueTx(items: EnqueueTxItem[]): Promise<TaskInfo[]> {
    if (items.length === 0) {
      throw new ConveyorError("conveyor: at least one task is required");
    }

    const requests = await Promise.all(
      items.map((item) => {
        const options = item.options ?? {};
        const uniqueKey = resolveUniqueKey(item.task, options);

        return this.buildRequest(item.task, options, uniqueKey);
      }),
    );

    try {
      const response = await this.rpc.enqueueTx({ tasks: requests });

      return response.tasks.map(taskInfoFromProto);
    } catch (error) {
      throw mapClientError(error);
    }
  }

  /** getTask returns the current state of one task, or rejects if unknown. */
  async getTask(id: string): Promise<TaskInfo> {
    if (id === "") {
      throw new ConveyorError("conveyor: task id is required");
    }

    const request = create(GetTaskRequestSchema, { id });

    try {
      const response = await this.rpc.getTask(request);

      return taskInfoFromProto(response.task);
    } catch (error) {
      throw mapClientError(error);
    }
  }

  /** commit builds the wire request and enqueues one task. */
  private async commit(task: Task, options: EnqueueOptions, uniqueKey: string): Promise<TaskInfo> {
    const request = await this.buildRequest(task, options, uniqueKey);

    try {
      const response = await this.rpc.enqueue(request);

      return taskInfoFromProto(response.task);
    } catch (error) {
      throw mapClientError(error);
    }
  }

  /**
   * buildRequest builds the wire request from a task and its resolved options,
   * sealing the payload when encryption is on. It is the single source of truth
   * shared by {@link Client.enqueue} and {@link Client.enqueueTx}.
   */
  private async buildRequest(task: Task, options: EnqueueOptions, uniqueKey: string): Promise<EnqueueRequest> {
    let payload = task.payload();
    const metadata = { ...task.metadata };

    if (this.encryptor !== undefined && payload.length > 0) {
      payload = await this.encryptor.encrypt(payload);
      metadata[ENCRYPTION_MARKER_KEY] = ENCRYPTION_MARKER_VALUE;
    }

    return create(EnqueueRequestSchema, {
      taskId: options.taskId ?? "",
      queue: options.queue ?? "",
      type: task.type,
      payload,
      contentType: task.contentType,
      metadata,
      maxRetry: options.maxRetry ?? 0,
      priority: options.priority ?? 0,
      uniqueKey,
      group: options.group ?? "",
      concurrencyKey: options.concurrencyKey ?? "",
      ...(options.timeout !== undefined ? { timeout: durationFromMs(options.timeout) } : {}),
      ...(options.deadline !== undefined ? { deadline: timestampFromDate(options.deadline) } : {}),
      ...(options.processAt !== undefined ? { processAt: timestampFromDate(options.processAt) } : {}),
      ...(options.processIn !== undefined ? { processIn: durationFromMs(options.processIn) } : {}),
      ...(options.retention !== undefined ? { retention: durationFromMs(options.retention) } : {}),
      ...(options.unique !== undefined ? { uniqueTtl: durationFromMs(options.unique) } : {}),
      ...(options.expiresIn !== undefined ? { expiresIn: durationFromMs(options.expiresIn) } : {}),
      ...(options.expiresAt !== undefined ? { expiresAt: timestampFromDate(options.expiresAt) } : {}),
      ...(options.dependsOn !== undefined ? { dependsOn: options.dependsOn.map(dependencyToProto) } : {}),
      ...(options.retryPolicy !== undefined ? { retryPolicy: retryPolicyToProto(options.retryPolicy) } : {}),
    });
  }
}

/**
 * resolveUniqueKey validates an enqueue's mutually exclusive options and returns
 * the effective unique key, derived over the plaintext payload (before any
 * encryption) so identical work still collides under `unique`.
 */
function resolveUniqueKey(task: Task, options: EnqueueOptions): string {
  if (options.processAt !== undefined && options.processIn !== undefined) {
    throw new ConveyorError("conveyor: processAt and processIn are mutually exclusive");
  }

  if (options.expiresAt !== undefined && options.expiresIn !== undefined) {
    throw new ConveyorError("conveyor: expiresAt and expiresIn are mutually exclusive");
  }

  let uniqueKey = options.uniqueKey ?? "";
  if (uniqueKey === "" && (options.unique ?? 0) > 0) {
    uniqueKey = derivedUniqueKey(task.type, task.payload());
  }

  return uniqueKey;
}

/**
 * derivedUniqueKey computes the default uniqueness key of a task — a SHA-256 of
 * the type, a separator byte, and the payload — matching the Go SDK so the same
 * logical task dedups across languages.
 */
function derivedUniqueKey(type: string, payload: Uint8Array): string {
  const digest = createHash("sha256");
  digest.update(Buffer.from(type, "utf8"));
  digest.update(Uint8Array.of(0));
  digest.update(payload);

  return digest.digest("hex");
}

/**
 * dependencyToProto normalizes a dependency option — a plain task id or a
 * {@link Dependency} — into the wire form, defaulting the failure policy to
 * block.
 */
function dependencyToProto(dependency: string | Dependency): { taskId: string; onFailure: ProtoDependencyFailurePolicy } {
  const normalized = typeof dependency === "string" ? { taskId: dependency } : dependency;

  return {
    taskId: normalized.taskId,
    onFailure: failurePolicyToProto(normalized.onFailure),
  };
}

/** retryPolicyToProto builds the wire retry policy; an omitted field keeps the server default. */
function retryPolicyToProto(policy: RetryPolicy) {
  return create(RetryPolicySchema, {
    strategy: retryStrategyToProto(policy.strategy),
    ...(policy.base !== undefined ? { base: durationFromMs(policy.base) } : {}),
    ...(policy.max !== undefined ? { max: durationFromMs(policy.max) } : {}),
  });
}

/** retryStrategyToProto maps the SDK retry strategy to the wire enum. */
function retryStrategyToProto(strategy?: RetryStrategy): ProtoRetryStrategy {
  switch (strategy) {
    case "exponential":
      return ProtoRetryStrategy.EXPONENTIAL;

    case "linear":
      return ProtoRetryStrategy.LINEAR;

    case "fixed":
      return ProtoRetryStrategy.FIXED;

    default:
      return ProtoRetryStrategy.UNSPECIFIED;
  }
}

/** failurePolicyToProto maps a public failure policy to its wire enum value. */
function failurePolicyToProto(policy: DependencyFailure | undefined): ProtoDependencyFailurePolicy {
  switch (policy) {
    case "cascade-cancel":
      return ProtoDependencyFailurePolicy.CASCADE_CANCEL;
    case "continue":
      return ProtoDependencyFailurePolicy.CONTINUE;
    default:
      return ProtoDependencyFailurePolicy.BLOCK;
  }
}

/** mapClientError maps a duplicate-task server error to {@link DuplicateTaskError}. */
function mapClientError(error: unknown): unknown {
  if (error instanceof ConnectError && error.code === Code.AlreadyExists) {
    return new DuplicateTaskError(error.rawMessage);
  }

  return error;
}

const stateNames: Record<ProtoTaskState, TaskState> = {
  [ProtoTaskState.UNSPECIFIED]: "unspecified",
  [ProtoTaskState.SCHEDULED]: "scheduled",
  [ProtoTaskState.PENDING]: "pending",
  [ProtoTaskState.ACTIVE]: "active",
  [ProtoTaskState.RETRY]: "retry",
  [ProtoTaskState.COMPLETED]: "completed",
  [ProtoTaskState.ARCHIVED]: "archived",
  [ProtoTaskState.CANCELED]: "canceled",
  [ProtoTaskState.AGGREGATING]: "aggregating",
  [ProtoTaskState.BLOCKED]: "blocked",
};

/** taskInfoFromProto maps a wire TaskInfo to the SDK's plain view. */
function taskInfoFromProto(info: ProtoTaskInfo | undefined): TaskInfo {
  if (info === undefined) {
    throw new ConveyorError("conveyor: server returned no task info");
  }

  const result: TaskInfo = {
    id: info.id,
    queue: info.queue,
    type: info.type,
    state: stateNames[info.state] ?? "unspecified",
    priority: info.priority,
    retried: info.retried,
    maxRetry: info.maxRetry,
    lastError: info.lastError,
    progress: info.progress,
    progressMessage: info.progressMessage,
  };

  const enqueuedAt = dateFromTimestamp(info.enqueuedAt);
  if (enqueuedAt !== undefined) {
    result.enqueuedAt = enqueuedAt;
  }

  const processAt = dateFromTimestamp(info.processAt);
  if (processAt !== undefined) {
    result.processAt = processAt;
  }

  const completedAt = dateFromTimestamp(info.completedAt);
  if (completedAt !== undefined) {
    result.completedAt = completedAt;
  }

  const startedAt = dateFromTimestamp(info.startedAt);
  if (startedAt !== undefined) {
    result.startedAt = startedAt;
  }

  return result;
}
