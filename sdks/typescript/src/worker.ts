// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

import { create } from "@bufbuild/protobuf";
import { type Client as ConnectClient, Code, ConnectError, createClient } from "@connectrpc/connect";

import { ConveyorError, SkipRetry } from "./errors.js";
import { ENCRYPTION_MARKER_KEY, type Encryptor } from "./encryption.js";
import {
  HeartbeatSchema,
  HelloSchema,
  ProgressSchema,
  ResultSchema,
  type ServerMessage,
  TaskOutcome,
  type WorkerMessage,
  WorkerMessageSchema,
  WorkerService,
} from "./gen/conveyor/v1/service_pb.js";
import type { TaskEnvelope } from "./gen/conveyor/v1/task_pb.js";
import { BatchError, type BatchHandler, type Handler, type Mux } from "./mux.js";
import type { WorkerOptions } from "./options.js";
import { Task } from "./task.js";
import { durationToMs } from "./time.js";
import { createTransport } from "./transport.js";

/** The SDK version reported in Hello. */
const SDK_VERSION = "conveyor-ts/0.1.0";

/** Reconnection backoff (full jitter), per the wire protocol §5.9. */
const RECONNECT_BASE_MS = 500;
const RECONNECT_MAX_MS = 30_000;

/** Default lease/heartbeat fallbacks when Welcome omits them. */
const DEFAULT_LEASE_TTL_MS = 60_000;
const DEFAULT_HEARTBEAT_MS = 20_000;

/** How long graceful drain waits for in-flight tasks before closing the stream. */
const DRAIN_GRACE_MS = 25_000;

/**
 * Worker is the consumer side of Conveyor: it opens a session, receives
 * dispatched tasks, runs the matching handler, and reports each outcome. It
 * implements the full worker session protocol — credit-bounded dispatch,
 * heartbeats, best-effort cancellation, full-jitter reconnect, and graceful
 * drain.
 *
 * ```ts
 * const worker = new Worker("http://localhost:8080", { queues: { default: 1 }, concurrency: 10, token });
 * const mux = new Mux().handle("email:welcome", async (task) => { ... });
 * await worker.run(mux, controller.signal);
 * ```
 */
export class Worker {
  private readonly rpc: ConnectClient<typeof WorkerService>;
  private readonly options: WorkerOptions;
  private readonly encryptor: Encryptor | undefined;

  constructor(baseUrl: string, options: WorkerOptions) {
    if (Object.keys(options.queues).length === 0) {
      throw new ConveyorError("conveyor: a worker must declare at least one queue");
    }

    if (options.concurrency <= 0) {
      throw new ConveyorError("conveyor: worker concurrency must be positive");
    }

    this.rpc = createClient(WorkerService, createTransport(baseUrl, options.token));
    this.options = options;
    this.encryptor = options.encryptor;
  }

  /**
   * run drives the worker until `signal` aborts (e.g. on SIGTERM), reconnecting
   * with full-jitter backoff across transient stream failures. It resolves once
   * the worker has stopped; it rejects only on a fatal error (bad auth, an
   * unmet minimum server version).
   */
  async run(mux: Mux, signal: AbortSignal): Promise<void> {
    let attempt = 0;

    while (!signal.aborted) {
      const established = await this.runSession(mux, signal);
      attempt = established ? 0 : attempt + 1;

      if (signal.aborted) {
        break;
      }

      await sleep(fullJitter(attempt), signal);
    }
  }

  /**
   * runSession runs one session attempt. It returns true if the session reached
   * Welcome (so the caller resets the backoff), false if it failed before. It
   * throws only on a fatal error that must stop the worker.
   */
  private async runSession(mux: Mux, signal: AbortSignal): Promise<boolean> {
    const session = new Session(this.rpc, this.options, this.encryptor, mux, signal);

    return session.run();
  }
}

/** Session owns the state of one connected worker stream. */
class Session {
  private readonly outbound = new Pushable<WorkerMessage>();
  private readonly inflight = new Map<string, AbortController>();
  private established = false;
  private draining = false;
  private heartbeatTimer: ReturnType<typeof setInterval> | undefined;
  private leaseTtlMs = DEFAULT_LEASE_TTL_MS;
  private heartbeatMs = DEFAULT_HEARTBEAT_MS;

  constructor(
    private readonly rpc: ConnectClient<typeof WorkerService>,
    private readonly options: WorkerOptions,
    private readonly encryptor: Encryptor | undefined,
    private readonly mux: Mux,
    private readonly signal: AbortSignal,
  ) {}

  /** run executes one session to completion and returns whether it established. */
  async run(): Promise<boolean> {
    const call = new AbortController();
    const onAbort = () => void this.drain(call);
    this.signal.addEventListener("abort", onAbort, { once: true });

    this.outbound.push(this.hello());

    try {
      for await (const message of this.rpc.session(this.outbound, { signal: call.signal })) {
        this.handleServerMessage(message);
      }

      return this.established;
    } catch (error) {
      if (isFatal(error)) {
        throw error;
      }

      return this.established;
    } finally {
      this.signal.removeEventListener("abort", onAbort);
      this.stopHeartbeat();
      this.outbound.end();
    }
  }

  /** hello builds the required first frame from the worker's declared shape. */
  private hello(): WorkerMessage {
    return create(WorkerMessageSchema, {
      frame: {
        case: "hello",
        value: create(HelloSchema, {
          queues: this.options.queues,
          concurrency: this.options.concurrency,
          sdkVersion: this.options.sdkVersion ?? SDK_VERSION,
          minServerVersion: this.options.minServerVersion ?? "",
          batchTypes: this.mux.batchTypes(),
        }),
      },
    });
  }

  private handleServerMessage(message: ServerMessage): void {
    switch (message.frame.case) {
      case "welcome": {
        this.established = true;
        this.leaseTtlMs = durationToMs(message.frame.value.leaseTtl) || DEFAULT_LEASE_TTL_MS;
        this.heartbeatMs = durationToMs(message.frame.value.heartbeatInterval) || this.leaseTtlMs / 3;
        this.startHeartbeat();
        break;
      }

      case "dispatch": {
        const task = message.frame.value.task;
        if (task !== undefined && !this.draining) {
          void this.runOne(task, deadlineMs(message.frame.value.deadline));
        }
        break;
      }

      case "batchDispatch": {
        if (!this.draining) {
          void this.runBatch(message.frame.value.tasks, message.frame.value.group, deadlineMs(message.frame.value.deadline));
        }
        break;
      }

      case "cancel": {
        this.inflight.get(message.frame.value.taskId)?.abort();
        break;
      }

      case "ping":
      case undefined:
        // Ping is a liveness probe a worker tolerates without replying.
        break;
    }
  }

  /** runOne executes a single dispatched task and reports its outcome. */
  private async runOne(envelope: TaskEnvelope, deadline: number | undefined): Promise<void> {
    const controller = this.scopedController(envelope.id, deadline);

    try {
      const task = await this.openTask(envelope);
      const handler = this.mux.resolve(envelope.type);

      if (handler === undefined) {
        this.report(envelope.id, TaskOutcome.RETRY, `conveyor: no handler registered for type ${envelope.type}`);
        return;
      }

      const [outcome, errorMsg] = await runHandler(handler, task, controller.signal, this.progressReporter(envelope.id));
      this.report(envelope.id, outcome, errorMsg);
    } catch (error) {
      // A payload that could not be opened (e.g. an undecryptable task) is
      // reported as a retryable failure; the handler never ran.
      this.report(envelope.id, TaskOutcome.RETRY, errorMessage(error));
    } finally {
      this.finish(envelope.id, controller);
    }
  }

  /** runBatch executes an aggregation group's members as one delivery. */
  private async runBatch(envelopes: TaskEnvelope[], group: string, deadline: number | undefined): Promise<void> {
    const controller = this.scopedController(`group:${group}`, deadline);
    const ids = envelopes.map((envelope) => envelope.id);

    try {
      const handler = this.mux.resolveBatch(envelopes[0]?.type ?? "");
      if (handler === undefined) {
        this.reportEach(ids, TaskOutcome.RETRY, "conveyor: no batch handler registered");
        return;
      }

      const tasks = await Promise.all(envelopes.map((envelope) => this.openTask(envelope)));
      await this.runBatchHandler(handler, tasks, ids, controller.signal);
    } catch (error) {
      this.reportEach(ids, TaskOutcome.RETRY, errorMessage(error));
    } finally {
      this.finish(`group:${group}`, controller);
    }
  }

  private async runBatchHandler(handler: BatchHandler, tasks: Task[], ids: string[], signal: AbortSignal): Promise<void> {
    try {
      // Progress is per single task; a batch handler gets a no-op reporter.
      await handler(tasks, { signal, reportProgress: () => {} });
      this.reportEach(ids, TaskOutcome.SUCCESS, "");
    } catch (error) {
      if (error instanceof BatchError) {
        for (const id of ids) {
          const failure = error.failures.get(id);
          if (failure === undefined) {
            this.report(id, TaskOutcome.SUCCESS, "");
          } else if (failure instanceof SkipRetry) {
            this.report(id, TaskOutcome.SKIP_RETRY, failure.message);
          } else {
            this.report(id, TaskOutcome.RETRY, errorMessage(failure));
          }
        }
        return;
      }

      throw error;
    }
  }

  /** openTask decodes a dispatched envelope into a Task, decrypting if marked. */
  private async openTask(envelope: TaskEnvelope): Promise<Task> {
    let payload = envelope.payload;
    const metadata = { ...envelope.metadata };

    if (metadata[ENCRYPTION_MARKER_KEY] !== undefined && metadata[ENCRYPTION_MARKER_KEY] !== "") {
      if (this.encryptor === undefined) {
        throw new ConveyorError(`conveyor: task ${envelope.id} is encrypted but the worker has no encryptor`);
      }

      payload = await this.encryptor.decrypt(payload);
      delete metadata[ENCRYPTION_MARKER_KEY];
    }

    return new Task({
      type: envelope.type,
      contentType: envelope.contentType,
      data: payload,
      metadata,
      id: envelope.id,
      queue: envelope.queue,
      retried: envelope.retried,
      maxRetry: envelope.options?.maxRetry ?? 0,
    });
  }

  /** scopedController registers an abort controller, firing at the deadline. */
  private scopedController(id: string, deadline: number | undefined): AbortController {
    const controller = new AbortController();
    this.inflight.set(id, controller);

    if (deadline !== undefined) {
      const remaining = deadline - Date.now();
      const timer = setTimeout(() => controller.abort(), Math.max(0, remaining));
      controller.signal.addEventListener("abort", () => clearTimeout(timer), { once: true });
    }

    return controller;
  }

  private finish(id: string, controller: AbortController): void {
    this.inflight.delete(id);
    controller.abort();
  }

  private report(taskId: string, outcome: TaskOutcome, errorMsg: string): void {
    this.outbound.push(
      create(WorkerMessageSchema, { frame: { case: "result", value: create(ResultSchema, { taskId, outcome, errorMsg }) } }),
    );
  }

  /**
   * progressReporter returns a per-task reporter that pushes a Progress frame,
   * clamping the percent to 0..100 and coalescing consecutive identical reports
   * so a chatty handler cannot flood the stream.
   */
  private progressReporter(taskId: string): (percent: number, message?: string) => void {
    let reported = false;
    let lastPercent = 0;
    let lastMessage = "";

    return (percent: number, message = ""): void => {
      const clamped = Math.max(0, Math.min(100, Math.floor(percent)));

      if (reported && clamped === lastPercent && message === lastMessage) {
        return;
      }

      reported = true;
      lastPercent = clamped;
      lastMessage = message;

      this.outbound.push(
        create(WorkerMessageSchema, {
          frame: { case: "progress", value: create(ProgressSchema, { taskId, percent: clamped, message }) },
        }),
      );
    };
  }

  private reportEach(ids: string[], outcome: TaskOutcome, errorMsg: string): void {
    for (const id of ids) {
      this.report(id, outcome, errorMsg);
    }
  }

  private startHeartbeat(): void {
    this.stopHeartbeat();
    this.heartbeatTimer = setInterval(() => {
      if (this.inflight.size === 0) {
        return;
      }

      this.outbound.push(
        create(WorkerMessageSchema, {
          frame: { case: "heartbeat", value: create(HeartbeatSchema, { activeTaskIds: [...this.inflight.keys()] }) },
        }),
      );
    }, this.heartbeatMs);
  }

  private stopHeartbeat(): void {
    if (this.heartbeatTimer !== undefined) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = undefined;
    }
  }

  /**
   * drain stops accepting work and lets in-flight tasks finish (reporting their
   * real outcomes) up to a grace window, then closes the stream — the server
   * releases any still-held leases with no retry penalty.
   */
  private async drain(call: AbortController): Promise<void> {
    this.draining = true;
    this.stopHeartbeat();

    const deadline = Date.now() + DRAIN_GRACE_MS;
    while (this.inflight.size > 0 && Date.now() < deadline) {
      await sleep(50, undefined);
    }

    this.outbound.end();
    call.abort();
  }
}

/** runHandler runs a single-task handler and maps its result to an outcome. */
async function runHandler(
  handler: Handler,
  task: Task,
  signal: AbortSignal,
  reportProgress: (percent: number, message?: string) => void,
): Promise<[TaskOutcome, string]> {
  try {
    await handler(task, { signal, reportProgress });

    return [TaskOutcome.SUCCESS, ""];
  } catch (error) {
    if (error instanceof SkipRetry) {
      return [TaskOutcome.SKIP_RETRY, error.message];
    }

    return [TaskOutcome.RETRY, errorMessage(error)];
  }
}

/** deadlineMs converts an optional dispatch deadline Timestamp to epoch millis. */
function deadlineMs(deadline: { seconds: bigint; nanos: number } | undefined): number | undefined {
  if (deadline === undefined) {
    return undefined;
  }

  return Number(deadline.seconds) * 1000 + deadline.nanos / 1_000_000;
}

/** isFatal reports whether a stream error must stop the worker (no reconnect). */
function isFatal(error: unknown): boolean {
  if (!(error instanceof ConnectError)) {
    return false;
  }

  return (
    error.code === Code.Unauthenticated ||
    error.code === Code.PermissionDenied ||
    error.code === Code.FailedPrecondition
  );
}

function errorMessage(error: unknown): string {
  if (error instanceof Error) {
    return error.message;
  }

  return String(error);
}

/** fullJitter returns a backoff delay in [0, min(max, base*2^attempt)). */
function fullJitter(attempt: number): number {
  const ceiling = Math.min(RECONNECT_MAX_MS, RECONNECT_BASE_MS * 2 ** attempt);

  return Math.random() * ceiling;
}

/** sleep waits for `ms`, resolving early if the optional signal aborts. */
function sleep(ms: number, signal: AbortSignal | undefined): Promise<void> {
  return new Promise((resolve) => {
    const timer = setTimeout(resolve, ms);
    signal?.addEventListener(
      "abort",
      () => {
        clearTimeout(timer);
        resolve();
      },
      { once: true },
    );
  });
}

/**
 * Pushable is a single-consumer async-iterable queue: the session pushes
 * outbound frames as they are produced, and the transport pulls them as the
 * request stream.
 */
class Pushable<T> implements AsyncIterable<T> {
  private readonly queue: T[] = [];
  private readonly waiters: ((result: IteratorResult<T>) => void)[] = [];
  private ended = false;

  push(value: T): void {
    if (this.ended) {
      return;
    }

    const waiter = this.waiters.shift();
    if (waiter !== undefined) {
      waiter({ value, done: false });
    } else {
      this.queue.push(value);
    }
  }

  end(): void {
    this.ended = true;
    let waiter = this.waiters.shift();
    while (waiter !== undefined) {
      waiter({ value: undefined, done: true });
      waiter = this.waiters.shift();
    }
  }

  [Symbol.asyncIterator](): AsyncIterator<T> {
    const done = (): Promise<IteratorResult<T>> => {
      this.end();

      return Promise.resolve({ value: undefined, done: true });
    };

    return {
      next: (): Promise<IteratorResult<T>> => {
        const value = this.queue.shift();
        if (value !== undefined) {
          return Promise.resolve({ value, done: false });
        }

        if (this.ended) {
          return Promise.resolve({ value: undefined, done: true });
        }

        return new Promise((resolve) => this.waiters.push(resolve));
      },
      // connect-node calls return()/throw() to tear down the request stream
      // when the response side ends or errors; both simply close the queue.
      return: done,
      throw: done,
    };
  }
}
