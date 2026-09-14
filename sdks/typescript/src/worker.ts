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

/**
 * Queue names the server accepts, per the wire protocol. Validating the same
 * shape locally turns a name the server would refuse into a construction error
 * instead of a session that is rejected on every reconnect.
 */
const QUEUE_NAME_PATTERN = /^[a-zA-Z0-9][a-zA-Z0-9\-_.]*$/;

/** Reconnection backoff (full jitter), per the wire protocol §5.9. */
const RECONNECT_BASE_MS = 500;
const RECONNECT_MAX_MS = 30_000;

/** Default lease/heartbeat fallbacks when Welcome omits them. */
const DEFAULT_LEASE_TTL_MS = 60_000;
const DEFAULT_HEARTBEAT_MS = 20_000;

/** How long graceful drain waits for in-flight tasks before closing the stream. */
const DRAIN_GRACE_MS = 25_000;

/**
 * How long drain waits, after aborting the handlers still running when the grace
 * window expires, for them to report RELEASED before the stream closes. Anything
 * still unreported is released by the server on stream close.
 */
const DRAIN_SETTLE_MS = 2_000;

/**
 * DRAINING marks a controller aborted because the worker is shutting down, so
 * the task reports RELEASED — returned to the queue with no retry penalty —
 * rather than RETRY. A server Cancel or a deadline aborts with no reason and so
 * reports RETRY, leaving the server to apply its policy.
 */
const DRAINING = Symbol("conveyor:draining");

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

    // Validate the session contract locally so a Hello the server would reject
    // fails fast at construction rather than becoming an endless reconnect loop
    // (the server rejects it with a fatal invalid_argument on every attempt).
    for (const [name, weight] of Object.entries(options.queues)) {
      if (!QUEUE_NAME_PATTERN.test(name)) {
        throw new ConveyorError(
          `conveyor: queue name ${JSON.stringify(name)} must start with a letter or digit and contain only letters, digits, '-', '_' or '.'`,
        );
      }

      if (weight <= 0) {
        throw new ConveyorError(`conveyor: queue ${name} weight must be positive, got ${weight}`);
      }
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

/**
 * Session owns the state of one connected worker stream.
 *
 * Exported for tests only; it is not part of the package's public API.
 */
export class Session {
  private readonly outbound = new Pushable<WorkerMessage>();
  private readonly inflight = new Map<string, AbortController>();
  /**
   * parked holds the ids of tasks dispatched after the drain began. They are
   * never started, but their leases are kept alive by the heartbeat until the
   * stream closes, when the server releases them with no retry penalty;
   * reporting RELEASED at once would only have the server redispatch them here.
   */
  private readonly parked = new Set<string>();
  private readonly slots: Semaphore;
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
  ) {
    // Gate concurrent executions to the declared concurrency, so a worker never
    // runs more handlers than it advertised even if the server over-grants
    // credit (one credit per queue). A batch counts as a single slot.
    this.slots = new Semaphore(options.concurrency);
  }

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
      if (isFatal(error, this.established)) {
        throw error;
      }

      return this.established;
    } finally {
      this.signal.removeEventListener("abort", onAbort);
      this.stopHeartbeat();
      this.outbound.end();
      this.abortInflight();
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
        if (task === undefined) {
          break;
        }

        if (this.draining) {
          this.parked.add(task.id);
        } else {
          void this.runOne(task, deadlineMs(message.frame.value.deadline));
        }
        break;
      }

      case "batchDispatch": {
        if (this.draining) {
          for (const task of message.frame.value.tasks) {
            this.parked.add(task.id);
          }
        } else {
          void this.runBatch(message.frame.value.tasks, deadlineMs(message.frame.value.deadline));
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

  /**
   * runOne executes a single dispatched task and reports its outcome. The task
   * is tracked in `inflight` before a slot is acquired, so heartbeats extend the
   * lease of work still queued behind the concurrency gate.
   */
  private async runOne(envelope: TaskEnvelope, deadline: number | undefined): Promise<void> {
    const controller = this.scopedController(envelope.id, deadline);

    try {
      await this.slots.acquire();

      try {
        // Aborted while queued behind the gate (a drain, a server cancel, or the
        // deadline): the handler never runs; report the matching outcome.
        if (controller.signal.aborted) {
          this.report(envelope.id, outcomeForAbort(controller.signal), "");
          return;
        }

        const task = await this.openTask(envelope);
        const handler = this.mux.resolve(envelope.type);

        if (handler === undefined) {
          this.report(envelope.id, TaskOutcome.RETRY, `conveyor: no handler registered for type ${envelope.type}`);
          return;
        }

        const [outcome, errorMsg] = await runHandler(handler, task, controller.signal, this.progressReporter(envelope.id));
        this.report(envelope.id, outcome, errorMsg);
      } finally {
        this.slots.release();
      }
    } catch (error) {
      // A payload that could not be opened (e.g. an undecryptable task) is
      // retryable; a task the drain interrupted here is released, no penalty.
      if (controller.signal.reason === DRAINING) {
        this.report(envelope.id, TaskOutcome.RELEASED, "");
      } else {
        this.report(envelope.id, TaskOutcome.RETRY, errorMessage(error));
      }
    } finally {
      this.finish(envelope.id, controller);
    }
  }

  /**
   * runBatch executes an aggregation group's members as one delivery. Every
   * member id is tracked in `inflight` under one shared controller, so the
   * heartbeat extends every member's lease (a batch longer than the lease TTL is
   * not reaped and retried) and a Cancel for any member aborts the whole batch.
   * A batch occupies a single concurrency slot, matching the one credit it holds.
   */
  private async runBatch(envelopes: TaskEnvelope[], deadline: number | undefined): Promise<void> {
    const ids = envelopes.map((envelope) => envelope.id);
    const controller = this.scopedBatch(ids, deadline);

    try {
      await this.slots.acquire();

      try {
        if (controller.signal.aborted) {
          this.reportEach(ids, outcomeForAbort(controller.signal), "");
          return;
        }

        const handler = this.mux.resolveBatch(envelopes[0]?.type ?? "");
        if (handler === undefined) {
          this.reportEach(ids, TaskOutcome.RETRY, "conveyor: no batch handler registered");
          return;
        }

        const tasks = await Promise.all(envelopes.map((envelope) => this.openTask(envelope)));
        await this.runBatchHandler(handler, tasks, ids, controller.signal);
      } finally {
        this.slots.release();
      }
    } catch (error) {
      // An undecryptable member is retryable; a batch the drain interrupted here
      // is released, no penalty.
      if (controller.signal.reason === DRAINING) {
        this.reportEach(ids, TaskOutcome.RELEASED, "");
      } else {
        this.reportEach(ids, TaskOutcome.RETRY, errorMessage(error));
      }
    } finally {
      this.finishBatch(ids, controller);
    }
  }

  private async runBatchHandler(handler: BatchHandler, tasks: Task[], ids: string[], signal: AbortSignal): Promise<void> {
    try {
      // Progress is per single task; a batch handler gets a no-op reporter.
      await handler(tasks, { signal, reportProgress: () => {} });
      this.reportEach(ids, TaskOutcome.SUCCESS, "");
    } catch (error) {
      // A drain interrupted the whole batch: every member is released, no retry
      // penalty, regardless of any partial-failure detail the handler raised.
      if (signal.reason === DRAINING) {
        this.reportEach(ids, TaskOutcome.RELEASED, "");
        return;
      }

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

  /** scopedController registers a single task's abort controller under its id. */
  private scopedController(id: string, deadline: number | undefined): AbortController {
    const controller = new AbortController();
    this.inflight.set(id, controller);
    this.armDeadline(controller, deadline);

    return controller;
  }

  /**
   * scopedBatch registers one controller for a whole batch under every member
   * id, so the heartbeat lists every member (extending each lease) and a Cancel
   * for any member aborts the batch. There is no synthetic group key.
   */
  private scopedBatch(ids: string[], deadline: number | undefined): AbortController {
    const controller = new AbortController();

    for (const id of ids) {
      this.inflight.set(id, controller);
    }

    this.armDeadline(controller, deadline);

    return controller;
  }

  /** armDeadline aborts the controller at the delivery deadline, if any. */
  private armDeadline(controller: AbortController, deadline: number | undefined): void {
    if (deadline === undefined) {
      return;
    }

    const remaining = deadline - Date.now();
    const timer = setTimeout(() => controller.abort(), Math.max(0, remaining));
    controller.signal.addEventListener("abort", () => clearTimeout(timer), { once: true });
  }

  private finish(id: string, controller: AbortController): void {
    this.inflight.delete(id);
    controller.abort();
  }

  private finishBatch(ids: string[], controller: AbortController): void {
    for (const id of ids) {
      this.inflight.delete(id);
    }

    controller.abort();
  }

  /**
   * abortInflight aborts every execution still running when the session ends and
   * forgets them. Nothing this session's handlers produce can reach the server
   * once the stream is gone, and the server releases their leases and redelivers
   * them, so letting them run would execute the same task twice at once after
   * the worker reconnects. The Go and Python SDKs cancel in-flight work at the
   * same point.
   */
  private abortInflight(): void {
    for (const controller of new Set(this.inflight.values())) {
      controller.abort();
    }

    this.inflight.clear();
    this.parked.clear();
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
      const activeTaskIds = this.activeTaskIds();
      if (activeTaskIds.length === 0) {
        return;
      }

      this.outbound.push(
        create(WorkerMessageSchema, {
          frame: { case: "heartbeat", value: create(HeartbeatSchema, { activeTaskIds }) },
        }),
      );
    }, this.heartbeatMs);
  }

  /**
   * activeTaskIds lists every id whose lease this session must keep alive: the
   * tasks executing plus those parked by the drain.
   */
  private activeTaskIds(): string[] {
    return [...this.inflight.keys(), ...this.parked];
  }

  private stopHeartbeat(): void {
    if (this.heartbeatTimer !== undefined) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = undefined;
    }
  }

  /**
   * drain stops accepting work and lets in-flight tasks finish (reporting their
   * real outcomes) up to a grace window. The heartbeat keeps running throughout,
   * so a task still executing does not lose its lease mid-drain and get
   * redelivered at the cost of a retry. When the grace window expires, any
   * handler still running is aborted so it reports RELEASED (no retry penalty),
   * given a brief settle window to send that, and then the stream closes — the
   * server releases anything still held on stream close.
   */
  private async drain(call: AbortController): Promise<void> {
    this.draining = true;

    const deadline = Date.now() + DRAIN_GRACE_MS;
    while (this.inflight.size > 0 && Date.now() < deadline) {
      await sleep(50, undefined);
    }

    if (this.inflight.size > 0) {
      for (const controller of new Set(this.inflight.values())) {
        controller.abort(DRAINING);
      }

      const settle = Date.now() + DRAIN_SETTLE_MS;
      while (this.inflight.size > 0 && Date.now() < settle) {
        await sleep(20, undefined);
      }
    }

    this.stopHeartbeat();
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
    // An explicit skip-retry is honored even mid-drain; a task the drain
    // interrupted is released with no retry penalty; everything else retries.
    if (error instanceof SkipRetry) {
      return [TaskOutcome.SKIP_RETRY, error.message];
    }

    if (signal.reason === DRAINING) {
      return [TaskOutcome.RELEASED, ""];
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

/**
 * isFatal reports whether a stream error must stop the worker (no reconnect).
 * A rejected session contract, meaning bad auth, an unmet server version, or a
 * Hello the server refuses (invalid_argument, per the wire protocol), can never
 * succeed by retrying, so retrying it is an endless loop.
 *
 * `established` says whether the session reached Welcome, and it is what keeps
 * invalid_argument from being over-applied: the client library raises that same
 * code locally when a connection is severed mid-envelope, which is transient and
 * must reconnect. A Hello rejection always arrives before Welcome, so
 * invalid_argument is fatal only while the session is not yet established. The
 * Go SDK draws the same line by asking whether the code came off the wire.
 */
function isFatal(error: unknown, established: boolean): boolean {
  if (!(error instanceof ConnectError)) {
    return false;
  }

  if (error.code === Code.InvalidArgument) {
    return !established;
  }

  return error.code === Code.Unauthenticated || error.code === Code.PermissionDenied;
}

function errorMessage(error: unknown): string {
  if (error instanceof Error) {
    return error.message;
  }

  return String(error);
}

/**
 * outcomeForAbort maps an aborted delivery's reason to its outcome: a drain
 * releases the task (no retry penalty), while a server Cancel or a deadline —
 * neither of which carries the draining reason — retries it so the server
 * applies its own policy.
 */
function outcomeForAbort(signal: AbortSignal): TaskOutcome {
  return signal.reason === DRAINING ? TaskOutcome.RELEASED : TaskOutcome.RETRY;
}

/** fullJitter returns a backoff delay in [0, min(max, base*2^attempt)). */
function fullJitter(attempt: number): number {
  const ceiling = Math.min(RECONNECT_MAX_MS, RECONNECT_BASE_MS * 2 ** attempt);

  return Math.random() * ceiling;
}

/**
 * sleep waits for `ms`, resolving early if the optional signal aborts. The
 * abort listener is removed when the timer fires, so a long-lived signal (the
 * worker's own, listened to on every reconnect) does not accumulate one
 * closure per call.
 *
 * Exported for tests only; it is not part of the package's public API.
 */
export function sleep(ms: number, signal: AbortSignal | undefined): Promise<void> {
  return new Promise((resolve) => {
    const onAbort = (): void => {
      clearTimeout(timer);
      resolve();
    };

    const timer = setTimeout(() => {
      signal?.removeEventListener("abort", onAbort);
      resolve();
    }, ms);

    signal?.addEventListener("abort", onAbort, { once: true });
  });
}

/**
 * Semaphore is an async counting gate limiting how many executions run at once.
 * acquire resolves immediately while permits remain, otherwise queues until a
 * release hands a permit directly to the next waiter (FIFO). It bounds a
 * worker's concurrent handlers to its declared concurrency.
 */
class Semaphore {
  private permits: number;
  private readonly waiters: (() => void)[] = [];

  constructor(permits: number) {
    this.permits = permits;
  }

  acquire(): Promise<void> {
    if (this.permits > 0) {
      this.permits -= 1;

      return Promise.resolve();
    }

    return new Promise<void>((resolve) => this.waiters.push(resolve));
  }

  release(): void {
    const waiter = this.waiters.shift();
    if (waiter !== undefined) {
      waiter();
    } else {
      this.permits += 1;
    }
  }
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
