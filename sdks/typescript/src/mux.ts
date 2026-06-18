// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

import { ConveyorError } from "./errors.js";
import type { Task } from "./task.js";

/**
 * HandlerContext is passed to every handler. Its {@link HandlerContext.signal}
 * aborts when the task's deadline passes or the server cancels it; a
 * well-behaved handler observes the signal to stop early.
 */
export interface HandlerContext {
  /** Aborts on the task's effective deadline or an operator cancel. */
  readonly signal: AbortSignal;
}

/**
 * Handler processes one task. Returning (or resolving) marks it completed;
 * throwing {@link SkipRetry} dead-letters it; any other throw retries it.
 */
export type Handler = (task: Task, ctx: HandlerContext) => void | Promise<void>;

/**
 * BatchHandler processes an aggregation group's members as one delivery. To
 * fail individual members, throw a {@link BatchError}; any other throw fails
 * the whole batch (each member retries).
 */
export type BatchHandler = (tasks: Task[], ctx: HandlerContext) => void | Promise<void>;

/** Middleware decorates a {@link Handler}, outermost first. */
export type Middleware = (next: Handler) => Handler;

/** BatchMiddleware decorates a {@link BatchHandler}, outermost first. */
export type BatchMiddleware = (next: BatchHandler) => BatchHandler;

/**
 * BatchError reports per-member failures from a {@link BatchHandler}: it maps a
 * member task id to its error. Members not listed are treated as succeeded.
 */
export class BatchError extends Error {
  /** Map of member task id to its failure. */
  readonly failures: Map<string, unknown>;

  constructor(failures: Map<string, unknown>) {
    super(`conveyor: ${failures.size} batch member(s) failed`);
    this.name = "BatchError";
    this.failures = failures;
  }
}

/**
 * Mux routes a task to the handler registered for its type, like the Go SDK's
 * Mux. Register single-task handlers with {@link Mux.handle} and batch handlers
 * (for aggregation groups) with {@link Mux.handleBatch}; decorate them with
 * {@link Mux.use} / {@link Mux.useBatch}.
 */
export class Mux {
  private readonly handlers = new Map<string, Handler>();
  private readonly batchHandlers = new Map<string, BatchHandler>();
  private readonly middleware: Middleware[] = [];
  private readonly batchMiddleware: BatchMiddleware[] = [];

  /** Register a single-task handler for a task type. */
  handle(type: string, handler: Handler): this {
    if (type === "") {
      throw new ConveyorError("conveyor: handler type is required");
    }

    this.handlers.set(type, handler);

    return this;
  }

  /** Register a batch handler for an aggregation-group task type. */
  handleBatch(type: string, handler: BatchHandler): this {
    if (type === "") {
      throw new ConveyorError("conveyor: batch handler type is required");
    }

    this.batchHandlers.set(type, handler);

    return this;
  }

  /** Append single-task middleware, applied outermost first. */
  use(...middleware: Middleware[]): this {
    this.middleware.push(...middleware);

    return this;
  }

  /** Append batch middleware, applied outermost first. */
  useBatch(...middleware: BatchMiddleware[]): this {
    this.batchMiddleware.push(...middleware);

    return this;
  }

  /** The task types registered as batch handlers, advertised to the server. */
  batchTypes(): string[] {
    return [...this.batchHandlers.keys()];
  }

  /** @internal resolve the middleware-wrapped single-task handler for a type. */
  resolve(type: string): Handler | undefined {
    const handler = this.handlers.get(type);
    if (handler === undefined) {
      return undefined;
    }

    let wrapped = handler;
    for (let i = this.middleware.length - 1; i >= 0; i--) {
      wrapped = this.middleware[i]!(wrapped);
    }

    return wrapped;
  }

  /** @internal resolve the middleware-wrapped batch handler for a type. */
  resolveBatch(type: string): BatchHandler | undefined {
    const handler = this.batchHandlers.get(type);
    if (handler === undefined) {
      return undefined;
    }

    let wrapped = handler;
    for (let i = this.batchMiddleware.length - 1; i >= 0; i--) {
      wrapped = this.batchMiddleware[i]!(wrapped);
    }

    return wrapped;
  }
}
