// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from "vitest";

import { ConveyorError } from "./errors.js";
import { type Handler, Mux } from "./mux.js";

const noop: Handler = () => {};

describe("Mux", () => {
  it("routes a type to its handler", () => {
    const mux = new Mux().handle("t", noop);

    expect(mux.resolve("t")).toBe(noop);
    expect(mux.resolve("other")).toBeUndefined();
    expect(mux.streams("t")).toBe(false);
  });

  it("registers a streaming handler as a single-task handler", () => {
    const mux = new Mux().handleStream("blob", noop);

    expect(mux.resolve("blob")).toBe(noop);
    expect(mux.streams("blob")).toBe(true);
    expect(mux.batchTypes()).toEqual([]);
  });

  it("forgets the streaming flag when the type is re-registered with handle", () => {
    const mux = new Mux().handleStream("blob", noop).handle("blob", noop);

    expect(mux.streams("blob")).toBe(false);
  });

  it("rejects an empty type", () => {
    expect(() => new Mux().handle("", noop)).toThrow(ConveyorError);
    expect(() => new Mux().handleStream("", noop)).toThrow(ConveyorError);
    expect(() => new Mux().handleBatch("", () => {})).toThrow(ConveyorError);
  });

  it("wraps handlers in middleware, outermost first", async () => {
    const order: string[] = [];
    const mux = new Mux()
      .handle("t", () => {
        order.push("handler");
      })
      .use(
        (next) => (task, ctx) => {
          order.push("outer");
          return next(task, ctx);
        },
        (next) => (task, ctx) => {
          order.push("inner");
          return next(task, ctx);
        },
      )
      .handleBatch("b", () => {
        order.push("batch");
      })
      .useBatch((next) => (tasks, ctx) => {
        order.push("batch middleware");
        return next(tasks, ctx);
      });

    const ctx = { signal: new AbortController().signal, reportProgress: () => {} };
    await mux.resolve("t")!(undefined as never, ctx);
    await mux.resolveBatch("b")!([], ctx);

    expect(order).toEqual(["outer", "inner", "handler", "batch middleware", "batch"]);
    expect(mux.resolveBatch("missing")).toBeUndefined();
    expect(mux.batchTypes()).toEqual(["b"]);
  });
});
