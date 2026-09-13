// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

import { describe, expect, it } from "vitest";

import { ConveyorError } from "./errors.js";
import { Worker } from "./worker.js";

// The worker validates its session contract locally at construction, so a Hello
// the server would reject fails fast instead of becoming an endless reconnect
// loop against a permanent invalid_argument.
describe("Worker Hello validation", () => {
  it("rejects a worker with no queues", () => {
    expect(() => new Worker("http://localhost:8080", { queues: {}, concurrency: 1 })).toThrow(ConveyorError);
  });

  it("rejects an empty queue name", () => {
    expect(() => new Worker("http://localhost:8080", { queues: { "": 1 }, concurrency: 1 })).toThrow(ConveyorError);
  });

  it("rejects a non-positive queue weight", () => {
    expect(() => new Worker("http://localhost:8080", { queues: { default: 0 }, concurrency: 1 })).toThrow(ConveyorError);
    expect(() => new Worker("http://localhost:8080", { queues: { default: -1 }, concurrency: 1 })).toThrow(ConveyorError);
  });

  it("rejects a non-positive concurrency", () => {
    expect(() => new Worker("http://localhost:8080", { queues: { default: 1 }, concurrency: 0 })).toThrow(ConveyorError);
  });

  it("accepts a valid contract", () => {
    expect(() => new Worker("http://localhost:8080", { queues: { default: 1, high: 2 }, concurrency: 4 })).not.toThrow();
  });
});
