// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

/** Config is the worker/producer configuration, read entirely from the env. */
export interface Config {
  /** Conveyor server base URL. */
  addr: string;
  /** Bearer token; undefined when the server runs with auth disabled. */
  token: string | undefined;
  /** Worker execution slots. */
  concurrency: number;
  /** Retry budget for an email task before it is dead-lettered. */
  maxRetry: number;
  /** Per-attempt timeout in milliseconds; aborts a slow provider call. */
  attemptTimeoutMs: number;
  /** Simulated transient provider-failure rate in [0, 1], to exercise retries. */
  failureRate: number;
}

/** loadConfig reads the configuration from the CONVEYOR_ and EMAIL_ env vars. */
export function loadConfig(): Config {
  return {
    addr: process.env.CONVEYOR_ADDR ?? "http://localhost:8080",
    token: process.env.CONVEYOR_TOKEN,
    concurrency: intEnv("WORKER_CONCURRENCY", 20),
    maxRetry: intEnv("EMAIL_MAX_RETRY", 5),
    attemptTimeoutMs: intEnv("EMAIL_ATTEMPT_TIMEOUT_MS", 10_000),
    failureRate: floatEnv("EMAIL_FAILURE_RATE", 0.15),
  };
}

function intEnv(name: string, fallback: number): number {
  const raw = process.env[name];
  if (raw === undefined || raw === "") {
    return fallback;
  }

  const value = Number.parseInt(raw, 10);

  return Number.isFinite(value) ? value : fallback;
}

function floatEnv(name: string, fallback: number): number {
  const raw = process.env[name];
  if (raw === undefined || raw === "") {
    return fallback;
  }

  const value = Number.parseFloat(raw);

  return Number.isFinite(value) ? value : fallback;
}
