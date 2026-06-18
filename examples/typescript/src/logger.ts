// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

/** Log severity levels, ordered low to high. */
export type Level = "debug" | "info" | "warn" | "error";

/** Structured fields attached to a log line. */
export type Fields = Record<string, unknown>;

/**
 * Logger emits one JSON object per line — the shape a production log pipeline
 * (Loki, CloudWatch, Datadog) ingests without a parser. {@link Logger.with}
 * returns a child that carries extra fields on every line, e.g. a task id.
 */
export interface Logger {
  debug(message: string, fields?: Fields): void;
  info(message: string, fields?: Fields): void;
  warn(message: string, fields?: Fields): void;
  error(message: string, fields?: Fields): void;
  with(fields: Fields): Logger;
}

const levels: Record<Level, number> = { debug: 0, info: 1, warn: 2, error: 3 };

/**
 * newLogger builds a JSON logger for a service. The threshold comes from
 * `$LOG_LEVEL` (default `info`).
 */
export function newLogger(service: string, base: Fields = {}): Logger {
  const threshold = levels[(process.env.LOG_LEVEL as Level) || "info"] ?? levels.info;

  const emit = (level: Level, message: string, fields?: Fields): void => {
    if (levels[level] < threshold) {
      return;
    }

    const line = JSON.stringify({ ts: new Date().toISOString(), level, service, msg: message, ...base, ...fields });
    process.stdout.write(`${line}\n`);
  };

  return {
    debug: (message, fields) => emit("debug", message, fields),
    info: (message, fields) => emit("info", message, fields),
    warn: (message, fields) => emit("warn", message, fields),
    error: (message, fields) => emit("error", message, fields),
    with: (fields) => newLogger(service, { ...base, ...fields }),
  };
}
