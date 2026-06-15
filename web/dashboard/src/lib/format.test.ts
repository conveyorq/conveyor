import { expect, test } from "vitest";
import { timestampFromDate } from "@bufbuild/protobuf/wkt";
import { TaskState } from "../gen/conveyor/v1/task_pb.ts";
import { decodePayload, formatDuration, formatNumber, formatTime, orDash, relativeTime, taskStateLabel } from "./format.ts";

test("labels every task state", () => {
  expect(taskStateLabel(TaskState.PENDING)).toBe("pending");
  expect(taskStateLabel(TaskState.ARCHIVED)).toBe("archived");
  expect(taskStateLabel(TaskState.UNSPECIFIED)).toBe("unknown");
});

test("formatTime returns a dash when absent", () => {
  expect(formatTime(undefined)).toBe("—");
});

test("formatTime renders a present timestamp", () => {
  const ts = timestampFromDate(new Date("2026-06-15T12:00:00Z"));
  expect(formatTime(ts)).not.toBe("—");
});

test("orDash replaces empty strings", () => {
  expect(orDash("")).toBe("—");
  expect(orDash("x")).toBe("x");
});

test("formatNumber groups thousands", () => {
  expect(formatNumber(1234567n)).toBe("1,234,567");
  expect(formatNumber(42)).toBe("42");
});

test("relativeTime renders a coarse age", () => {
  const now = new Date("2026-06-15T12:00:00Z");
  expect(relativeTime(undefined, now)).toBe("—");
  expect(relativeTime(timestampFromDate(new Date("2026-06-15T11:55:00Z")), now)).toBe("5m ago");
  expect(relativeTime(timestampFromDate(new Date("2026-06-15T09:00:00Z")), now)).toBe("3h ago");
});

test("formatDuration measures completed and running spans", () => {
  const started = timestampFromDate(new Date("2026-06-15T12:00:00Z"));
  const completed = timestampFromDate(new Date("2026-06-15T12:00:03.5Z"));
  const now = new Date("2026-06-15T12:00:10Z");

  expect(formatDuration(undefined, undefined, now)).toBe("—");
  expect(formatDuration(started, completed, now)).toBe("3.50s");
  expect(formatDuration(started, undefined, now)).toBe("10.0s (running)");
});

test("decodePayload pretty-prints JSON and handles binary", () => {
  const json = new TextEncoder().encode('{"b":2,"a":1}');
  expect(decodePayload(json, "application/json")).toBe('{\n  "b": 2,\n  "a": 1\n}');

  expect(decodePayload(new Uint8Array(), "application/json")).toBe("—");
  expect(decodePayload(new Uint8Array([0xff, 0xfe]), "application/octet-stream")).toBe("2 bytes (binary)");
});
