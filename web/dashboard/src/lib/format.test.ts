import { expect, test } from "vitest";
import { timestampFromDate } from "@bufbuild/protobuf/wkt";
import { TaskState } from "../gen/conveyor/v1/task_pb.ts";
import { formatNumber, formatTime, orDash, relativeTime, taskStateLabel } from "./format.ts";

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
