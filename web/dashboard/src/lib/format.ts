import type { Timestamp } from "@bufbuild/protobuf/wkt";
import { timestampDate } from "@bufbuild/protobuf/wkt";
import { TaskState } from "../gen/conveyor/v1/task_pb.ts";
import type { Tone } from "../components/Badge.tsx";

// taskStateLabel maps a TaskState enum value to a short human label.
export function taskStateLabel(state: TaskState): string {
  switch (state) {
    case TaskState.SCHEDULED:
      return "scheduled";
    case TaskState.PENDING:
      return "pending";
    case TaskState.ACTIVE:
      return "active";
    case TaskState.RETRY:
      return "retry";
    case TaskState.COMPLETED:
      return "completed";
    case TaskState.ARCHIVED:
      return "archived";
    case TaskState.CANCELED:
      return "canceled";
    default:
      return "unknown";
  }
}

// taskStateTone maps a TaskState to the badge tone used to color it.
export function taskStateTone(state: TaskState): Tone {
  switch (state) {
    case TaskState.PENDING:
      return "sky";
    case TaskState.SCHEDULED:
      return "violet";
    case TaskState.ACTIVE:
      return "amber";
    case TaskState.RETRY:
      return "orange";
    case TaskState.COMPLETED:
      return "emerald";
    case TaskState.ARCHIVED:
      return "rose";
    default:
      return "zinc";
  }
}

// formatTime renders a protobuf timestamp as a local date-time string, or "—"
// when absent.
export function formatTime(ts: Timestamp | undefined): string {
  if (!ts) {
    return "—";
  }

  return timestampDate(ts).toLocaleString();
}

// orDash returns the value or an em dash when it is empty.
export function orDash(value: string): string {
  return value === "" ? "—" : value;
}

// formatNumber renders a count with thousands separators.
export function formatNumber(value: bigint | number): string {
  return value.toLocaleString("en-US");
}

// humanizeDuration renders a millisecond span as a compact human string.
function humanizeDuration(ms: number): string {
  if (ms < 1000) {
    return `${ms}ms`;
  }

  const seconds = ms / 1000;
  if (seconds < 60) {
    return `${seconds.toFixed(seconds < 10 ? 2 : 1)}s`;
  }

  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) {
    return `${minutes}m ${Math.round(seconds % 60)}s`;
  }

  const hours = Math.floor(minutes / 60);
  return `${hours}h ${minutes % 60}m`;
}

// formatDuration renders a task's execution duration: the span from its start
// (lease) to completion, or the elapsed time so far while it is still running.
// Returns "—" when the task has not started. now is injectable for testing.
export function formatDuration(
  startedAt: Timestamp | undefined,
  completedAt: Timestamp | undefined,
  now: Date = new Date(),
): string {
  if (!startedAt) {
    return "—";
  }

  const start = timestampDate(startedAt).getTime();
  const end = completedAt ? timestampDate(completedAt).getTime() : now.getTime();
  const text = humanizeDuration(Math.max(0, end - start));

  return completedAt ? text : `${text} (running)`;
}

// decodePayload renders a task payload for inspection: JSON content is pretty-
// printed, other text is shown as-is, and undecodable bytes fall back to a size
// summary. Output is capped at maxLength characters to keep the panel bounded.
export function decodePayload(
  payload: Uint8Array,
  contentType: string,
  maxLength = 2000,
): string {
  if (payload.length === 0) {
    return "—";
  }

  let text: string;
  try {
    text = new TextDecoder("utf-8", { fatal: true }).decode(payload);
  } catch {
    return `${payload.length} bytes (binary)`;
  }

  if (contentType.includes("json")) {
    try {
      text = JSON.stringify(JSON.parse(text), null, 2);
    } catch {
      // Not valid JSON despite the content type: show the raw text.
    }
  }

  if (text.length > maxLength) {
    return `${text.slice(0, maxLength)}… (${payload.length} bytes)`;
  }

  return text;
}

// relativeTime renders how long ago a timestamp was (e.g. "5m ago"), or "—"
// when absent. now is injectable for testing.
export function relativeTime(ts: Timestamp | undefined, now: Date = new Date()): string {
  if (!ts) {
    return "—";
  }

  const seconds = Math.max(0, Math.round((now.getTime() - timestampDate(ts).getTime()) / 1000));

  if (seconds < 45) {
    return "just now";
  }

  const minutes = Math.round(seconds / 60);
  if (minutes < 60) {
    return `${minutes}m ago`;
  }

  const hours = Math.round(minutes / 60);
  if (hours < 24) {
    return `${hours}h ago`;
  }

  return `${Math.round(hours / 24)}d ago`;
}
