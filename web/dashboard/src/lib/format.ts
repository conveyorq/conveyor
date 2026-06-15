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
