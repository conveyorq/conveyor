import { afterEach, expect, test } from "vitest";
import { history, recordSample, resetHistory, type QueueSample } from "./history.ts";

afterEach(() => resetHistory());

function sample(pending: number): QueueSample {
  return { time: pending, scheduled: 0, pending, active: 0, retry: 0, completed: 0, archived: 0 };
}

test("accumulates samples in order", () => {
  recordSample(sample(1));
  const snapshot = recordSample(sample(2));

  expect(snapshot.map((s) => s.pending)).toEqual([1, 2]);
  expect(history()).toHaveLength(2);
});

test("bounds the retained window", () => {
  for (let i = 0; i < 250; i++) {
    recordSample(sample(i));
  }

  const snapshot = history();
  expect(snapshot.length).toBeLessThanOrEqual(180);
  // The window keeps the most recent samples.
  expect(snapshot[snapshot.length - 1].pending).toBe(249);
});
