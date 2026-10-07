import { afterEach, expect, test } from "vitest";
import { TaskState } from "../gen/conveyor/v1/task_pb.ts";
import { rememberTaskFilters, resetTaskFilters, taskFilters } from "./taskFilters.ts";

afterEach(() => resetTaskFilters());

test("defaults to every queue and state", () => {
  expect(taskFilters()).toEqual({ queue: "", state: TaskState.UNSPECIFIED });
});

test("remembers the last applied filters until reset", () => {
  rememberTaskFilters({ queue: "email", state: TaskState.RETRY });
  expect(taskFilters()).toEqual({ queue: "email", state: TaskState.RETRY });

  resetTaskFilters();
  expect(taskFilters()).toEqual({ queue: "", state: TaskState.UNSPECIFIED });
});
