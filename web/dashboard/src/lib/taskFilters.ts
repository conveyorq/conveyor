import { TaskState } from "../gen/conveyor/v1/task_pb.ts";

// TaskFilters is the Tasks view's queue and state filter.
export interface TaskFilters {
  // queue limits the list to one queue; empty means every queue.
  queue: string;
  // state limits the list to one state; UNSPECIFIED means every state.
  state: TaskState;
}

// defaultFilters shows every task.
const defaultFilters: TaskFilters = { queue: "", state: TaskState.UNSPECIFIED };

// remembered is a module-level copy so the filters survive switching to
// another view and back within a session; it resets on a full page reload.
let remembered: TaskFilters = defaultFilters;

// taskFilters returns the last filters the Tasks view applied.
export function taskFilters(): TaskFilters {
  return remembered;
}

// rememberTaskFilters records the filters the Tasks view applied.
export function rememberTaskFilters(filters: TaskFilters): void {
  remembered = filters;
}

// resetTaskFilters restores the default filters (used by tests).
export function resetTaskFilters(): void {
  remembered = defaultFilters;
}
