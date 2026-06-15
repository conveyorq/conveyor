import { useEffect, useState } from "react";
import { useApi, type Api } from "../api/context.tsx";
import { useAction, type ActionState } from "../api/useAction.ts";
import { useRefreshTick } from "../api/refresh.ts";
import { ConfirmButton } from "../components/ConfirmButton.tsx";
import { Panel } from "../components/Panel.tsx";
import { Badge } from "../components/Badge.tsx";
import { errorMessage } from "../lib/errors.ts";
import { formatTime, orDash, taskStateLabel, taskStateTone } from "../lib/format.ts";
import { TaskState } from "../gen/conveyor/v1/task_pb.ts";
import type { TaskInfo } from "../gen/conveyor/v1/service_pb.ts";

// stateOptions are the task-state filter choices; UNSPECIFIED means all states.
const stateOptions: { value: TaskState; label: string }[] = [
  { value: TaskState.UNSPECIFIED, label: "All states" },
  { value: TaskState.PENDING, label: "Pending" },
  { value: TaskState.ACTIVE, label: "Active" },
  { value: TaskState.SCHEDULED, label: "Scheduled" },
  { value: TaskState.RETRY, label: "Retry" },
  { value: TaskState.COMPLETED, label: "Completed" },
  { value: TaskState.ARCHIVED, label: "Archived" },
  { value: TaskState.CANCELED, label: "Canceled" },
];

// pageSize is how many tasks one page shows.
const pageSize = 20;

const inputClass =
  "rounded-md border border-[var(--border)] bg-[var(--input-bg)] px-2 py-1 text-xs text-[var(--text)] placeholder:text-[var(--muted)] focus:border-indigo-500/60 focus:outline-none";

// Tasks browses the task store with queue and state filters, paging, and a
// per-task detail panel.
export function Tasks() {
  const api = useApi();
  const [queue, setQueue] = useState("");
  const [state, setState] = useState<TaskState>(TaskState.UNSPECIFIED);
  const [tasks, setTasks] = useState<TaskInfo[]>([]);
  // pageStack holds the page_token at the start of each visited page; its last
  // entry is the current page, and its length is the page number.
  const [pageStack, setPageStack] = useState<string[]>([""]);
  const [nextToken, setNextToken] = useState("");
  const [error, setError] = useState<string | undefined>(undefined);
  const [loading, setLoading] = useState(true);
  const [selected, setSelected] = useState<TaskInfo | undefined>(undefined);
  const [refresh, setRefresh] = useState(0);
  const action = useAction(() => setRefresh((n) => n + 1));
  const tick = useRefreshTick();

  const pageToken = pageStack[pageStack.length - 1];

  useEffect(() => {
    let active = true;

    // loading is not re-raised on refresh/filter/page change: the current rows
    // stay on screen until the new ones arrive, so the view does not blink.
    api.admin
      .listTasks({ queue, state, limit: pageSize, pageToken })
      .then((resp) => {
        if (active) {
          setTasks(resp.tasks);
          setNextToken(resp.nextPageToken);
          setError(undefined);
          setLoading(false);
        }
      })
      .catch((err: unknown) => {
        if (active) {
          setError(errorMessage(err));
          setLoading(false);
        }
      });

    return () => {
      active = false;
    };
  }, [api, queue, state, pageToken, refresh, tick]);

  // resetTo applies a filter change and returns to the first page.
  function resetTo(change: () => void) {
    change();
    setPageStack([""]);
    setSelected(undefined);
  }

  const filters = (
    <>
      <input
        aria-label="Queue filter"
        placeholder="All queues"
        value={queue}
        onChange={(event) => resetTo(() => setQueue(event.target.value))}
        className={`w-32 ${inputClass}`}
      />
      <select
        aria-label="State filter"
        value={state}
        onChange={(event) => resetTo(() => setState(Number(event.target.value) as TaskState))}
        className={inputClass}
      >
        {stateOptions.map((option) => (
          <option key={option.value} value={option.value}>
            {option.label}
          </option>
        ))}
      </select>
    </>
  );

  return (
    <div className="space-y-4">
      {action.error !== undefined && (
        <p role="alert" className="rounded-lg border border-rose-500/30 bg-rose-50 px-4 py-2.5 text-sm text-rose-700 dark:bg-rose-500/10 dark:text-rose-300">
          {action.error}
        </p>
      )}

      <div className="flex flex-col gap-4 lg:flex-row lg:items-start">
        <div className="min-w-0 flex-1">
          <Panel title="Tasks" actions={filters}>
            {loading ? (
              <div className="flex items-center gap-2 px-5 py-8 text-sm text-[var(--muted)]">
                <span className="size-4 animate-spin rounded-full border-2 border-[var(--border)] border-t-[var(--muted)]" />
                Loading…
              </div>
            ) : error !== undefined && tasks.length === 0 ? (
              <p role="alert" className="px-5 py-8 text-sm text-rose-600 dark:text-rose-300">
                {error}
              </p>
            ) : tasks.length === 0 ? (
              <p className="px-5 py-8 text-sm text-[var(--muted)]">No tasks match.</p>
            ) : (
              <table className="w-full text-sm">
                <thead>
                  <tr className="text-left text-xs font-medium uppercase tracking-wider text-[var(--muted)]">
                    <th className="px-5 py-2.5">ID</th>
                    <th className="px-5 py-2.5">Type</th>
                    <th className="px-5 py-2.5">Queue</th>
                    <th className="px-5 py-2.5">State</th>
                  </tr>
                </thead>
                <tbody>
                  {tasks.map((task) => (
                    <tr
                      key={task.id}
                      onClick={() => setSelected(task)}
                      className={
                        "cursor-pointer border-t border-[var(--border)] hover:bg-[var(--row-hover)] " +
                        (selected?.id === task.id ? "bg-indigo-50 dark:bg-indigo-500/10" : "")
                      }
                    >
                      <td className="px-5 py-3 font-mono text-[var(--text-soft)]">{task.id}</td>
                      <td className="px-5 py-3 text-[var(--text-soft)]">{task.type}</td>
                      <td className="px-5 py-3 text-[var(--text-soft)]">{task.queue}</td>
                      <td className="px-5 py-3">
                        <Badge tone={taskStateTone(task.state)}>{taskStateLabel(task.state)}</Badge>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}

            {(pageStack.length > 1 || nextToken !== "") && (
              <div className="flex items-center justify-between border-t border-[var(--border)] px-5 py-3 text-xs text-[var(--muted)]">
                <span>Page {pageStack.length}</span>
                <span className="flex gap-2">
                  <button
                    type="button"
                    disabled={pageStack.length === 1}
                    onClick={() => {
                      setPageStack((stack) => stack.slice(0, -1));
                      setSelected(undefined);
                    }}
                    className="rounded-md bg-[var(--btn-bg)] px-3 py-1 text-[var(--text-soft)] hover:bg-[var(--btn-hover)] disabled:opacity-40"
                  >
                    Previous
                  </button>
                  <button
                    type="button"
                    disabled={nextToken === ""}
                    onClick={() => {
                      setPageStack((stack) => [...stack, nextToken]);
                      setSelected(undefined);
                    }}
                    className="rounded-md bg-[var(--btn-bg)] px-3 py-1 text-[var(--text-soft)] hover:bg-[var(--btn-hover)] disabled:opacity-40"
                  >
                    Next
                  </button>
                </span>
              </div>
            )}
          </Panel>
        </div>

        {selected !== undefined && (
          <aside className="lg:w-80">
            <Panel title="Task detail">
              <TaskDetail task={selected} api={api} action={action} onActed={() => setSelected(undefined)} />
            </Panel>
          </aside>
        )}
      </div>
    </div>
  );
}

// TaskDetail shows the full record for the selected task and the actions that
// apply to it: run-now, cancel, and delete (the latter two confirmed).
function TaskDetail({
  task,
  api,
  action,
  onActed,
}: {
  task: TaskInfo;
  api: Api;
  action: ActionState;
  onActed: () => void;
}) {
  const act = (fn: () => Promise<unknown>) => action.run(fn).then(onActed);

  // Only offer actions the task's state allows, matching the broker's rules,
  // so the dashboard never sends an operation that fails as invalid-state.
  const state = task.state;
  const canRun = state === TaskState.SCHEDULED || state === TaskState.RETRY;
  const canCancel =
    state === TaskState.SCHEDULED ||
    state === TaskState.PENDING ||
    state === TaskState.RETRY ||
    state === TaskState.ACTIVE;
  const canDelete = state !== TaskState.ACTIVE;

  const rows: [string, string][] = [
    ["ID", task.id],
    ["Type", task.type],
    ["Queue", task.queue],
    ["Priority", String(task.priority)],
    ["Retried", `${task.retried}/${task.maxRetry}`],
    ["Last error", orDash(task.lastError)],
    ["Enqueued", formatTime(task.enqueuedAt)],
    ["Process at", formatTime(task.processAt)],
    ["Completed", formatTime(task.completedAt)],
  ];

  return (
    <div aria-label="Task detail" className="px-5 py-4 text-sm">
      <div className="mb-3">
        <Badge tone={taskStateTone(task.state)}>{taskStateLabel(task.state)}</Badge>
      </div>

      <dl className="grid grid-cols-[7rem_1fr] gap-y-1.5">
        {rows.map(([label, value]) => (
          <div key={label} className="contents">
            <dt className="text-[var(--muted)]">{label}</dt>
            <dd className="break-all text-[var(--text-soft)]">{value}</dd>
          </div>
        ))}
      </dl>

      <div className="mt-4 flex flex-wrap gap-2">
        {canRun && (
          <ConfirmButton label="Run now" onConfirm={() => act(() => api.admin.runTask({ id: task.id }))} />
        )}
        {canCancel && (
          <ConfirmButton label="Cancel" confirm onConfirm={() => act(() => api.admin.cancelTask({ id: task.id }))} />
        )}
        {canDelete && (
          <ConfirmButton label="Delete" confirm danger onConfirm={() => act(() => api.admin.deleteTask({ id: task.id }))} />
        )}
        {!canRun && !canCancel && !canDelete && <span className="text-xs text-[var(--muted)]">No actions available.</span>}
      </div>
    </div>
  );
}
