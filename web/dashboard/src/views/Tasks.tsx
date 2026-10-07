import { useEffect, useState } from "react";
import { useApi, type Api } from "../api/context.tsx";
import { useAction, type ActionState } from "../api/useAction.ts";
import { useRefreshTick } from "../api/refresh.ts";
import { useReadOnly } from "../api/readonly.tsx";
import { ConfirmButton } from "../components/ConfirmButton.tsx";
import { Panel } from "../components/Panel.tsx";
import { ActionAlert } from "../components/ActionAlert.tsx";
import { Badge } from "../components/Badge.tsx";
import { errorMessage } from "../lib/errors.ts";
import { rememberTaskFilters, taskFilters } from "../lib/taskFilters.ts";
import { decodePayload, formatDuration, formatTime, orDash, taskStateLabel, taskStateTone } from "../lib/format.ts";
import { TaskState } from "../gen/conveyor/v1/task_pb.ts";
import type { TaskInfo } from "../gen/conveyor/v1/service_pb.ts";
import { timestampFromDate } from "@bufbuild/protobuf/wkt";
import { Code, ConnectError } from "@connectrpc/connect";

// stateOptions are the task-state filter choices; UNSPECIFIED means all states.
const stateOptions: { value: TaskState; label: string }[] = [
  { value: TaskState.UNSPECIFIED, label: "All states" },
  { value: TaskState.PENDING, label: "Pending" },
  { value: TaskState.ACTIVE, label: "Active" },
  { value: TaskState.SCHEDULED, label: "Scheduled" },
  { value: TaskState.RETRY, label: "Retry" },
  { value: TaskState.BLOCKED, label: "Blocked" },
  { value: TaskState.AGGREGATING, label: "Aggregating" },
  { value: TaskState.COMPLETED, label: "Completed" },
  { value: TaskState.ARCHIVED, label: "Archived" },
  { value: TaskState.CANCELED, label: "Canceled" },
];

// pageSize is how many tasks one page shows.
const pageSize = 20;

// queueFilterDelayMs is how long typing in the queue filter must pause before
// it applies, so a typed queue name costs one fetch, not one per keystroke.
const queueFilterDelayMs = 300;

// maxNamedFailures caps how many failed tasks a batch error message names.
const maxNamedFailures = 3;

// BatchResult is one task's outcome in a batch action; an empty error means
// the action succeeded for that task.
interface BatchResult {
  id: string;
  error: string;
}

// batchFailureMessage summarizes a partly failed batch, naming the first few
// failed tasks and why, so the operator knows which ones to look at.
function batchFailureMessage(failed: BatchResult[], total: number): string {
  const named = failed
    .slice(0, maxNamedFailures)
    .map((result) => `${result.id} (${result.error})`)
    .join(", ");
  const more = failed.length > maxNamedFailures ? ` and ${failed.length - maxNamedFailures} more` : "";

  return `${failed.length} of ${total} failed: ${named}${more}. The failed tasks stay selected.`;
}

const inputClass =
  "rounded-md border border-[var(--border)] bg-[var(--input-bg)] px-2 py-1 text-xs text-[var(--text)] placeholder:text-[var(--muted)] focus:border-indigo-500/60 focus:outline-none";

// Tasks browses the task store with queue and state filters, paging, and a
// per-task detail panel.
export function Tasks() {
  const api = useApi();
  // queueInput is what the operator has typed; queue is the applied filter,
  // which follows once typing pauses.
  const [queueInput, setQueueInput] = useState(() => taskFilters().queue);
  const [queue, setQueue] = useState(() => taskFilters().queue);
  const [state, setState] = useState<TaskState>(() => taskFilters().state);
  const [tasks, setTasks] = useState<TaskInfo[]>([]);
  // pageStack holds the page_token at the start of each visited page; its last
  // entry is the current page, and its length is the page number.
  const [pageStack, setPageStack] = useState<string[]>([""]);
  const [nextToken, setNextToken] = useState("");
  const [error, setError] = useState<string | undefined>(undefined);
  const [loading, setLoading] = useState(true);
  // selectedId is the task shown in the detail panel; detail is its latest
  // record, refetched after every action and refresh so the panel stays open
  // and current.
  const [selectedId, setSelectedId] = useState<string | undefined>(undefined);
  const [detail, setDetail] = useState<TaskInfo | undefined>(undefined);
  const [checked, setChecked] = useState<Set<string>>(new Set());
  const [refresh, setRefresh] = useState(0);
  const action = useAction(() => setRefresh((n) => n + 1));
  const tick = useRefreshTick();
  const readOnly = useReadOnly();

  const pageToken = pageStack[pageStack.length - 1];

  // toggleChecked adds or removes one task id from the batch selection.
  function toggleChecked(id: string) {
    setChecked((current) => {
      const next = new Set(current);
      next.has(id) ? next.delete(id) : next.add(id);

      return next;
    });
  }

  // toggleAll selects or clears every task on the current page.
  function toggleAll() {
    setChecked((current) => (current.size === tasks.length ? new Set() : new Set(tasks.map((task) => task.id))));
  }

  // closeDetail hides the detail panel.
  function closeDetail() {
    setSelectedId(undefined);
    setDetail(undefined);
  }

  // runBatch applies a batch RPC to the selection. Succeeded tasks leave the
  // selection; failed ones stay selected and are named in the error, so they
  // can be inspected or retried.
  function runBatch(call: (ids: string[]) => Promise<{ results: BatchResult[] }>) {
    const ids = Array.from(checked);

    return action.run(async () => {
      const { results } = await call(ids);
      const failed = results.filter((result) => result.error !== "");

      setChecked(new Set(failed.map((result) => result.id)));

      if (failed.length > 0) {
        // The rest of the batch committed, so reload before reporting the
        // failures; the action only reloads on full success.
        setRefresh((n) => n + 1);
        throw new Error(batchFailureMessage(failed, results.length));
      }
    });
  }

  useEffect(() => {
    let active = true;

    // loading is not re-raised on refresh/filter/page change: the current rows
    // stay on screen until the new ones arrive, so the view does not blink.
    api.admin
      .listTasks({ queue, state, limit: pageSize, pageToken })
      .then((resp) => {
        if (active) {
          setTasks(resp.tasks);
          // A selected task can leave the page (deleted, or moved out of the
          // filtered state); drop it so the batch bar only counts visible rows.
          const visible = new Set(resp.tasks.map((task) => task.id));
          setChecked((current) => {
            const kept = [...current].filter((id) => visible.has(id));

            return kept.length === current.size ? current : new Set(kept);
          });
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

  // Refetch the selected task whenever the list reloads, so the panel shows
  // the result of an action (and auto-refresh) without being reopened. A task
  // that no longer exists (deleted) closes the panel; any other failure keeps
  // the last known record on screen.
  useEffect(() => {
    if (selectedId === undefined) {
      return;
    }

    let active = true;

    api.tasks
      .getTask({ id: selectedId })
      .then((resp) => {
        if (active) {
          setDetail(resp.task);
        }
      })
      .catch((err: unknown) => {
        if (active && ConnectError.from(err).code === Code.NotFound) {
          closeDetail();
        }
      });

    return () => {
      active = false;
    };
  }, [api, selectedId, refresh, tick]);

  useEffect(() => {
    rememberTaskFilters({ queue, state });
  }, [queue, state]);

  // resetTo applies a filter change and returns to the first page. A filter
  // change starts a new search, so the last action's error no longer applies.
  function resetTo(change: () => void) {
    change();
    setPageStack([""]);
    closeDetail();
    setChecked(new Set());
    action.dismiss();
  }

  useEffect(() => {
    if (queueInput === queue) {
      return;
    }

    const timer = setTimeout(() => resetTo(() => setQueue(queueInput)), queueFilterDelayMs);

    return () => clearTimeout(timer);
  }, [queueInput, queue]);

  const filters = (
    <>
      <input
        aria-label="Queue filter"
        placeholder="All queues"
        value={queueInput}
        onChange={(event) => setQueueInput(event.target.value)}
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
      <ActionAlert message={action.error} onDismiss={action.dismiss} />

      <div className="flex flex-col gap-4 lg:flex-row lg:items-start">
        <div className="min-w-0 flex-1">
          <Panel title="Tasks" actions={filters}>
            {!readOnly && checked.size > 0 && (
              <div className="flex flex-wrap items-center gap-2 border-b border-[var(--border)] bg-[var(--row-hover)] px-5 py-2.5 text-sm">
                <span className="text-[var(--text-soft)]">{checked.size} selected</span>
                <span className="flex-1" />
                <ConfirmButton label="Run" onConfirm={() => runBatch((ids) => api.admin.batchRunTasks({ ids }))} />
                <ConfirmButton label="Archive" confirm onConfirm={() => runBatch((ids) => api.admin.batchArchiveTasks({ ids }))} />
                <ConfirmButton label="Cancel" confirm onConfirm={() => runBatch((ids) => api.admin.batchCancelTasks({ ids }))} />
                <ConfirmButton label="Delete" confirm danger onConfirm={() => runBatch((ids) => api.admin.batchDeleteTasks({ ids }))} />
                <button
                  type="button"
                  onClick={() => setChecked(new Set())}
                  className="rounded-md px-2 py-1 text-xs text-[var(--muted)] hover:text-[var(--text-soft)]"
                >
                  Clear
                </button>
              </div>
            )}
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
                    {!readOnly && (
                      <th className="w-10 px-5 py-2.5">
                        <input
                          type="checkbox"
                          aria-label="Select all tasks on this page"
                          checked={tasks.length > 0 && checked.size === tasks.length}
                          onChange={toggleAll}
                        />
                      </th>
                    )}
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
                      onClick={() => {
                        setSelectedId(task.id);
                        setDetail(task);
                      }}
                      className={
                        "cursor-pointer border-t border-[var(--border)] hover:bg-[var(--row-hover)] " +
                        (selectedId === task.id ? "bg-indigo-50 dark:bg-indigo-500/10" : "")
                      }
                    >
                      {!readOnly && (
                        <td className="px-5 py-3" onClick={(event) => event.stopPropagation()}>
                          <input
                            type="checkbox"
                            aria-label={`Select task ${task.id}`}
                            checked={checked.has(task.id)}
                            onChange={() => toggleChecked(task.id)}
                          />
                        </td>
                      )}
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
                      closeDetail();
                      setChecked(new Set());
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
                      closeDetail();
                      setChecked(new Set());
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

        {detail !== undefined && (
          <aside className="lg:w-80">
            <Panel title="Task detail">
              <TaskDetail key={detail.id} task={detail} api={api} action={action} readOnly={readOnly} />
            </Panel>
          </aside>
        )}
      </div>
    </div>
  );
}

// TaskDetail shows the full record for the selected task and the actions that
// apply to it: run-now, cancel, and delete (the latter two confirmed). A
// successful action reloads the view, which refetches this task, so the panel
// stays open and shows the result.
function TaskDetail({
  task,
  api,
  action,
  readOnly,
}: {
  task: TaskInfo;
  api: Api;
  action: ActionState;
  readOnly: boolean;
}) {
  const act = (fn: () => Promise<unknown>) => action.run(fn);

  // Only offer actions the task's state allows, matching the broker's rules,
  // so the dashboard never sends an operation that fails as invalid-state.
  // Read-only mode hides every action.
  const state = task.state;
  const canRun = !readOnly && (state === TaskState.SCHEDULED || state === TaskState.RETRY || state === TaskState.ARCHIVED);
  const canArchive =
    !readOnly && (state === TaskState.SCHEDULED || state === TaskState.PENDING || state === TaskState.RETRY);
  const canCancel =
    !readOnly &&
    (state === TaskState.SCHEDULED ||
      state === TaskState.PENDING ||
      state === TaskState.RETRY ||
      state === TaskState.ACTIVE);
  const canDelete = !readOnly && state !== TaskState.ACTIVE;
  const canReschedule =
    !readOnly && (state === TaskState.SCHEDULED || state === TaskState.PENDING || state === TaskState.RETRY);

  const [rescheduleAt, setRescheduleAt] = useState("");

  const rows: [string, string][] = [
    ["ID", task.id],
    ["Type", task.type],
    ["Queue", task.queue],
    ["Priority", String(task.priority)],
    ["Retried", `${task.retried}/${task.maxRetry}`],
    ["Last error", orDash(task.lastError)],
    ["Enqueued", formatTime(task.enqueuedAt)],
    ["Process at", formatTime(task.processAt)],
    ["Started", formatTime(task.startedAt)],
    ["Completed", formatTime(task.completedAt)],
    // A waiting task keeps its last attempt's start time, so only an active or
    // finished task has a meaningful duration.
    ["Duration", state === TaskState.ACTIVE || task.completedAt ? formatDuration(task.startedAt, task.completedAt) : orDash("")],
    [
      "Progress",
      task.progress > 0 || task.progressMessage !== ""
        ? `${task.progress}%${task.progressMessage !== "" ? ` (${task.progressMessage})` : ""}`
        : orDash(""),
    ],
  ];

  const payload = decodePayload(task.payload, task.contentType);

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

      <div className="mt-4">
        <div className="mb-1 flex items-center justify-between">
          <span className="text-[var(--muted)]">
            Payload{task.contentType !== "" ? ` (${task.contentType})` : ""}
          </span>
        </div>
        <pre className="max-h-64 overflow-auto rounded-md border border-[var(--border)] bg-[var(--bg)] p-3 font-mono text-xs text-[var(--text-soft)]">
          {payload}
        </pre>
      </div>

      <div className="mt-4 flex flex-wrap gap-2">
        {canRun && (
          <ConfirmButton label="Run now" onConfirm={() => act(() => api.admin.runTask({ id: task.id }))} />
        )}
        {canArchive && (
          <ConfirmButton label="Archive" confirm onConfirm={() => act(() => api.admin.archiveTask({ id: task.id }))} />
        )}
        {canCancel && (
          <ConfirmButton label="Cancel" confirm onConfirm={() => act(() => api.admin.cancelTask({ id: task.id }))} />
        )}
        {canDelete && (
          <ConfirmButton label="Delete" confirm danger onConfirm={() => act(() => api.admin.deleteTask({ id: task.id }))} />
        )}
        {!canRun && !canArchive && !canCancel && !canDelete && !canReschedule && (
          <span className="text-xs text-[var(--muted)]">No actions available.</span>
        )}
      </div>

      {canReschedule && (
        <div className="mt-3 flex items-center gap-2">
          <input
            type="datetime-local"
            aria-label="Reschedule to"
            value={rescheduleAt}
            onChange={(event) => setRescheduleAt(event.target.value)}
            className="rounded border border-[var(--border)] bg-[var(--bg)] px-2 py-0.5 text-xs text-[var(--text-soft)]"
          />
          <button
            type="button"
            disabled={rescheduleAt === ""}
            onClick={() => {
              const when = new Date(rescheduleAt);

              if (Number.isNaN(when.getTime())) {
                return;
              }

              void act(() => api.admin.rescheduleTask({ id: task.id, processAt: timestampFromDate(when) }));
            }}
            className="rounded bg-[var(--btn-bg)] px-2 py-0.5 text-xs text-[var(--text-soft)] hover:bg-[var(--btn-hover)] disabled:opacity-50"
          >
            Reschedule
          </button>
        </div>
      )}
    </div>
  );
}
