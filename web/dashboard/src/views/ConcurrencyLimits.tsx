import { useState } from "react";
import { useApi } from "../api/context.tsx";
import { useQuery } from "../api/useQuery.ts";
import { useAction } from "../api/useAction.ts";
import { useReadOnly } from "../api/readonly.tsx";
import { QueryView } from "../components/QueryView.tsx";
import { ConfirmButton } from "../components/ConfirmButton.tsx";
import { Panel } from "../components/Panel.tsx";
import type { ConcurrencyLimitInfo } from "../gen/conveyor/v1/service_pb.ts";

const inputClass =
  "w-full rounded-md border border-[var(--border)] bg-[var(--input-bg)] px-2 py-1 text-sm text-[var(--text)] placeholder:text-[var(--muted)] focus:border-indigo-500/60 focus:outline-none";

// emptyForm is the cleared limit-editor state.
const emptyForm = { queue: "", maxActive: "" };

// ConcurrencyLimits manages per-queue per-key concurrency limits: a queue with a
// limit dispatches at most max-active tasks sharing a concurrency key at once,
// holding the rest pending. Removing a limit leaves the queue's keys unbounded.
// The editor and actions are hidden in read-only mode.
export function ConcurrencyLimits() {
  const api = useApi();
  const query = useQuery(() => api.admin.listConcurrencyLimits({}), []);
  const action = useAction(query.reload);
  const readOnly = useReadOnly();
  const [form, setForm] = useState(emptyForm);

  // loadLimit populates the editor from an existing limit for editing.
  function loadLimit(limit: ConcurrencyLimitInfo) {
    setForm({ queue: limit.queue, maxActive: String(limit.maxActive) });
  }

  function save() {
    const queue = form.queue.trim();
    const maxActive = Number(form.maxActive);

    // Validate before the round-trip so an empty queue or a sub-1/non-integer
    // max gives an immediate message instead of a server invalid-argument error.
    if (queue === "" || !Number.isInteger(maxActive) || maxActive < 1) {
      return action.run(() =>
        Promise.reject(new Error("Queue is required and max active must be an integer ≥ 1.")),
      );
    }

    return action
      .run(() => api.admin.setQueueConcurrencyLimit({ queue, maxActive }))
      .then(() => setForm(emptyForm));
  }

  return (
    <div className="space-y-4">
      {action.error !== undefined && (
        <p role="alert" className="rounded-lg border border-rose-500/30 bg-rose-50 px-4 py-2.5 text-sm text-rose-700 dark:bg-rose-500/10 dark:text-rose-300">
          {action.error}
        </p>
      )}

      {!readOnly && (
        <Panel title="Concurrency-limit editor">
          <div className="grid grid-cols-2 gap-3 px-5 py-4">
            <label className="text-xs text-[var(--muted)]">
              Queue
              <input className={inputClass} value={form.queue} onChange={(e) => setForm({ ...form, queue: e.target.value })} placeholder="email" />
            </label>
            <label className="text-xs text-[var(--muted)]">
              Max active per key
              <input className={inputClass} type="number" min="1" step="1" value={form.maxActive} onChange={(e) => setForm({ ...form, maxActive: e.target.value })} placeholder="5" />
            </label>
            <div className="col-span-2 flex gap-2">
              <ConfirmButton label="Save limit" onConfirm={save} />
              <button type="button" onClick={() => setForm(emptyForm)} className="rounded px-2 py-0.5 text-xs text-[var(--muted)] hover:text-[var(--text-soft)]">
                Clear
              </button>
            </div>
          </div>
        </Panel>
      )}

      <Panel title="Concurrency limits">
        <QueryView query={query}>
          {(data) =>
            data.limits.length === 0 ? (
              <p className="px-5 py-8 text-sm text-[var(--muted)]">No concurrency limits set. Queue keys are unbounded.</p>
            ) : (
              <table className="w-full text-sm">
                <thead>
                  <tr className="text-left text-xs font-medium uppercase tracking-wider text-[var(--muted)]">
                    <th className="px-5 py-2.5">Queue</th>
                    <th className="px-5 py-2.5 text-right">Max / key</th>
                    {!readOnly && <th className="px-5 py-2.5 text-right">Actions</th>}
                  </tr>
                </thead>
                <tbody>
                  {data.limits.map((limit) => (
                    <tr key={limit.queue} className="border-t border-[var(--border)] hover:bg-[var(--row-hover)]">
                      <td className="px-5 py-3 font-medium text-[var(--text)]">{limit.queue}</td>
                      <td className="px-5 py-3 text-right tabular-nums text-[var(--text-soft)]">{limit.maxActive}</td>
                      {!readOnly && (
                        <td className="px-5 py-3 text-right">
                          <span className="inline-flex gap-2">
                            <ConfirmButton label="Edit" onConfirm={async () => loadLimit(limit)} />
                            <ConfirmButton label="Remove" confirm danger onConfirm={() => action.run(() => api.admin.deleteQueueConcurrencyLimit({ queue: limit.queue }))} />
                          </span>
                        </td>
                      )}
                    </tr>
                  ))}
                </tbody>
              </table>
            )
          }
        </QueryView>
      </Panel>
    </div>
  );
}
