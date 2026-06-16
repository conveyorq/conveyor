import { useState } from "react";
import { useApi } from "../api/context.tsx";
import { useQuery } from "../api/useQuery.ts";
import { useAction } from "../api/useAction.ts";
import { useReadOnly } from "../api/readonly.tsx";
import { QueryView } from "../components/QueryView.tsx";
import { ConfirmButton } from "../components/ConfirmButton.tsx";
import { Panel } from "../components/Panel.tsx";
import type { RateLimitInfo } from "../gen/conveyor/v1/service_pb.ts";

const inputClass =
  "w-full rounded-md border border-[var(--border)] bg-[var(--input-bg)] px-2 py-1 text-sm text-[var(--text)] placeholder:text-[var(--muted)] focus:border-indigo-500/60 focus:outline-none";

// emptyForm is the cleared limit-editor state. Burst defaults to 1, the
// smallest valid bucket, matching the CLI default.
const emptyForm = { queue: "", rate: "", burst: "1" };

// Limits manages per-queue dispatch rate-limit overrides: a queue with an
// override dispatches at most rate tasks/second with the given burst, instead
// of the server's global default. Removing an override reverts the queue to the
// default. The editor and actions are hidden in read-only mode.
export function Limits() {
  const api = useApi();
  const query = useQuery(() => api.admin.listRateLimits({}), []);
  const action = useAction(query.reload);
  const readOnly = useReadOnly();
  const [form, setForm] = useState(emptyForm);

  // loadLimit populates the editor from an existing override for editing.
  function loadLimit(limit: RateLimitInfo) {
    setForm({ queue: limit.queue, rate: String(limit.ratePerSec), burst: String(limit.burst) });
  }

  function save() {
    return action
      .run(() =>
        api.admin.setQueueRateLimit({
          queue: form.queue.trim(),
          ratePerSec: Number(form.rate),
          burst: Number(form.burst),
        }),
      )
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
        <Panel title="Rate-limit editor">
          <div className="grid grid-cols-2 gap-3 px-5 py-4 lg:grid-cols-3">
            <label className="text-xs text-[var(--muted)]">
              Queue
              <input className={inputClass} value={form.queue} onChange={(e) => setForm({ ...form, queue: e.target.value })} placeholder="email" />
            </label>
            <label className="text-xs text-[var(--muted)]">
              Rate (tasks/second)
              <input className={inputClass} type="number" min="0" step="any" value={form.rate} onChange={(e) => setForm({ ...form, rate: e.target.value })} placeholder="50" />
            </label>
            <label className="text-xs text-[var(--muted)]">
              Burst
              <input className={inputClass} type="number" min="1" step="1" value={form.burst} onChange={(e) => setForm({ ...form, burst: e.target.value })} placeholder="10" />
            </label>
            <div className="col-span-2 flex gap-2 lg:col-span-3">
              <ConfirmButton label="Save limit" onConfirm={save} />
              <button type="button" onClick={() => setForm(emptyForm)} className="rounded px-2 py-0.5 text-xs text-[var(--muted)] hover:text-[var(--text-soft)]">
                Clear
              </button>
            </div>
          </div>
        </Panel>
      )}

      <Panel title="Rate limits">
        <QueryView query={query}>
          {(data) =>
            data.limits.length === 0 ? (
              <p className="px-5 py-8 text-sm text-[var(--muted)]">No rate limits set. Queues use the server default.</p>
            ) : (
              <table className="w-full text-sm">
                <thead>
                  <tr className="text-left text-xs font-medium uppercase tracking-wider text-[var(--muted)]">
                    <th className="px-5 py-2.5">Queue</th>
                    <th className="px-5 py-2.5 text-right">Rate /s</th>
                    <th className="px-5 py-2.5 text-right">Burst</th>
                    {!readOnly && <th className="px-5 py-2.5 text-right">Actions</th>}
                  </tr>
                </thead>
                <tbody>
                  {data.limits.map((limit) => (
                    <tr key={limit.queue} className="border-t border-[var(--border)] hover:bg-[var(--row-hover)]">
                      <td className="px-5 py-3 font-medium text-[var(--text)]">{limit.queue}</td>
                      <td className="px-5 py-3 text-right tabular-nums text-[var(--text-soft)]">{limit.ratePerSec}</td>
                      <td className="px-5 py-3 text-right tabular-nums text-[var(--text-soft)]">{limit.burst}</td>
                      {!readOnly && (
                        <td className="px-5 py-3 text-right">
                          <span className="inline-flex gap-2">
                            <ConfirmButton label="Edit" onConfirm={async () => loadLimit(limit)} />
                            <ConfirmButton label="Remove" confirm danger onConfirm={() => action.run(() => api.admin.deleteQueueRateLimit({ queue: limit.queue }))} />
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
