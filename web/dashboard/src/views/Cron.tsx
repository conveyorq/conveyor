import { useState } from "react";
import { useApi } from "../api/context.tsx";
import { useQuery } from "../api/useQuery.ts";
import { useAction } from "../api/useAction.ts";
import { useReadOnly } from "../api/readonly.tsx";
import { QueryView } from "../components/QueryView.tsx";
import { ConfirmButton } from "../components/ConfirmButton.tsx";
import { Panel } from "../components/Panel.tsx";
import { Badge } from "../components/Badge.tsx";
import { formatTime } from "../lib/format.ts";
import type { CronEntry } from "../gen/conveyor/v1/service_pb.ts";

const inputClass =
  "w-full rounded-md border border-[var(--border)] bg-[var(--input-bg)] px-2 py-1 text-sm text-[var(--text)] placeholder:text-[var(--muted)] focus:border-indigo-500/60 focus:outline-none";

// emptyForm is the cleared schedule-editor state.
const emptyForm = { id: "", spec: "", taskType: "", queue: "", contentType: "", payload: "" };

// Cron lists the registered schedules with their spec, target, next run, and
// paused flag, and (outside read-only mode) an editor to create or update them.
export function Cron() {
  const api = useApi();
  const query = useQuery(() => api.admin.listCron({}), []);
  const action = useAction(query.reload);
  const readOnly = useReadOnly();
  const [form, setForm] = useState(emptyForm);

  // loadEntry populates the editor from an existing schedule for editing.
  function loadEntry(entry: CronEntry) {
    setForm({
      id: entry.id,
      spec: entry.spec,
      taskType: entry.taskType,
      queue: entry.queue,
      contentType: entry.contentType,
      payload: new TextDecoder().decode(entry.payload),
    });
  }

  function save() {
    const id = form.id.trim();
    const spec = form.spec.trim();
    const taskType = form.taskType.trim();

    // Require the fields the entry cannot exist without before the round-trip,
    // so a missing id/spec/type gives an immediate message rather than a server
    // invalid-argument error.
    if (id === "" || spec === "" || taskType === "") {
      return action.run(() =>
        Promise.reject(new Error("ID, schedule spec, and task type are required.")),
      );
    }

    return action
      .run(() =>
        api.admin.upsertCron({
          entry: {
            id,
            spec,
            taskType,
            queue: form.queue.trim(),
            contentType: form.contentType.trim(),
            payload: new TextEncoder().encode(form.payload),
          },
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
        <Panel title="Schedule editor">
          <div className="grid grid-cols-2 gap-3 px-5 py-4 lg:grid-cols-3">
            <label className="text-xs text-[var(--muted)]">
              ID
              <input className={inputClass} value={form.id} onChange={(e) => setForm({ ...form, id: e.target.value })} placeholder="hourly-report" />
            </label>
            <label className="text-xs text-[var(--muted)]">
              Schedule (6-field cron)
              <input className={inputClass} value={form.spec} onChange={(e) => setForm({ ...form, spec: e.target.value })} placeholder="0 0 * * * *" />
            </label>
            <label className="text-xs text-[var(--muted)]">
              Task type
              <input className={inputClass} value={form.taskType} onChange={(e) => setForm({ ...form, taskType: e.target.value })} placeholder="report:hourly" />
            </label>
            <label className="text-xs text-[var(--muted)]">
              Queue
              <input className={inputClass} value={form.queue} onChange={(e) => setForm({ ...form, queue: e.target.value })} placeholder="default" />
            </label>
            <label className="text-xs text-[var(--muted)]">
              Content type
              <input className={inputClass} value={form.contentType} onChange={(e) => setForm({ ...form, contentType: e.target.value })} placeholder="application/json" />
            </label>
            <label className="col-span-2 text-xs text-[var(--muted)] lg:col-span-3">
              Payload
              <textarea className={`${inputClass} font-mono`} rows={2} value={form.payload} onChange={(e) => setForm({ ...form, payload: e.target.value })} placeholder='{"report":"daily"}' />
            </label>
            <div className="col-span-2 flex gap-2 lg:col-span-3">
              <ConfirmButton label="Save schedule" onConfirm={save} />
              <button type="button" onClick={() => setForm(emptyForm)} className="rounded px-2 py-0.5 text-xs text-[var(--muted)] hover:text-[var(--text-soft)]">
                Clear
              </button>
            </div>
          </div>
        </Panel>
      )}

      <Panel title="Schedules">
        <QueryView query={query}>
          {(data) =>
            data.entries.length === 0 ? (
              <p className="px-5 py-8 text-sm text-[var(--muted)]">No cron entries.</p>
            ) : (
              <table className="w-full text-sm">
                <thead>
                  <tr className="text-left text-xs font-medium uppercase tracking-wider text-[var(--muted)]">
                    <th className="px-5 py-2.5">ID</th>
                    <th className="px-5 py-2.5">Schedule</th>
                    <th className="px-5 py-2.5">Type</th>
                    <th className="px-5 py-2.5">Queue</th>
                    <th className="px-5 py-2.5">Next run</th>
                    <th className="px-5 py-2.5">State</th>
                    {!readOnly && <th className="px-5 py-2.5 text-right">Actions</th>}
                  </tr>
                </thead>
                <tbody>
                  {data.entries.map((entry) => (
                    <tr key={entry.id} className="border-t border-[var(--border)] hover:bg-[var(--row-hover)]">
                      <td className="px-5 py-3 font-medium text-[var(--text)]">{entry.id}</td>
                      <td className="px-5 py-3 font-mono text-[var(--text-soft)]">{entry.spec}</td>
                      <td className="px-5 py-3 text-[var(--text-soft)]">{entry.taskType}</td>
                      <td className="px-5 py-3 text-[var(--text-soft)]">{entry.queue}</td>
                      <td className="px-5 py-3 text-[var(--muted)]">
                        {entry.paused ? "—" : formatTime(entry.nextRunAt)}
                      </td>
                      <td className="px-5 py-3">
                        {entry.paused ? <Badge tone="amber">paused</Badge> : <Badge tone="emerald">active</Badge>}
                      </td>
                      {!readOnly && (
                        <td className="px-5 py-3 text-right">
                          <span className="inline-flex gap-2">
                            <ConfirmButton label="Edit" onConfirm={async () => loadEntry(entry)} />
                            {entry.paused ? (
                              <ConfirmButton label="Resume" onConfirm={() => action.run(() => api.admin.resumeCron({ id: entry.id }))} />
                            ) : (
                              <ConfirmButton label="Pause" onConfirm={() => action.run(() => api.admin.pauseCron({ id: entry.id }))} />
                            )}
                            <ConfirmButton label="Delete" confirm danger onConfirm={() => action.run(() => api.admin.deleteCron({ id: entry.id }))} />
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
