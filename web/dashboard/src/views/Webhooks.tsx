import { useState } from "react";
import { useApi } from "../api/context.tsx";
import { useQuery } from "../api/useQuery.ts";
import { useAction } from "../api/useAction.ts";
import { useReadOnly } from "../api/readonly.tsx";
import { QueryView } from "../components/QueryView.tsx";
import { ConfirmButton } from "../components/ConfirmButton.tsx";
import { Panel } from "../components/Panel.tsx";
import { Badge } from "../components/Badge.tsx";
import type { WebhookWorker } from "../gen/conveyor/v1/service_pb.ts";

const inputClass =
  "w-full rounded-md border border-[var(--border)] bg-[var(--input-bg)] px-2 py-1 text-sm text-[var(--text)] placeholder:text-[var(--muted)] focus:border-indigo-500/60 focus:outline-none";

// emptyForm is the cleared registration-editor state. Queues are entered as
// "name=weight" pairs and secrets one per line; both parse on save.
const emptyForm = { name: "", url: "", queues: "", concurrency: "4", secrets: "" };

// parseQueues turns comma-separated "name" or "name=weight" pairs into the
// wire map; a bare name weighs one.
function parseQueues(text: string): Record<string, number> {
  const queues: Record<string, number> = {};

  for (const part of text.split(",")) {
    const entry = part.trim();
    if (entry === "") continue;

    const [name, weight] = entry.split("=", 2);
    queues[name.trim()] = weight === undefined ? 1 : Number(weight);
  }

  return queues;
}

// Webhooks lists the webhook worker registrations (endpoints the server
// pushes tasks to) and, outside read-only mode, an editor to create or
// update them. Secrets are write-only: the server redacts them in listings.
export function Webhooks() {
  const api = useApi();
  const query = useQuery(() => api.admin.listWebhookWorkers({}), []);
  const action = useAction(query.reload);
  const readOnly = useReadOnly();
  const [form, setForm] = useState(emptyForm);

  // loadWorker populates the editor from an existing registration; the
  // secrets stay blank and must be re-entered, because listings redact them.
  function loadWorker(worker: WebhookWorker) {
    setForm({
      name: worker.name,
      url: worker.url,
      queues: Object.entries(worker.queues)
        .map(([queue, weight]) => `${queue}=${weight}`)
        .join(", "),
      concurrency: String(worker.concurrency),
      secrets: "",
    });
  }

  function save() {
    const name = form.name.trim();
    const url = form.url.trim();
    const queues = parseQueues(form.queues);
    const secrets = form.secrets
      .split("\n")
      .map((secret) => secret.trim())
      .filter((secret) => secret !== "");

    // Require what the registration cannot exist without before the
    // round-trip, so a gap gives an immediate message rather than a server
    // invalid-argument error.
    if (name === "" || url === "" || Object.keys(queues).length === 0 || secrets.length === 0) {
      return action.run(() =>
        Promise.reject(new Error("Name, URL, at least one queue, and a secret are required.")),
      );
    }

    return action
      .run(() =>
        api.admin.upsertWebhookWorker({
          worker: {
            name,
            url,
            queues,
            concurrency: Number(form.concurrency) || 1,
            secrets,
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
        <Panel title="Registration editor">
          <div className="grid grid-cols-2 gap-3 px-5 py-4 lg:grid-cols-3">
            <label className="text-xs text-[var(--muted)]">
              Name
              <input className={inputClass} value={form.name} onChange={(e) => setForm({ ...form, name: e.target.value })} placeholder="billing-hooks" />
            </label>
            <label className="text-xs text-[var(--muted)]">
              URL
              <input className={inputClass} value={form.url} onChange={(e) => setForm({ ...form, url: e.target.value })} placeholder="https://hooks.example.com/tasks" />
            </label>
            <label className="text-xs text-[var(--muted)]">
              Queues (name=weight, comma-separated)
              <input className={inputClass} value={form.queues} onChange={(e) => setForm({ ...form, queues: e.target.value })} placeholder="billing=3, default=1" />
            </label>
            <label className="text-xs text-[var(--muted)]">
              Concurrency
              <input className={inputClass} type="number" min="1" value={form.concurrency} onChange={(e) => setForm({ ...form, concurrency: e.target.value })} />
            </label>
            <label className="col-span-2 text-xs text-[var(--muted)]">
              Secrets (one per line, newest first; re-enter when editing)
              <textarea className={`${inputClass} font-mono`} rows={2} value={form.secrets} onChange={(e) => setForm({ ...form, secrets: e.target.value })} placeholder="signing secret" />
            </label>
            <div className="col-span-2 flex gap-2 lg:col-span-3">
              <ConfirmButton label="Save registration" onConfirm={save} />
              <button type="button" onClick={() => setForm(emptyForm)} className="rounded px-2 py-0.5 text-xs text-[var(--muted)] hover:text-[var(--text-soft)]">
                Clear
              </button>
            </div>
          </div>
        </Panel>
      )}

      <Panel title="Webhook workers">
        <QueryView query={query}>
          {(data) =>
            data.workers.length === 0 ? (
              <p className="px-5 py-8 text-sm text-[var(--muted)]">No webhook workers.</p>
            ) : (
              <table className="w-full text-sm">
                <thead>
                  <tr className="text-left text-xs font-medium uppercase tracking-wider text-[var(--muted)]">
                    <th className="px-5 py-2.5">Name</th>
                    <th className="px-5 py-2.5">URL</th>
                    <th className="px-5 py-2.5">Queues</th>
                    <th className="px-5 py-2.5">Concurrency</th>
                    <th className="px-5 py-2.5">State</th>
                    {!readOnly && <th className="px-5 py-2.5 text-right">Actions</th>}
                  </tr>
                </thead>
                <tbody>
                  {data.workers.map((worker) => (
                    <tr key={worker.name} className="border-t border-[var(--border)] hover:bg-[var(--row-hover)]">
                      <td className="px-5 py-3 font-medium text-[var(--text)]">{worker.name}</td>
                      <td className="px-5 py-3 font-mono text-[var(--text-soft)]">{worker.url}</td>
                      <td className="px-5 py-3 text-[var(--text-soft)]">
                        {Object.entries(worker.queues)
                          .map(([queue, weight]) => `${queue}=${weight}`)
                          .join(", ")}
                      </td>
                      <td className="px-5 py-3 text-[var(--text-soft)]">{worker.concurrency}</td>
                      <td className="px-5 py-3">
                        {worker.paused ? <Badge tone="amber">paused</Badge> : <Badge tone="emerald">active</Badge>}
                      </td>
                      {!readOnly && (
                        <td className="px-5 py-3 text-right">
                          <span className="inline-flex gap-2">
                            <ConfirmButton label="Edit" onConfirm={async () => loadWorker(worker)} />
                            {worker.paused ? (
                              <ConfirmButton label="Resume" onConfirm={() => action.run(() => api.admin.resumeWebhookWorker({ name: worker.name }))} />
                            ) : (
                              <ConfirmButton label="Pause" onConfirm={() => action.run(() => api.admin.pauseWebhookWorker({ name: worker.name }))} />
                            )}
                            <ConfirmButton label="Delete" confirm danger onConfirm={() => action.run(() => api.admin.deleteWebhookWorker({ name: worker.name }))} />
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
