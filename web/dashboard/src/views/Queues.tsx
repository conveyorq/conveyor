import { useApi } from "../api/context.tsx";
import { useQuery } from "../api/useQuery.ts";
import { useAction } from "../api/useAction.ts";
import { useReadOnly } from "../api/readonly.tsx";
import { QueryView } from "../components/QueryView.tsx";
import { ConfirmButton } from "../components/ConfirmButton.tsx";
import { Panel } from "../components/Panel.tsx";
import { Badge } from "../components/Badge.tsx";
import { formatNumber } from "../lib/format.ts";

// columns are the per-queue depth counters, in the order shown.
const columns: { key: "scheduled" | "aggregating" | "blocked" | "pending" | "active" | "retry" | "completed" | "archived"; label: string }[] = [
  { key: "scheduled", label: "Scheduled" },
  { key: "aggregating", label: "Aggregating" },
  { key: "blocked", label: "Blocked" },
  { key: "pending", label: "Pending" },
  { key: "active", label: "Active" },
  { key: "retry", label: "Retry" },
  { key: "completed", label: "Completed" },
  { key: "archived", label: "Archived" },
];

// Queues lists every queue with its per-state depth and paused flag.
export function Queues() {
  const api = useApi();
  const query = useQuery(() => api.admin.listQueues({}), []);
  const action = useAction(query.reload);
  const readOnly = useReadOnly();

  return (
    <div className="space-y-4">
      {action.error !== undefined && (
        <p role="alert" className="rounded-lg border border-rose-500/30 bg-rose-50 px-4 py-2.5 text-sm text-rose-700 dark:bg-rose-500/10 dark:text-rose-300">
          {action.error}
        </p>
      )}

      <Panel title="Queues">
        <QueryView query={query}>
          {(data) =>
            data.queues.length === 0 ? (
              <p className="px-5 py-8 text-sm text-[var(--muted)]">No queues yet.</p>
            ) : (
              <table className="w-full text-sm">
                <thead>
                  <tr className="text-left text-xs font-medium uppercase tracking-wider text-[var(--muted)]">
                    <th className="px-5 py-2.5">Queue</th>
                    <th className="px-5 py-2.5">State</th>
                    {columns.map((c) => (
                      <th key={c.key} className="px-5 py-2.5 text-right">
                        {c.label}
                      </th>
                    ))}
                    {!readOnly && <th className="px-5 py-2.5 text-right">Actions</th>}
                  </tr>
                </thead>
                <tbody>
                  {data.queues.map((q) => (
                    <tr key={q.name} className="border-t border-[var(--border)] hover:bg-[var(--row-hover)]">
                      <td className="px-5 py-3 font-medium text-[var(--text)]">{q.name}</td>
                      <td className="px-5 py-3">
                        {q.paused ? <Badge tone="amber">paused</Badge> : <Badge tone="emerald">active</Badge>}
                      </td>
                      {columns.map((c) => (
                        <td key={c.key} className="px-5 py-3 text-right tabular-nums text-[var(--text-soft)]">
                          {formatNumber(q[c.key])}
                        </td>
                      ))}
                      {!readOnly && (
                        <td className="px-5 py-3 text-right">
                          {q.paused ? (
                            <ConfirmButton
                              label="Resume"
                              onConfirm={() => action.run(() => api.admin.resumeQueue({ queue: q.name }))}
                            />
                          ) : (
                            <ConfirmButton
                              label="Pause"
                              onConfirm={() => action.run(() => api.admin.pauseQueue({ queue: q.name }))}
                            />
                          )}
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
