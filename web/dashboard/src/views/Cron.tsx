import { useApi } from "../api/context.tsx";
import { useQuery } from "../api/useQuery.ts";
import { useAction } from "../api/useAction.ts";
import { QueryView } from "../components/QueryView.tsx";
import { ConfirmButton } from "../components/ConfirmButton.tsx";
import { Panel } from "../components/Panel.tsx";
import { Badge } from "../components/Badge.tsx";

// Cron lists the registered schedules with their spec, target, and paused flag.
export function Cron() {
  const api = useApi();
  const query = useQuery(() => api.admin.listCron({}), []);
  const action = useAction(query.reload);

  return (
    <div className="space-y-4">
      {action.error !== undefined && (
        <p role="alert" className="rounded-lg border border-rose-500/30 bg-rose-50 px-4 py-2.5 text-sm text-rose-700 dark:bg-rose-500/10 dark:text-rose-300">
          {action.error}
        </p>
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
                    <th className="px-5 py-2.5">State</th>
                    <th className="px-5 py-2.5 text-right">Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {data.entries.map((entry) => (
                    <tr key={entry.id} className="border-t border-[var(--border)] hover:bg-[var(--row-hover)]">
                      <td className="px-5 py-3 font-medium text-[var(--text)]">{entry.id}</td>
                      <td className="px-5 py-3 font-mono text-[var(--text-soft)]">{entry.spec}</td>
                      <td className="px-5 py-3 text-[var(--text-soft)]">{entry.taskType}</td>
                      <td className="px-5 py-3 text-[var(--text-soft)]">{entry.queue}</td>
                      <td className="px-5 py-3">
                        {entry.paused ? <Badge tone="amber">paused</Badge> : <Badge tone="emerald">active</Badge>}
                      </td>
                      <td className="px-5 py-3 text-right">
                        <span className="inline-flex gap-2">
                          {entry.paused ? (
                            <ConfirmButton
                              label="Resume"
                              onConfirm={() => action.run(() => api.admin.resumeCron({ id: entry.id }))}
                            />
                          ) : (
                            <ConfirmButton
                              label="Pause"
                              onConfirm={() => action.run(() => api.admin.pauseCron({ id: entry.id }))}
                            />
                          )}
                          <ConfirmButton
                            label="Delete"
                            confirm
                            danger
                            onConfirm={() => action.run(() => api.admin.deleteCron({ id: entry.id }))}
                          />
                        </span>
                      </td>
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
