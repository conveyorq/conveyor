import { useApi } from "../api/context.tsx";
import { useQuery } from "../api/useQuery.ts";
import { QueryView } from "../components/QueryView.tsx";
import { Panel } from "../components/Panel.tsx";
import { formatTime, orDash, relativeTime } from "../lib/format.ts";

// Workers shows the worker sessions connected to the node serving the request:
// the queues each serves, its declared concurrency, SDK version, and uptime.
export function Workers() {
  const api = useApi();
  const query = useQuery(() => api.admin.listWorkerSessions({}), []);

  return (
    <Panel title="Worker sessions">
      <QueryView query={query}>
        {(data) =>
          data.sessions.length === 0 ? (
            <p className="px-5 py-8 text-sm text-[var(--muted)]">No workers connected to this node.</p>
          ) : (
            <table className="w-full text-sm">
              <thead>
                <tr className="text-left text-xs font-medium uppercase tracking-wider text-[var(--muted)]">
                  <th className="px-5 py-2.5">Session</th>
                  <th className="px-5 py-2.5">Queues</th>
                  <th className="px-5 py-2.5 text-right">Concurrency</th>
                  <th className="px-5 py-2.5">SDK</th>
                  <th className="px-5 py-2.5">Connected</th>
                </tr>
              </thead>
              <tbody>
                {data.sessions.map((session) => (
                  <tr key={session.id} className="border-t border-[var(--border)] hover:bg-[var(--row-hover)]">
                    <td className="px-5 py-3 font-mono text-[var(--text-soft)]">{session.id}</td>
                    <td className="px-5 py-3 text-[var(--text-soft)]">{session.queues.join(", ")}</td>
                    <td className="px-5 py-3 text-right tabular-nums text-[var(--text-soft)]">{session.concurrency}</td>
                    <td className="px-5 py-3 text-[var(--text-soft)]">{orDash(session.sdkVersion)}</td>
                    <td className="px-5 py-3 text-[var(--muted)]" title={formatTime(session.connectedAt)}>
                      {relativeTime(session.connectedAt)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )
        }
      </QueryView>
    </Panel>
  );
}
