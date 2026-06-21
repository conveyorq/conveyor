import { useApi } from "../api/context.tsx";
import { useQuery } from "../api/useQuery.ts";
import { QueryView } from "../components/QueryView.tsx";
import { Panel } from "../components/Panel.tsx";
import type { Tone } from "../components/Badge.tsx";
import { formatNumber, relativeTime } from "../lib/format.ts";
import type { QueueInfo } from "../gen/conveyor/v1/service_pb.ts";

// queueColumns are the aggregate task-state totals shown as stat cards, with
// the tone each is colored and a tooltip explaining the state.
const queueColumns: {
  key: keyof Pick<QueueInfo, "pending" | "active" | "retry" | "completed" | "archived">;
  label: string;
  tone: Tone;
  hint: string;
}[] = [
  { key: "pending", label: "Pending", tone: "sky", hint: "Due and waiting for a free worker." },
  { key: "active", label: "Active", tone: "amber", hint: "Currently running on a worker." },
  { key: "retry", label: "Retry", tone: "orange", hint: "Failed; waiting for backoff before retrying." },
  { key: "completed", label: "Completed", tone: "emerald", hint: "Finished successfully; kept for the retention window." },
  { key: "archived", label: "Archived", tone: "rose", hint: "Dead-lettered: retries exhausted or skipped." },
];

// Overview is the dashboard landing page: headline totals across all queues,
// plus the worker, queue, and node counts and the cluster membership.
export function Overview() {
  const api = useApi();
  const query = useQuery(async () => {
    const [queues, cluster, workers] = await Promise.all([
      api.admin.listQueues({}),
      api.admin.clusterInfo({}),
      api.admin.listWorkerSessions({}),
    ]);

    return { queues: queues.queues, nodes: cluster.nodes, workers: workers.sessions };
  }, []);

  return (
    <QueryView query={query}>
      {(data) => {
        // Sum every state in a single pass over the queues rather than one
        // reduce per stat card.
        const totals = data.queues.reduce(
          (acc, queue) => {
            for (const column of queueColumns) {
              acc[column.key] += queue[column.key];
            }

            return acc;
          },
          { pending: 0n, active: 0n, retry: 0n, completed: 0n, archived: 0n } as Record<
            (typeof queueColumns)[number]["key"],
            bigint
          >,
        );

        return (
          <div className="space-y-5">
            <div className="grid grid-cols-2 gap-4 sm:grid-cols-3 lg:grid-cols-4">
              <Stat label="Queues" value={formatNumber(data.queues.length)} tone="indigo" hint="Named queues with work." />
              <Stat label="Workers (node)" value={formatNumber(data.workers.length)} tone="indigo" hint="Worker sessions connected to the node serving this request, not the whole cluster." />
              <Stat label="Nodes" value={formatNumber(data.nodes.length)} tone="indigo" hint="conveyord nodes in the cluster." />
              {queueColumns.map((column) => (
                <Stat
                  key={column.key}
                  label={`Tasks ${column.label}`}
                  value={formatNumber(totals[column.key])}
                  tone={column.tone}
                  hint={column.hint}
                />
              ))}
            </div>

            <Panel title="Cluster">
              {data.nodes.length === 0 ? (
                <p className="px-5 py-8 text-sm text-[var(--muted)]">No nodes reported.</p>
              ) : (
                <table className="w-full text-sm">
                  <thead>
                    <tr className="text-left text-xs font-medium uppercase tracking-wider text-[var(--muted)]">
                      <th className="px-5 py-2.5">Node</th>
                      <th className="px-5 py-2.5">Uptime</th>
                    </tr>
                  </thead>
                  <tbody>
                    {data.nodes.map((node) => (
                      <tr key={node.address} className="border-t border-[var(--border)] hover:bg-[var(--row-hover)]">
                        <td className="px-5 py-3 font-mono text-[var(--text-soft)]">{node.address}</td>
                        <td className="px-5 py-3 text-[var(--muted)]">{relativeTime(node.startedAt)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </Panel>
          </div>
        );
      }}
    </QueryView>
  );
}

// toneText colors a stat's value by semantic tone.
const toneText: Record<Tone, string> = {
  zinc: "text-[var(--text)]",
  sky: "text-sky-700 dark:text-sky-300",
  violet: "text-violet-700 dark:text-violet-300",
  amber: "text-amber-700 dark:text-amber-300",
  orange: "text-orange-700 dark:text-orange-300",
  emerald: "text-emerald-700 dark:text-emerald-300",
  rose: "text-rose-600 dark:text-rose-300",
  indigo: "text-indigo-700 dark:text-indigo-300",
};

// Stat is a headline metric card; hint is shown as a hover tooltip.
function Stat({ label, value, tone = "zinc", hint }: { label: string; value: string; tone?: Tone; hint?: string }) {
  return (
    <div className="rounded-xl border border-[var(--border)] bg-[var(--surface)] p-4" title={hint}>
      <div className="text-xs font-medium uppercase tracking-wider text-[var(--muted)]">{label}</div>
      <div className={`mt-1 text-2xl font-semibold tabular-nums ${toneText[tone]}`}>{value}</div>
    </div>
  );
}
