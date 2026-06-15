import { useApi } from "../api/context.tsx";
import { useQuery } from "../api/useQuery.ts";
import { QueryView } from "../components/QueryView.tsx";
import { Panel } from "../components/Panel.tsx";
import { Badge } from "../components/Badge.tsx";

// metricLabel turns a snake_case metric key into a human label.
function metricLabel(key: string): string {
  return key.replace(/_/g, " ").replace(/^\w/, (c) => c.toUpperCase());
}

// Broker shows the storage engine backing the task log: its driver and runtime
// statistics (connection pool, row counts, server version). It is the analogue
// of a backing-store health page.
export function Broker() {
  const api = useApi();
  const query = useQuery(() => api.admin.brokerInfo({}), []);

  return (
    <Panel title="Broker">
      <QueryView query={query}>
        {(data) => {
          const keys = Object.keys(data.metrics).sort();

          return (
            <div className="px-5 py-4">
              <div className="mb-4 flex items-center gap-2 text-sm">
                <span className="text-[var(--muted)]">Driver</span>
                <Badge tone="indigo">{data.driver || "unknown"}</Badge>
              </div>

              {keys.length === 0 ? (
                <p className="text-sm text-[var(--muted)]">No engine statistics reported.</p>
              ) : (
                <dl className="grid grid-cols-[12rem_1fr] gap-y-1.5 text-sm">
                  {keys.map((key) => (
                    <div key={key} className="contents">
                      <dt className="text-[var(--muted)]">{metricLabel(key)}</dt>
                      <dd className="tabular-nums text-[var(--text-soft)]">{data.metrics[key]}</dd>
                    </div>
                  ))}
                </dl>
              )}
            </div>
          );
        }}
      </QueryView>
    </Panel>
  );
}
