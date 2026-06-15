import { useEffect, useState } from "react";
import { useApi } from "../api/context.tsx";
import { useRefreshTick } from "../api/refresh.ts";
import { Panel } from "../components/Panel.tsx";
import { LineChart, type Series } from "../components/LineChart.tsx";
import { history, recordSample, type QueueSample } from "../lib/history.ts";

// Metrics renders native time-series charts of the cluster-wide task totals.
// It samples ListQueues on every refresh tick and keeps a rolling session
// history, so charts populate without an external metrics store. Enabling
// auto-refresh fills the window over time.
export function Metrics() {
  const api = useApi();
  const tick = useRefreshTick();
  const [samples, setSamples] = useState<QueueSample[]>(history);

  useEffect(() => {
    let active = true;

    api.admin
      .listQueues({})
      .then((resp) => {
        if (!active) {
          return;
        }

        const sample: QueueSample = {
          time: Date.now(),
          scheduled: 0,
          pending: 0,
          active: 0,
          retry: 0,
          completed: 0,
          archived: 0,
        };

        for (const queue of resp.queues) {
          sample.scheduled += Number(queue.scheduled);
          sample.pending += Number(queue.pending);
          sample.active += Number(queue.active);
          sample.retry += Number(queue.retry);
          sample.completed += Number(queue.completed);
          sample.archived += Number(queue.archived);
        }

        setSamples(recordSample(sample));
      })
      .catch(() => {
        // A failed poll just skips this sample; the next tick retries.
      });

    return () => {
      active = false;
    };
  }, [api, tick]);

  const backlog: Series[] = [
    { label: "Pending", color: "#0ea5e9", values: samples.map((s) => s.pending) },
    { label: "Active", color: "#f59e0b", values: samples.map((s) => s.active) },
    { label: "Scheduled", color: "#8b5cf6", values: samples.map((s) => s.scheduled) },
    { label: "Retry", color: "#f97316", values: samples.map((s) => s.retry) },
  ];

  const outcomes: Series[] = [
    { label: "Completed", color: "#10b981", values: samples.map((s) => s.completed) },
    { label: "Archived", color: "#f43f5e", values: samples.map((s) => s.archived) },
  ];

  return (
    <div className="space-y-4">
      {samples.length < 2 && (
        <p className="rounded-lg border border-[var(--border)] bg-[var(--surface)] px-4 py-2.5 text-sm text-[var(--muted)]">
          Charts build from live samples. Turn on auto-refresh to populate the window.
        </p>
      )}

      <Panel title="Backlog over time">
        <div className="px-5 py-4">
          <LineChart series={backlog} ariaLabel="Backlog over time" />
        </div>
      </Panel>

      <Panel title="Outcomes over time">
        <div className="px-5 py-4">
          <LineChart series={outcomes} ariaLabel="Outcomes over time" />
        </div>
      </Panel>
    </div>
  );
}
