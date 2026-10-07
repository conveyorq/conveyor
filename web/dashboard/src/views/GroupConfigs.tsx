import { useState } from "react";
import type { Duration } from "@bufbuild/protobuf/wkt";
import { useApi } from "../api/context.tsx";
import { useQuery } from "../api/useQuery.ts";
import { useAction } from "../api/useAction.ts";
import { useReadOnly } from "../api/readonly.tsx";
import { QueryView } from "../components/QueryView.tsx";
import { ConfirmButton } from "../components/ConfirmButton.tsx";
import { Panel } from "../components/Panel.tsx";
import { ActionAlert } from "../components/ActionAlert.tsx";
import type { GroupConfigInfo } from "../gen/conveyor/v1/service_pb.ts";

const inputClass =
  "w-full rounded-md border border-[var(--border)] bg-[var(--input-bg)] px-2 py-1 text-sm text-[var(--text)] placeholder:text-[var(--muted)] focus:border-indigo-500/60 focus:outline-none";

// emptyForm is the cleared override-editor state. Durations are entered as whole
// seconds.
const emptyForm = { queue: "", group: "", maxSize: "", maxDelay: "", grace: "" };

// queueDefaultLabel marks the empty-group row as the queue-wide default.
const queueDefaultLabel = "(queue default)";

// rowKeySeparator joins a queue and group into a collision-free React row key:
// neither name can contain a NUL.
const rowKeySeparator = "\u0000";

// durationSeconds converts a protobuf Duration to a whole-second number for the
// editor and table.
function durationSeconds(duration?: Duration): number {
  if (duration === undefined) {
    return 0;
  }

  return Number(duration.seconds) + duration.nanos / 1e9;
}

// durationLabel renders a Duration compactly: whole minutes as "Nm", otherwise
// seconds as "Ns".
function durationLabel(duration?: Duration): string {
  const seconds = durationSeconds(duration);
  if (seconds >= 60 && seconds % 60 === 0) {
    return `${seconds / 60}m`;
  }

  return `${seconds}s`;
}

// GroupConfigs manages per-group aggregation overrides: a group with an override
// fires on its own size, max-delay, and grace thresholds instead of the server's
// global defaults. An empty group is the queue-wide default applied to every
// group on the queue without its own override. Removing an override reverts the
// group to the queue-wide or global default. The editor and actions are hidden
// in read-only mode.
export function GroupConfigs() {
  const api = useApi();
  const query = useQuery(() => api.admin.listGroupConfigs({}), []);
  const action = useAction(query.reload);
  const readOnly = useReadOnly();
  const [form, setForm] = useState(emptyForm);

  // loadConfig populates the editor from an existing override for editing.
  function loadConfig(config: GroupConfigInfo) {
    setForm({
      queue: config.queue,
      group: config.group,
      maxSize: String(config.maxSize),
      maxDelay: String(durationSeconds(config.maxDelay)),
      grace: String(durationSeconds(config.gracePeriod)),
    });
  }

  function save() {
    const queue = form.queue.trim();
    const group = form.group.trim();
    const maxSize = Number(form.maxSize);
    const maxDelay = Number(form.maxDelay);
    const grace = Number(form.grace);

    // Validate before the round-trip so a bad value gives an immediate message
    // instead of a server invalid-argument error.
    if (queue === "" || !Number.isInteger(maxSize) || maxSize < 1 || !(maxDelay > 0) || !(grace > 0)) {
      return action.run(() =>
        Promise.reject(
          new Error("Queue is required; max size must be an integer ≥ 1; max delay and grace must be positive seconds."),
        ),
      );
    }

    return action
      .run(() =>
        api.admin.setGroupConfig({
          queue,
          group,
          maxSize,
          maxDelay: { seconds: BigInt(Math.trunc(maxDelay)) },
          gracePeriod: { seconds: BigInt(Math.trunc(grace)) },
        }),
      )
      .then(() => setForm(emptyForm));
  }

  return (
    <div className="space-y-4">
      <ActionAlert message={action.error} onDismiss={action.dismiss} />

      {!readOnly && (
        <Panel title="Group-config editor">
          <div className="grid grid-cols-2 gap-3 px-5 py-4">
            <label className="text-xs text-[var(--muted)]">
              Queue
              <input className={inputClass} value={form.queue} onChange={(e) => setForm({ ...form, queue: e.target.value })} placeholder="email" />
            </label>
            <label className="text-xs text-[var(--muted)]">
              Group <span className="text-[var(--muted)]">(blank = queue default)</span>
              <input className={inputClass} value={form.group} onChange={(e) => setForm({ ...form, group: e.target.value })} placeholder="welcome" />
            </label>
            <label className="text-xs text-[var(--muted)]">
              Max size
              <input className={inputClass} type="number" min="1" step="1" value={form.maxSize} onChange={(e) => setForm({ ...form, maxSize: e.target.value })} placeholder="100" />
            </label>
            <label className="text-xs text-[var(--muted)]">
              Max delay (seconds)
              <input className={inputClass} type="number" min="1" step="1" value={form.maxDelay} onChange={(e) => setForm({ ...form, maxDelay: e.target.value })} placeholder="60" />
            </label>
            <label className="text-xs text-[var(--muted)]">
              Grace period (seconds)
              <input className={inputClass} type="number" min="1" step="1" value={form.grace} onChange={(e) => setForm({ ...form, grace: e.target.value })} placeholder="10" />
            </label>
            <div className="col-span-2 flex gap-2">
              <ConfirmButton label="Save override" onConfirm={save} />
              <button type="button" onClick={() => setForm(emptyForm)} className="rounded px-2 py-0.5 text-xs text-[var(--muted)] hover:text-[var(--text-soft)]">
                Clear
              </button>
            </div>
          </div>
        </Panel>
      )}

      <Panel title="Group configs">
        <QueryView query={query}>
          {(data) =>
            data.configs.length === 0 ? (
              <p className="px-5 py-8 text-sm text-[var(--muted)]">No group overrides set. Groups fire on the global defaults.</p>
            ) : (
              <table className="w-full text-sm">
                <thead>
                  <tr className="text-left text-xs font-medium uppercase tracking-wider text-[var(--muted)]">
                    <th className="px-5 py-2.5">Queue</th>
                    <th className="px-5 py-2.5">Group</th>
                    <th className="px-5 py-2.5 text-right">Max size</th>
                    <th className="px-5 py-2.5 text-right">Max delay</th>
                    <th className="px-5 py-2.5 text-right">Grace</th>
                    {!readOnly && <th className="px-5 py-2.5 text-right">Actions</th>}
                  </tr>
                </thead>
                <tbody>
                  {data.configs.map((config) => (
                    <tr key={`${config.queue}${rowKeySeparator}${config.group}`} className="border-t border-[var(--border)] hover:bg-[var(--row-hover)]">
                      <td className="px-5 py-3 font-medium text-[var(--text)]">{config.queue}</td>
                      <td className="px-5 py-3 text-[var(--text-soft)]">{config.group === "" ? <span className="text-[var(--muted)]">{queueDefaultLabel}</span> : config.group}</td>
                      <td className="px-5 py-3 text-right tabular-nums text-[var(--text-soft)]">{config.maxSize}</td>
                      <td className="px-5 py-3 text-right tabular-nums text-[var(--text-soft)]">{durationLabel(config.maxDelay)}</td>
                      <td className="px-5 py-3 text-right tabular-nums text-[var(--text-soft)]">{durationLabel(config.gracePeriod)}</td>
                      {!readOnly && (
                        <td className="px-5 py-3 text-right">
                          <span className="inline-flex gap-2">
                            <ConfirmButton label="Edit" onConfirm={async () => loadConfig(config)} />
                            <ConfirmButton label="Remove" confirm danger onConfirm={() => action.run(() => api.admin.deleteGroupConfig({ queue: config.queue, group: config.group }))} />
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
