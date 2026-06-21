import { useEffect, useState } from "react";
import { Overview } from "./views/Overview.tsx";
import { Queues } from "./views/Queues.tsx";
import { Limits } from "./views/Limits.tsx";
import { ConcurrencyLimits } from "./views/ConcurrencyLimits.tsx";
import { Tasks } from "./views/Tasks.tsx";
import { Cron } from "./views/Cron.tsx";
import { Workers } from "./views/Workers.tsx";
import { Metrics } from "./views/Metrics.tsx";
import { Broker } from "./views/Broker.tsx";
import { getToken, setToken } from "./api/token.ts";
import { RefreshTickContext } from "./api/refresh.ts";
import { ReadOnlyProvider } from "./api/readonly.tsx";
import { loadDashboardConfig } from "./api/config.ts";
import { apiBaseUrl } from "./api/transport.ts";
import { applyTheme, getTheme, setTheme, type Theme } from "./lib/theme.ts";
import { Badge } from "./components/Badge.tsx";
import { ErrorBoundary } from "./components/ErrorBoundary.tsx";
import {
  IconBroker,
  IconConcurrency,
  IconCron,
  IconExternal,
  IconLimits,
  IconLogo,
  IconMetrics,
  IconMoon,
  IconOverview,
  IconQueues,
  IconSun,
  IconTasks,
  IconWorkers,
} from "./components/icons.tsx";

// refreshIntervalMs is how often views re-fetch when auto-refresh is on.
const refreshIntervalMs = 2000;

// tabs are the navigable views, in display order.
const tabs = [
  { id: "overview", label: "Overview", icon: <IconOverview />, render: () => <Overview /> },
  { id: "queues", label: "Queues", icon: <IconQueues />, render: () => <Queues /> },
  { id: "limits", label: "Limits", icon: <IconLimits />, render: () => <Limits /> },
  { id: "concurrency", label: "Concurrency", icon: <IconConcurrency />, render: () => <ConcurrencyLimits /> },
  { id: "tasks", label: "Tasks", icon: <IconTasks />, render: () => <Tasks /> },
  { id: "cron", label: "Cron", icon: <IconCron />, render: () => <Cron /> },
  { id: "workers", label: "Workers", icon: <IconWorkers />, render: () => <Workers /> },
  { id: "metrics", label: "Metrics", icon: <IconMetrics />, render: () => <Metrics /> },
  { id: "broker", label: "Broker", icon: <IconBroker />, render: () => <Broker /> },
] as const;

type TabId = (typeof tabs)[number]["id"];

// ghostButton styles the secondary top-bar controls (theme, metrics).
const ghostButton =
  "inline-flex items-center gap-1.5 rounded-lg px-3 py-1.5 text-sm text-[var(--muted)] hover:bg-[var(--row-hover)] hover:text-[var(--text)]";

// App is the dashboard shell: a sidebar of view tabs, a top bar with the
// theme and auto-refresh toggles, an optional Grafana link, and the API-token
// field, and the active view below.
export function App() {
  const [active, setActive] = useState<TabId>("overview");
  const [token, setTokenValue] = useState(getToken);
  const [auto, setAuto] = useState(false);
  const [tick, setTick] = useState(0);
  const [grafanaUrl, setGrafanaUrl] = useState("");
  const [readOnly, setReadOnly] = useState(false);
  const [theme, setThemeValue] = useState<Theme>(getTheme);

  useEffect(() => {
    applyTheme(theme);
  }, [theme]);

  useEffect(() => {
    if (!auto) {
      return;
    }

    const id = setInterval(() => setTick((value) => value + 1), refreshIntervalMs);

    return () => clearInterval(id);
  }, [auto]);

  useEffect(() => {
    void loadDashboardConfig(apiBaseUrl()).then((config) => {
      setGrafanaUrl(config.grafanaUrl);
      setReadOnly(config.readOnly);
    });
  }, []);

  const current = tabs.find((tab) => tab.id === active) ?? tabs[0];

  function toggleTheme() {
    setThemeValue((value) => {
      const next: Theme = value === "dark" ? "light" : "dark";
      setTheme(next);

      return next;
    });
  }

  return (
    <div className="flex h-screen bg-[var(--bg)] text-[var(--text)]">
      <aside className="flex w-60 shrink-0 flex-col border-r border-[var(--border)] bg-[var(--surface)]">
        <div className="flex items-center gap-2.5 px-5 py-4">
          <span className="text-indigo-500 dark:text-indigo-400">
            <IconLogo width="22" height="22" />
          </span>
          <h1 className="text-lg font-semibold tracking-tight">Conveyor</h1>
        </div>

        <nav className="flex flex-col gap-1 px-3 py-2" aria-label="Views">
          {tabs.map((tab) => {
            const selected = tab.id === active;

            return (
              <button
                key={tab.id}
                type="button"
                aria-current={selected ? "page" : undefined}
                onClick={() => setActive(tab.id)}
                className={
                  "flex items-center gap-3 rounded-lg px-3 py-2 text-sm font-medium transition-colors " +
                  (selected
                    ? "bg-indigo-50 text-indigo-700 ring-1 ring-inset ring-indigo-600/20 dark:bg-indigo-500/15 dark:text-indigo-200 dark:ring-indigo-500/30"
                    : "text-[var(--muted)] hover:bg-[var(--row-hover)] hover:text-[var(--text)]")
                }
              >
                <span className={selected ? "text-indigo-600 dark:text-indigo-300" : "text-[var(--muted)]"}>
                  {tab.icon}
                </span>
                {tab.label}
              </button>
            );
          })}
        </nav>

        <div className="mt-auto px-5 py-4 text-xs text-[var(--muted)]">Operations console</div>
      </aside>

      <div className="flex min-w-0 flex-1 flex-col">
        <header className="flex h-16 shrink-0 items-center gap-4 border-b border-[var(--border)] px-6">
          <h2 className="text-base font-semibold">{current.label}</h2>
          {readOnly && <Badge tone="amber">Read-only</Badge>}

          <div className="ml-auto flex items-center gap-3">
            <button
              type="button"
              aria-label="Toggle theme"
              onClick={toggleTheme}
              className={ghostButton}
            >
              {theme === "dark" ? <IconSun width="16" height="16" /> : <IconMoon width="16" height="16" />}
            </button>

            {grafanaUrl !== "" && (
              <a href={grafanaUrl} target="_blank" rel="noreferrer" className={ghostButton}>
                Metrics
                <IconExternal width="14" height="14" />
              </a>
            )}

            <button
              type="button"
              aria-pressed={auto}
              onClick={() => setAuto((value) => !value)}
              className={
                "inline-flex items-center gap-2 rounded-lg px-3 py-1.5 text-sm font-medium transition-colors " +
                (auto
                  ? "bg-emerald-50 text-emerald-700 ring-1 ring-inset ring-emerald-600/20 dark:bg-emerald-500/15 dark:text-emerald-300 dark:ring-emerald-500/30"
                  : "text-[var(--muted)] hover:bg-[var(--row-hover)] hover:text-[var(--text)]")
              }
            >
              <span className={"size-1.5 rounded-full " + (auto ? "bg-emerald-500 dark:bg-emerald-400" : "bg-[var(--muted)]")} />
              Auto-refresh {auto ? "on" : "off"}
            </button>

            <input
              aria-label="API token"
              type="password"
              placeholder="API token"
              value={token}
              onChange={(event) => {
                setTokenValue(event.target.value);
                setToken(event.target.value);
              }}
              className="w-40 rounded-lg border border-[var(--border)] bg-[var(--input-bg)] px-3 py-1.5 text-sm placeholder:text-[var(--muted)] focus:border-indigo-500/60 focus:outline-none"
            />
          </div>
        </header>

        <RefreshTickContext.Provider value={tick}>
          <ReadOnlyProvider value={readOnly}>
            <main className="flex-1 overflow-auto p-6">
              <ErrorBoundary key={active}>{current.render()}</ErrorBoundary>
            </main>
          </ReadOnlyProvider>
        </RefreshTickContext.Provider>
      </div>
    </div>
  );
}
