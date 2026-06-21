// DashboardConfig is the operator-supplied runtime configuration the SPA reads
// on load — settings that are not baked into the static bundle.
export interface DashboardConfig {
  grafanaUrl: string;
  // readOnly mirrors the server's admin read-only mode; the SPA hides its
  // action controls when set (the server enforces it regardless).
  readOnly: boolean;
}

// emptyConfig is the safe fallback when the endpoint is absent or malformed.
const emptyConfig: DashboardConfig = { grafanaUrl: "", readOnly: false };

// safeHttpUrl returns value only when it is a syntactically valid http(s) URL,
// otherwise "". It rejects non-strings and dangerous schemes (e.g. javascript:)
// so the value can be used directly in an anchor href.
function safeHttpUrl(value: unknown): string {
  if (typeof value !== "string" || value === "") {
    return "";
  }

  try {
    const url = new URL(value);

    return url.protocol === "https:" || url.protocol === "http:" ? value : "";
  } catch {
    return "";
  }
}

// loadDashboardConfig fetches /dashboard-config.json relative to the API base.
// Any failure yields an empty config, so the dashboard still works when the
// endpoint is absent (e.g. a separately hosted older server). Field types are
// validated rather than trusted, so a malformed response cannot inject a bad
// Grafana href or a non-boolean read-only flag.
export async function loadDashboardConfig(baseUrl: string): Promise<DashboardConfig> {
  try {
    const response = await fetch(`${baseUrl}/dashboard-config.json`);
    if (!response.ok) {
      return emptyConfig;
    }

    const data = (await response.json()) as Partial<DashboardConfig>;

    return { grafanaUrl: safeHttpUrl(data.grafanaUrl), readOnly: data.readOnly === true };
  } catch {
    return emptyConfig;
  }
}
