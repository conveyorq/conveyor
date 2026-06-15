// DashboardConfig is the operator-supplied runtime configuration the SPA reads
// on load — settings that are not baked into the static bundle.
export interface DashboardConfig {
  grafanaUrl: string;
}

// loadDashboardConfig fetches /dashboard-config.json relative to the API base.
// Any failure yields an empty config, so the dashboard still works when the
// endpoint is absent (e.g. a separately hosted older server).
export async function loadDashboardConfig(baseUrl: string): Promise<DashboardConfig> {
  try {
    const response = await fetch(`${baseUrl}/dashboard-config.json`);
    if (!response.ok) {
      return { grafanaUrl: "" };
    }

    const data = (await response.json()) as Partial<DashboardConfig>;

    return { grafanaUrl: data.grafanaUrl ?? "" };
  } catch {
    return { grafanaUrl: "" };
  }
}
