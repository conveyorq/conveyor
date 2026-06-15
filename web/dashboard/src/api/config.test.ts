import { afterEach, expect, test, vi } from "vitest";
import { loadDashboardConfig } from "./config.ts";

afterEach(() => {
  vi.unstubAllGlobals();
});

test("returns the grafana URL from the endpoint", async () => {
  vi.stubGlobal(
    "fetch",
    vi.fn().mockResolvedValue({ ok: true, json: () => Promise.resolve({ grafanaUrl: "https://g" }) }),
  );

  expect(await loadDashboardConfig("")).toEqual({ grafanaUrl: "https://g" });
});

test("returns empty config on a non-ok response", async () => {
  vi.stubGlobal("fetch", vi.fn().mockResolvedValue({ ok: false }));
  expect(await loadDashboardConfig("")).toEqual({ grafanaUrl: "" });
});

test("returns empty config when the fetch throws", async () => {
  vi.stubGlobal("fetch", vi.fn().mockRejectedValue(new Error("offline")));
  expect(await loadDashboardConfig("")).toEqual({ grafanaUrl: "" });
});
