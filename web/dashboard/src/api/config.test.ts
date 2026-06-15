import { afterEach, expect, test, vi } from "vitest";
import { loadDashboardConfig } from "./config.ts";

afterEach(() => {
  vi.unstubAllGlobals();
});

test("returns the grafana URL and read-only flag from the endpoint", async () => {
  vi.stubGlobal(
    "fetch",
    vi.fn().mockResolvedValue({ ok: true, json: () => Promise.resolve({ grafanaUrl: "https://g", readOnly: true }) }),
  );

  expect(await loadDashboardConfig("")).toEqual({ grafanaUrl: "https://g", readOnly: true });
});

test("defaults read-only off when the field is absent", async () => {
  vi.stubGlobal(
    "fetch",
    vi.fn().mockResolvedValue({ ok: true, json: () => Promise.resolve({ grafanaUrl: "https://g" }) }),
  );

  expect(await loadDashboardConfig("")).toEqual({ grafanaUrl: "https://g", readOnly: false });
});

test("returns empty config on a non-ok response", async () => {
  vi.stubGlobal("fetch", vi.fn().mockResolvedValue({ ok: false }));
  expect(await loadDashboardConfig("")).toEqual({ grafanaUrl: "", readOnly: false });
});

test("returns empty config when the fetch throws", async () => {
  vi.stubGlobal("fetch", vi.fn().mockRejectedValue(new Error("offline")));
  expect(await loadDashboardConfig("")).toEqual({ grafanaUrl: "", readOnly: false });
});
