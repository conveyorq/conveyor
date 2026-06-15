import { afterEach, expect, test, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { createRouterTransport } from "@connectrpc/connect";
import { AdminService } from "./gen/conveyor/v1/service_pb.ts";
import { ApiProvider, createApi } from "./api/context.tsx";
import { App } from "./App.tsx";

afterEach(() => {
  vi.unstubAllGlobals();
});

function renderApp(grafanaUrl = "") {
  vi.stubGlobal(
    "fetch",
    vi.fn().mockResolvedValue({ ok: true, json: () => Promise.resolve({ grafanaUrl }) }),
  );

  const transport = createRouterTransport((router) => {
    router.service(AdminService, {
      clusterInfo: () => ({ nodes: [] }),
      listQueues: () => ({
        queues: [
          {
            name: "emails",
            paused: false,
            scheduled: 0n,
            pending: 0n,
            active: 0n,
            retry: 0n,
            completed: 0n,
            archived: 0n,
          },
        ],
      }),
    });
  });

  render(
    <ApiProvider api={createApi(transport)}>
      <App />
    </ApiProvider>,
  );
}

test("renders the Conveyor heading", () => {
  renderApp();
  expect(screen.getByRole("heading", { name: "Conveyor" })).toBeInTheDocument();
});

test("switches to the Queues view when its tab is clicked", async () => {
  renderApp();

  await userEvent.click(screen.getByRole("button", { name: "Queues" }));

  expect(await screen.findByText("emails")).toBeInTheDocument();
});

test("toggles auto-refresh", async () => {
  renderApp();

  const toggle = screen.getByRole("button", { name: /Auto-refresh/ });
  expect(toggle).toHaveAttribute("aria-pressed", "false");

  await userEvent.click(toggle);
  expect(toggle).toHaveAttribute("aria-pressed", "true");
});

test("shows a Metrics link when a Grafana URL is configured", async () => {
  renderApp("https://grafana.example.com");

  const link = await screen.findByRole("link", { name: /Metrics/ });
  expect(link).toHaveAttribute("href", "https://grafana.example.com");
});
