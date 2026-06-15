import { expect, test, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { createRouterTransport } from "@connectrpc/connect";
import { AdminService } from "../gen/conveyor/v1/service_pb.ts";
import { ApiProvider, createApi } from "../api/context.tsx";
import { Cron } from "./Cron.tsx";

test("renders cron entries", async () => {
  const transport = createRouterTransport((router) => {
    router.service(AdminService, {
      listCron: () => ({
        entries: [
          { id: "nightly", spec: "0 0 0 * * *", taskType: "report:daily", queue: "default", paused: false },
        ],
      }),
    });
  });

  render(
    <ApiProvider api={createApi(transport)}>
      <Cron />
    </ApiProvider>,
  );

  expect(await screen.findByText("nightly")).toBeInTheDocument();
  expect(screen.getByText("report:daily")).toBeInTheDocument();
});

test("renders an empty state with no entries", async () => {
  const transport = createRouterTransport((router) => {
    router.service(AdminService, { listCron: () => ({ entries: [] }) });
  });

  render(
    <ApiProvider api={createApi(transport)}>
      <Cron />
    </ApiProvider>,
  );

  expect(await screen.findByText("No cron entries.")).toBeInTheDocument();
});

test("deletes a cron entry after confirmation", async () => {
  const deleteCron = vi.fn().mockReturnValue({});
  const transport = createRouterTransport((router) => {
    router.service(AdminService, {
      listCron: () => ({
        entries: [{ id: "nightly", spec: "0 0 0 * * *", taskType: "report:daily", queue: "default", paused: false }],
      }),
      deleteCron,
    });
  });

  render(
    <ApiProvider api={createApi(transport)}>
      <Cron />
    </ApiProvider>,
  );

  await userEvent.click(await screen.findByRole("button", { name: "Delete" }));
  expect(deleteCron).not.toHaveBeenCalled();

  await userEvent.click(screen.getByRole("button", { name: "Confirm" }));
  expect(deleteCron).toHaveBeenCalledOnce();
});
