import { expect, test, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { createRouterTransport } from "@connectrpc/connect";
import { AdminService } from "../gen/conveyor/v1/service_pb.ts";
import { ApiProvider, createApi } from "../api/context.tsx";
import { ReadOnlyProvider } from "../api/readonly.tsx";
import { Limits } from "./Limits.tsx";

test("renders a rate-limit row", async () => {
  const transport = createRouterTransport((router) => {
    router.service(AdminService, {
      listRateLimits: () => ({ limits: [{ queue: "email", ratePerSec: 50, burst: 10 }] }),
    });
  });

  render(
    <ApiProvider api={createApi(transport)}>
      <Limits />
    </ApiProvider>,
  );

  expect(await screen.findByText("email")).toBeInTheDocument();
  expect(screen.getByText("50")).toBeInTheDocument();
  expect(screen.getByText("10")).toBeInTheDocument();
});

test("renders an empty state with no limits", async () => {
  const transport = createRouterTransport((router) => {
    router.service(AdminService, { listRateLimits: () => ({ limits: [] }) });
  });

  render(
    <ApiProvider api={createApi(transport)}>
      <Limits />
    </ApiProvider>,
  );

  expect(await screen.findByText(/No rate limits set/)).toBeInTheDocument();
});

test("saves a new limit", async () => {
  const setQueueRateLimit = vi.fn().mockReturnValue({});
  const transport = createRouterTransport((router) => {
    router.service(AdminService, {
      listRateLimits: () => ({ limits: [] }),
      setQueueRateLimit,
    });
  });

  render(
    <ApiProvider api={createApi(transport)}>
      <Limits />
    </ApiProvider>,
  );

  await screen.findByText(/No rate limits set/);
  await userEvent.type(screen.getByPlaceholderText("email"), "reports");
  await userEvent.type(screen.getByPlaceholderText("50"), "25");
  await userEvent.click(screen.getByRole("button", { name: "Save limit" }));

  expect(setQueueRateLimit).toHaveBeenCalledOnce();
});

test("removes a limit", async () => {
  const deleteQueueRateLimit = vi.fn().mockReturnValue({});
  const transport = createRouterTransport((router) => {
    router.service(AdminService, {
      listRateLimits: () => ({ limits: [{ queue: "email", ratePerSec: 50, burst: 10 }] }),
      deleteQueueRateLimit,
    });
  });

  render(
    <ApiProvider api={createApi(transport)}>
      <Limits />
    </ApiProvider>,
  );

  // Remove is a confirm+danger button: the first click arms an inline Confirm.
  await userEvent.click(await screen.findByRole("button", { name: "Remove" }));
  await userEvent.click(screen.getByRole("button", { name: "Confirm" }));

  expect(deleteQueueRateLimit).toHaveBeenCalledOnce();
});

test("hides the editor and actions in read-only mode", async () => {
  const transport = createRouterTransport((router) => {
    router.service(AdminService, {
      listRateLimits: () => ({ limits: [{ queue: "email", ratePerSec: 50, burst: 10 }] }),
    });
  });

  render(
    <ApiProvider api={createApi(transport)}>
      <ReadOnlyProvider value={true}>
        <Limits />
      </ReadOnlyProvider>
    </ApiProvider>,
  );

  await screen.findByText("email");
  expect(screen.queryByRole("button", { name: "Save limit" })).toBeNull();
  expect(screen.queryByRole("button", { name: "Remove" })).toBeNull();
});
