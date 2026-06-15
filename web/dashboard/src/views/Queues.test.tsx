import { expect, test, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { createRouterTransport } from "@connectrpc/connect";
import { AdminService } from "../gen/conveyor/v1/service_pb.ts";
import { ApiProvider, createApi } from "../api/context.tsx";
import { ReadOnlyProvider } from "../api/readonly.tsx";
import { Queues } from "./Queues.tsx";

test("renders a queue row with its depths", async () => {
  const transport = createRouterTransport((router) => {
    router.service(AdminService, {
      listQueues: () => ({
        queues: [
          {
            name: "default",
            paused: false,
            scheduled: 0n,
            pending: 5n,
            active: 1n,
            retry: 0n,
            completed: 9n,
            archived: 0n,
          },
        ],
      }),
    });
  });

  render(
    <ApiProvider api={createApi(transport)}>
      <Queues />
    </ApiProvider>,
  );

  expect(await screen.findByText("default")).toBeInTheDocument();
  expect(screen.getByText("5")).toBeInTheDocument();
});

test("renders an empty state with no queues", async () => {
  const transport = createRouterTransport((router) => {
    router.service(AdminService, { listQueues: () => ({ queues: [] }) });
  });

  render(
    <ApiProvider api={createApi(transport)}>
      <Queues />
    </ApiProvider>,
  );

  expect(await screen.findByText("No queues yet.")).toBeInTheDocument();
});

test("pauses a queue", async () => {
  const pauseQueue = vi.fn().mockReturnValue({});
  const transport = createRouterTransport((router) => {
    router.service(AdminService, {
      listQueues: () => ({
        queues: [
          {
            name: "default",
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
      pauseQueue,
    });
  });

  render(
    <ApiProvider api={createApi(transport)}>
      <Queues />
    </ApiProvider>,
  );

  await userEvent.click(await screen.findByRole("button", { name: "Pause" }));

  expect(pauseQueue).toHaveBeenCalledOnce();
});

test("hides actions in read-only mode", async () => {
  const transport = createRouterTransport((router) => {
    router.service(AdminService, {
      listQueues: () => ({
        queues: [
          {
            name: "default",
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
      <ReadOnlyProvider value={true}>
        <Queues />
      </ReadOnlyProvider>
    </ApiProvider>,
  );

  await screen.findByText("default");
  expect(screen.queryByRole("button", { name: "Pause" })).toBeNull();
});
