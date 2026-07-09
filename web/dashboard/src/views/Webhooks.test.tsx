import { expect, test, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { createRouterTransport } from "@connectrpc/connect";
import { AdminService } from "../gen/conveyor/v1/service_pb.ts";
import { ApiProvider, createApi } from "../api/context.tsx";
import { Webhooks } from "./Webhooks.tsx";

test("renders webhook workers with redacted secrets", async () => {
  const transport = createRouterTransport((router) => {
    router.service(AdminService, {
      listWebhookWorkers: () => ({
        workers: [
          {
            name: "billing-hooks",
            url: "https://hooks.example.com/tasks",
            queues: { billing: 3 },
            concurrency: 8,
            paused: false,
          },
        ],
      }),
    });
  });

  render(
    <ApiProvider api={createApi(transport)}>
      <Webhooks />
    </ApiProvider>,
  );

  expect(await screen.findByText("billing-hooks")).toBeInTheDocument();
  expect(screen.getByText("https://hooks.example.com/tasks")).toBeInTheDocument();
  expect(screen.getByText("billing=3")).toBeInTheDocument();
  expect(screen.getByText("active")).toBeInTheDocument();
});

test("renders an empty state with no registrations", async () => {
  const transport = createRouterTransport((router) => {
    router.service(AdminService, { listWebhookWorkers: () => ({ workers: [] }) });
  });

  render(
    <ApiProvider api={createApi(transport)}>
      <Webhooks />
    </ApiProvider>,
  );

  expect(await screen.findByText("No webhook workers.")).toBeInTheDocument();
});

test("creates a registration from the editor form", async () => {
  const upsertWebhookWorker = vi.fn().mockReturnValue({});
  const transport = createRouterTransport((router) => {
    router.service(AdminService, {
      listWebhookWorkers: () => ({ workers: [] }),
      upsertWebhookWorker,
    });
  });

  render(
    <ApiProvider api={createApi(transport)}>
      <Webhooks />
    </ApiProvider>,
  );

  await screen.findByText("No webhook workers.");
  await userEvent.type(screen.getByPlaceholderText("billing-hooks"), "hooks");
  await userEvent.type(screen.getByPlaceholderText("https://hooks.example.com/tasks"), "https://h.example.com/t");
  await userEvent.type(screen.getByPlaceholderText("billing=3, default=1"), "billing=3, default");
  await userEvent.type(screen.getByPlaceholderText("signing secret"), "s3cret");
  await userEvent.click(screen.getByRole("button", { name: "Save registration" }));

  // The argument is a protobuf message with a cyclic descriptor; assert on
  // fields rather than deep-comparing the message.
  expect(upsertWebhookWorker).toHaveBeenCalledOnce();

  const worker = upsertWebhookWorker.mock.calls[0][0].worker;
  expect(worker.name).toBe("hooks");
  expect(worker.queues).toEqual({ billing: 3, default: 1 });
  expect(worker.secrets).toEqual(["s3cret"]);
});

test("pauses and deletes a registration after confirmation", async () => {
  const pauseWebhookWorker = vi.fn().mockReturnValue({});
  const deleteWebhookWorker = vi.fn().mockReturnValue({});
  const transport = createRouterTransport((router) => {
    router.service(AdminService, {
      listWebhookWorkers: () => ({
        workers: [{ name: "hooks", url: "https://h.example.com/t", queues: { default: 1 }, concurrency: 4, paused: false }],
      }),
      pauseWebhookWorker,
      deleteWebhookWorker,
    });
  });

  render(
    <ApiProvider api={createApi(transport)}>
      <Webhooks />
    </ApiProvider>,
  );

  await userEvent.click(await screen.findByRole("button", { name: "Pause" }));
  expect(pauseWebhookWorker).toHaveBeenCalledOnce();
  expect(pauseWebhookWorker.mock.calls[0][0].name).toBe("hooks");

  // Deletion is destructive and requires the confirm step.
  await userEvent.click(await screen.findByRole("button", { name: "Delete" }));
  expect(deleteWebhookWorker).not.toHaveBeenCalled();

  await userEvent.click(await screen.findByRole("button", { name: "Confirm" }));
  expect(deleteWebhookWorker).toHaveBeenCalledOnce();
  expect(deleteWebhookWorker.mock.calls[0][0].name).toBe("hooks");
});
