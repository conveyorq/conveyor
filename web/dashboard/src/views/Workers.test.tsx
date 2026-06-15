import { expect, test } from "vitest";
import { render, screen } from "@testing-library/react";
import { createRouterTransport } from "@connectrpc/connect";
import { timestampFromDate } from "@bufbuild/protobuf/wkt";
import { AdminService } from "../gen/conveyor/v1/service_pb.ts";
import { ApiProvider, createApi } from "../api/context.tsx";
import { Workers } from "./Workers.tsx";

test("renders connected worker sessions", async () => {
  const transport = createRouterTransport((router) => {
    router.service(AdminService, {
      listWorkerSessions: () => ({
        sessions: [
          {
            id: "sess-1",
            queues: ["default", "critical"],
            concurrency: 8,
            sdkVersion: "v1.2.3",
            connectedAt: timestampFromDate(new Date("2026-06-15T12:00:00Z")),
          },
        ],
      }),
    });
  });

  render(
    <ApiProvider api={createApi(transport)}>
      <Workers />
    </ApiProvider>,
  );

  expect(await screen.findByText("sess-1")).toBeInTheDocument();
  expect(screen.getByText("default, critical")).toBeInTheDocument();
});

test("renders an empty state with no workers", async () => {
  const transport = createRouterTransport((router) => {
    router.service(AdminService, { listWorkerSessions: () => ({ sessions: [] }) });
  });

  render(
    <ApiProvider api={createApi(transport)}>
      <Workers />
    </ApiProvider>,
  );

  expect(await screen.findByText("No workers connected to this node.")).toBeInTheDocument();
});
