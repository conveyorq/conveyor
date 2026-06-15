import { expect, test } from "vitest";
import { render, screen } from "@testing-library/react";
import { createRouterTransport } from "@connectrpc/connect";
import { timestampFromDate } from "@bufbuild/protobuf/wkt";
import { AdminService } from "../gen/conveyor/v1/service_pb.ts";
import { ApiProvider, createApi } from "../api/context.tsx";
import { Overview } from "./Overview.tsx";

// overviewTransport stubs the three reads the Overview aggregates.
function overviewTransport(nodes: { address: string }[]) {
  return createRouterTransport((router) => {
    router.service(AdminService, {
      clusterInfo: () => ({ nodes: nodes.map((n) => ({ address: n.address, startedAt: timestampFromDate(new Date()) })) }),
      listQueues: () => ({
        queues: [
          { name: "default", paused: false, scheduled: 0n, pending: 5n, active: 1n, retry: 0n, completed: 9n, archived: 0n },
        ],
      }),
      listWorkerSessions: () => ({ sessions: [] }),
    });
  });
}

test("renders cluster nodes and aggregate stats", async () => {
  render(
    <ApiProvider api={createApi(overviewTransport([{ address: "10.0.0.1:9002" }]))}>
      <Overview />
    </ApiProvider>,
  );

  expect(await screen.findByText("10.0.0.1:9002")).toBeInTheDocument();
  expect(screen.getByText("Tasks Pending")).toBeInTheDocument();
});

test("renders an empty state with no nodes", async () => {
  render(
    <ApiProvider api={createApi(overviewTransport([]))}>
      <Overview />
    </ApiProvider>,
  );

  expect(await screen.findByText("No nodes reported.")).toBeInTheDocument();
});
