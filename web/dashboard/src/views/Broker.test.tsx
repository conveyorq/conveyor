import { expect, test } from "vitest";
import { render, screen } from "@testing-library/react";
import { createRouterTransport } from "@connectrpc/connect";
import { AdminService } from "../gen/conveyor/v1/service_pb.ts";
import { ApiProvider, createApi } from "../api/context.tsx";
import { Broker } from "./Broker.tsx";

test("shows the driver and engine metrics", async () => {
  const transport = createRouterTransport((router) => {
    router.service(AdminService, {
      brokerInfo: () => ({ driver: "postgres", metrics: { tasks: "42", pool_idle_conns: "3" } }),
    });
  });

  render(
    <ApiProvider api={createApi(transport)}>
      <Broker />
    </ApiProvider>,
  );

  expect(await screen.findByText("postgres")).toBeInTheDocument();
  expect(screen.getByText("Tasks")).toBeInTheDocument();
  expect(screen.getByText("42")).toBeInTheDocument();
  expect(screen.getByText("Pool idle conns")).toBeInTheDocument();
});
