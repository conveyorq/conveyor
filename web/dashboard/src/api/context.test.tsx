import type { ReactNode } from "react";
import { expect, test } from "vitest";
import { renderHook } from "@testing-library/react";
import { createRouterTransport } from "@connectrpc/connect";
import { AdminService } from "../gen/conveyor/v1/service_pb.ts";
import { ApiProvider, createApi, useApi } from "./context.tsx";

test("useApi throws outside a provider", () => {
  expect(() => renderHook(() => useApi())).toThrow(/ApiProvider/);
});

test("useApi returns the clients within a provider", () => {
  const transport = createRouterTransport((router) => {
    router.service(AdminService, { listQueues: () => ({ queues: [] }) });
  });
  const api = createApi(transport);

  const wrapper = ({ children }: { children: ReactNode }) => (
    <ApiProvider api={api}>{children}</ApiProvider>
  );

  const { result } = renderHook(() => useApi(), { wrapper });

  expect(result.current.admin).toBeDefined();
  expect(result.current.tasks).toBeDefined();
});
