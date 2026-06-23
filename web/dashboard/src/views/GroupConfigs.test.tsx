import { expect, test, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { createRouterTransport } from "@connectrpc/connect";
import { AdminService } from "../gen/conveyor/v1/service_pb.ts";
import { ApiProvider, createApi } from "../api/context.tsx";
import { ReadOnlyProvider } from "../api/readonly.tsx";
import { GroupConfigs } from "./GroupConfigs.tsx";

// override builds a GroupConfigInfo with the durations the view renders.
const override = {
  queue: "email",
  group: "welcome",
  maxSize: 20,
  maxDelay: { seconds: 120n, nanos: 0 },
  gracePeriod: { seconds: 5n, nanos: 0 },
};

test("renders a group override row", async () => {
  const transport = createRouterTransport((router) => {
    router.service(AdminService, {
      listGroupConfigs: () => ({ configs: [override] }),
    });
  });

  render(
    <ApiProvider api={createApi(transport)}>
      <GroupConfigs />
    </ApiProvider>,
  );

  expect(await screen.findByText("email")).toBeInTheDocument();
  expect(screen.getByText("welcome")).toBeInTheDocument();
  expect(screen.getByText("20")).toBeInTheDocument();
  expect(screen.getByText("2m")).toBeInTheDocument();
  expect(screen.getByText("5s")).toBeInTheDocument();
});

test("labels an empty group as the queue default", async () => {
  const transport = createRouterTransport((router) => {
    router.service(AdminService, {
      listGroupConfigs: () => ({
        configs: [{ ...override, group: "" }],
      }),
    });
  });

  render(
    <ApiProvider api={createApi(transport)}>
      <GroupConfigs />
    </ApiProvider>,
  );

  expect(await screen.findByText("(queue default)")).toBeInTheDocument();
});

test("renders an empty state with no overrides", async () => {
  const transport = createRouterTransport((router) => {
    router.service(AdminService, { listGroupConfigs: () => ({ configs: [] }) });
  });

  render(
    <ApiProvider api={createApi(transport)}>
      <GroupConfigs />
    </ApiProvider>,
  );

  expect(await screen.findByText(/No group overrides set/)).toBeInTheDocument();
});

test("saves a new override", async () => {
  const setGroupConfig = vi.fn().mockReturnValue({});
  const transport = createRouterTransport((router) => {
    router.service(AdminService, {
      listGroupConfigs: () => ({ configs: [] }),
      setGroupConfig,
    });
  });

  render(
    <ApiProvider api={createApi(transport)}>
      <GroupConfigs />
    </ApiProvider>,
  );

  await screen.findByText(/No group overrides set/);
  await userEvent.type(screen.getByPlaceholderText("email"), "reports");
  await userEvent.type(screen.getByPlaceholderText("welcome"), "daily");
  await userEvent.type(screen.getByPlaceholderText("100"), "50");
  await userEvent.type(screen.getByPlaceholderText("60"), "30");
  await userEvent.type(screen.getByPlaceholderText("10"), "5");
  await userEvent.click(screen.getByRole("button", { name: "Save override" }));

  expect(setGroupConfig).toHaveBeenCalledOnce();
  expect(setGroupConfig.mock.calls[0][0].queue).toBe("reports");
  expect(setGroupConfig.mock.calls[0][0].group).toBe("daily");
  expect(setGroupConfig.mock.calls[0][0].maxSize).toBe(50);
  expect(setGroupConfig.mock.calls[0][0].maxDelay.seconds).toBe(30n);
  expect(setGroupConfig.mock.calls[0][0].gracePeriod.seconds).toBe(5n);
});

test("removes an override", async () => {
  const deleteGroupConfig = vi.fn().mockReturnValue({});
  const transport = createRouterTransport((router) => {
    router.service(AdminService, {
      listGroupConfigs: () => ({ configs: [override] }),
      deleteGroupConfig,
    });
  });

  render(
    <ApiProvider api={createApi(transport)}>
      <GroupConfigs />
    </ApiProvider>,
  );

  // Remove is a confirm+danger button: the first click arms an inline Confirm.
  await userEvent.click(await screen.findByRole("button", { name: "Remove" }));
  await userEvent.click(screen.getByRole("button", { name: "Confirm" }));

  expect(deleteGroupConfig).toHaveBeenCalledOnce();
  expect(deleteGroupConfig.mock.calls[0][0].queue).toBe("email");
  expect(deleteGroupConfig.mock.calls[0][0].group).toBe("welcome");
});

test("hides the editor and actions in read-only mode", async () => {
  const transport = createRouterTransport((router) => {
    router.service(AdminService, {
      listGroupConfigs: () => ({ configs: [override] }),
    });
  });

  render(
    <ApiProvider api={createApi(transport)}>
      <ReadOnlyProvider value={true}>
        <GroupConfigs />
      </ReadOnlyProvider>
    </ApiProvider>,
  );

  await screen.findByText("email");
  expect(screen.queryByRole("button", { name: "Save override" })).toBeNull();
  expect(screen.queryByRole("button", { name: "Remove" })).toBeNull();
});
