import { expect, test, vi } from "vitest";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { createRouterTransport } from "@connectrpc/connect";
import { create } from "@bufbuild/protobuf";
import { AdminService, TaskInfoSchema } from "../gen/conveyor/v1/service_pb.ts";
import { TaskState } from "../gen/conveyor/v1/task_pb.ts";
import { ApiProvider, createApi } from "../api/context.tsx";
import { Tasks } from "./Tasks.tsx";

function task(id: string, state: TaskState) {
  return create(TaskInfoSchema, { id, type: "email:welcome", queue: "default", state });
}

test("lists tasks and shows detail on row click", async () => {
  const transport = createRouterTransport((router) => {
    router.service(AdminService, {
      listTasks: () => ({ tasks: [task("01ABC", TaskState.PENDING)], nextPageToken: "" }),
    });
  });

  render(
    <ApiProvider api={createApi(transport)}>
      <Tasks />
    </ApiProvider>,
  );

  await userEvent.click(await screen.findByText("01ABC"));
  expect(screen.getByLabelText("Task detail")).toHaveTextContent("email:welcome");
});

test("refetches when the state filter changes", async () => {
  const seen: TaskState[] = [];
  const transport = createRouterTransport((router) => {
    router.service(AdminService, {
      listTasks: (req) => {
        seen.push(req.state);
        return { tasks: [], nextPageToken: "" };
      },
    });
  });

  render(
    <ApiProvider api={createApi(transport)}>
      <Tasks />
    </ApiProvider>,
  );

  await screen.findByText("No tasks match.");
  await userEvent.selectOptions(screen.getByLabelText("State filter"), String(TaskState.ARCHIVED));

  await screen.findByText("No tasks match.");
  expect(seen).toContain(TaskState.ARCHIVED);
});

test("pages forward and back", async () => {
  const transport = createRouterTransport((router) => {
    router.service(AdminService, {
      listTasks: (req) =>
        req.pageToken === ""
          ? { tasks: [task("01A", TaskState.PENDING)], nextPageToken: "01A" }
          : { tasks: [task("02B", TaskState.PENDING)], nextPageToken: "" },
    });
  });

  render(
    <ApiProvider api={createApi(transport)}>
      <Tasks />
    </ApiProvider>,
  );

  expect(await screen.findByText("01A")).toBeInTheDocument();

  await userEvent.click(screen.getByRole("button", { name: "Next" }));
  expect(await screen.findByText("02B")).toBeInTheDocument();

  await userEvent.click(screen.getByRole("button", { name: "Previous" }));
  expect(await screen.findByText("01A")).toBeInTheDocument();
});

test("hides Run for a completed task", async () => {
  const transport = createRouterTransport((router) => {
    router.service(AdminService, {
      listTasks: () => ({ tasks: [task("01DONE", TaskState.COMPLETED)], nextPageToken: "" }),
    });
  });

  render(
    <ApiProvider api={createApi(transport)}>
      <Tasks />
    </ApiProvider>,
  );

  await userEvent.click(await screen.findByText("01DONE"));

  expect(screen.queryByRole("button", { name: "Run now" })).toBeNull();
  expect(screen.queryByRole("button", { name: "Cancel" })).toBeNull();
  expect(screen.getByRole("button", { name: "Delete" })).toBeInTheDocument();
});

test("batch-runs the selected tasks", async () => {
  // The handler receives a protobuf message whose descriptor graph is cyclic,
  // so assert on the ids field directly rather than deep-comparing the message.
  const batchRunTasks = vi.fn().mockReturnValue({ results: [] });
  const transport = createRouterTransport((router) => {
    router.service(AdminService, {
      listTasks: () => ({
        tasks: [task("01A", TaskState.RETRY), task("02B", TaskState.RETRY)],
        nextPageToken: "",
      }),
      batchRunTasks,
    });
  });

  render(
    <ApiProvider api={createApi(transport)}>
      <Tasks />
    </ApiProvider>,
  );

  await userEvent.click(await screen.findByLabelText("Select task 01A"));
  expect(screen.getByText("1 selected")).toBeInTheDocument();

  await userEvent.click(screen.getByRole("button", { name: "Run" }));

  await waitFor(() => expect(batchRunTasks).toHaveBeenCalledOnce());
  expect(batchRunTasks.mock.calls[0][0].ids).toEqual(["01A"]);
});

test("runs a task from the detail panel", async () => {
  const runTask = vi.fn().mockReturnValue({});
  const transport = createRouterTransport((router) => {
    router.service(AdminService, {
      listTasks: () => ({ tasks: [task("01ABC", TaskState.RETRY)], nextPageToken: "" }),
      runTask,
    });
  });

  render(
    <ApiProvider api={createApi(transport)}>
      <Tasks />
    </ApiProvider>,
  );

  await userEvent.click(await screen.findByText("01ABC"));
  await userEvent.click(screen.getByRole("button", { name: "Run now" }));

  expect(runTask).toHaveBeenCalledOnce();
});

test("reschedules a task from the detail panel", async () => {
  // The handler receives a protobuf message whose descriptor graph is cyclic,
  // so assert on individual fields rather than deep-comparing the message.
  const rescheduleTask = vi.fn().mockReturnValue({});
  const transport = createRouterTransport((router) => {
    router.service(AdminService, {
      listTasks: () => ({ tasks: [task("01ABC", TaskState.SCHEDULED)], nextPageToken: "" }),
      rescheduleTask,
    });
  });

  render(
    <ApiProvider api={createApi(transport)}>
      <Tasks />
    </ApiProvider>,
  );

  await userEvent.click(await screen.findByText("01ABC"));

  // The button stays disabled until a time is chosen.
  const button = screen.getByRole("button", { name: "Reschedule" });
  expect(button).toBeDisabled();

  fireEvent.change(screen.getByLabelText("Reschedule to"), { target: { value: "2999-01-01T00:00" } });
  await userEvent.click(button);

  await waitFor(() => expect(rescheduleTask).toHaveBeenCalledOnce());
  expect(rescheduleTask.mock.calls[0][0].id).toBe("01ABC");
  expect(rescheduleTask.mock.calls[0][0].processAt).toBeDefined();
});
