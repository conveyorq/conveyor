import { afterEach, expect, test, vi } from "vitest";
import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { Code, ConnectError, createRouterTransport } from "@connectrpc/connect";
import { create } from "@bufbuild/protobuf";
import { timestampFromDate } from "@bufbuild/protobuf/wkt";
import { AdminService, TaskInfoSchema, TaskService } from "../gen/conveyor/v1/service_pb.ts";
import { TaskState } from "../gen/conveyor/v1/task_pb.ts";
import { ApiProvider, createApi } from "../api/context.tsx";
import { resetTaskFilters } from "../lib/taskFilters.ts";
import { Tasks } from "./Tasks.tsx";

// Filters are remembered across view switches; start every test from the
// defaults.
afterEach(() => resetTaskFilters());

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

test("shows reported progress in the detail panel", async () => {
  const transport = createRouterTransport((router) => {
    router.service(AdminService, {
      listTasks: () => ({
        tasks: [
          create(TaskInfoSchema, {
            id: "01PROG",
            type: "report:build",
            queue: "default",
            state: TaskState.ACTIVE,
            progress: 42,
            progressMessage: "halfway",
          }),
        ],
        nextPageToken: "",
      }),
    });
  });

  render(
    <ApiProvider api={createApi(transport)}>
      <Tasks />
    </ApiProvider>,
  );

  await userEvent.click(await screen.findByText("01PROG"));
  expect(screen.getByLabelText("Task detail")).toHaveTextContent("42% (halfway)");
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

test("shows a running duration only for an active task", async () => {
  // A retry task keeps its last attempt's start time but is not running.
  const startedAt = timestampFromDate(new Date(Date.now() - 60_000));
  const transport = createRouterTransport((router) => {
    router.service(AdminService, {
      listTasks: () => ({
        tasks: [
          create(TaskInfoSchema, { id: "01RETRY", state: TaskState.RETRY, startedAt }),
          create(TaskInfoSchema, { id: "01ACTIVE", state: TaskState.ACTIVE, startedAt }),
        ],
        nextPageToken: "",
      }),
    });
  });

  render(
    <ApiProvider api={createApi(transport)}>
      <Tasks />
    </ApiProvider>,
  );

  await userEvent.click(await screen.findByText("01RETRY"));
  expect(screen.getByLabelText("Task detail")).not.toHaveTextContent("(running)");

  await userEvent.click(screen.getByText("01ACTIVE"));
  expect(screen.getByLabelText("Task detail")).toHaveTextContent("(running)");
});

test("reloads the list after a partially failed batch", async () => {
  // Part of the batch committed, so the list must refresh even though the
  // action reports the failures.
  const listTasks = vi.fn().mockReturnValue({
    tasks: [task("01A", TaskState.PENDING), task("02B", TaskState.ARCHIVED)],
    nextPageToken: "",
  });
  const batchRunTasks = vi.fn().mockReturnValue({
    results: [
      { id: "01A", error: "" },
      { id: "02B", error: "invalid state" },
    ],
  });
  const transport = createRouterTransport((router) => {
    router.service(AdminService, { listTasks, batchRunTasks });
  });

  render(
    <ApiProvider api={createApi(transport)}>
      <Tasks />
    </ApiProvider>,
  );

  await userEvent.click(await screen.findByLabelText("Select all tasks on this page"));
  const callsBefore = listTasks.mock.calls.length;
  await userEvent.click(screen.getByRole("button", { name: "Run" }));

  expect(await screen.findByRole("alert")).toHaveTextContent("1 of 2 failed: 02B (invalid state)");
  await waitFor(() => expect(listTasks.mock.calls.length).toBeGreaterThan(callsBefore));

  // The failure stays selected for a retry; the success leaves the selection.
  expect(screen.getByLabelText("Select task 02B")).toBeChecked();
  expect(screen.getByLabelText("Select task 01A")).not.toBeChecked();

  // A new search clears the stale error.
  await userEvent.selectOptions(screen.getByLabelText("State filter"), String(TaskState.PENDING));
  await waitFor(() => expect(screen.queryByRole("alert")).toBeNull());
});

test("filters by every waiting state, including blocked and aggregating", async () => {
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
  await userEvent.selectOptions(screen.getByLabelText("State filter"), String(TaskState.BLOCKED));
  await userEvent.selectOptions(screen.getByLabelText("State filter"), String(TaskState.AGGREGATING));

  await waitFor(() => expect(seen).toEqual(expect.arrayContaining([TaskState.BLOCKED, TaskState.AGGREGATING])));
});

test("keeps the detail panel open and current after an action", async () => {
  // The run commits, so the refetched record comes back pending.
  let state = TaskState.SCHEDULED;
  const transport = createRouterTransport((router) => {
    router.service(AdminService, {
      listTasks: () => ({ tasks: [task("01ABC", state)], nextPageToken: "" }),
      runTask: () => {
        state = TaskState.PENDING;
        return {};
      },
    });
    router.service(TaskService, {
      getTask: () => ({ task: task("01ABC", state) }),
    });
  });

  render(
    <ApiProvider api={createApi(transport)}>
      <Tasks />
    </ApiProvider>,
  );

  await userEvent.click(await screen.findByText("01ABC"));
  await userEvent.click(screen.getByRole("button", { name: "Run now" }));

  await waitFor(() => expect(screen.getByLabelText("Task detail")).toHaveTextContent("pending"));
});

test("closes the detail panel once the task is deleted", async () => {
  let deleted = false;
  const transport = createRouterTransport((router) => {
    router.service(AdminService, {
      listTasks: () => ({ tasks: deleted ? [] : [task("01ABC", TaskState.ARCHIVED)], nextPageToken: "" }),
      deleteTask: () => {
        deleted = true;
        return {};
      },
    });
    router.service(TaskService, {
      getTask: () => {
        if (deleted) {
          throw new ConnectError("task not found", Code.NotFound);
        }

        return { task: task("01ABC", TaskState.ARCHIVED) };
      },
    });
  });

  render(
    <ApiProvider api={createApi(transport)}>
      <Tasks />
    </ApiProvider>,
  );

  await userEvent.click(await screen.findByText("01ABC"));
  await userEvent.click(screen.getByRole("button", { name: "Delete" }));
  await userEvent.click(screen.getByRole("button", { name: "Confirm delete" }));

  await waitFor(() => expect(screen.queryByLabelText("Task detail")).toBeNull());
});

test("applies the queue filter once typing pauses", async () => {
  const queues: string[] = [];
  const transport = createRouterTransport((router) => {
    router.service(AdminService, {
      listTasks: (req) => {
        queues.push(req.queue);
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
  await userEvent.type(screen.getByLabelText("Queue filter"), "email");

  await waitFor(() => expect(queues).toContain("email"));
  // One fetch for the typed name, not one per keystroke.
  expect(queues.filter((queue) => queue !== "")).toEqual(["email"]);
});

test("remembers the filters across view switches", async () => {
  const transport = createRouterTransport((router) => {
    router.service(AdminService, {
      listTasks: () => ({ tasks: [], nextPageToken: "" }),
    });
  });
  const api = createApi(transport);

  const first = render(
    <ApiProvider api={api}>
      <Tasks />
    </ApiProvider>,
  );

  await screen.findByText("No tasks match.");
  await userEvent.selectOptions(screen.getByLabelText("State filter"), String(TaskState.RETRY));
  await userEvent.type(screen.getByLabelText("Queue filter"), "email");
  await waitFor(() => expect(screen.getByLabelText("Queue filter")).toHaveValue("email"));
  await new Promise((resolve) => setTimeout(resolve, 400));
  first.unmount();

  render(
    <ApiProvider api={api}>
      <Tasks />
    </ApiProvider>,
  );

  expect(screen.getByLabelText("State filter")).toHaveValue(String(TaskState.RETRY));
  expect(screen.getByLabelText("Queue filter")).toHaveValue("email");
});

test("drops a selected task from the batch once it leaves the page", async () => {
  let rows = [task("01A", TaskState.ARCHIVED), task("02B", TaskState.ARCHIVED)];
  const transport = createRouterTransport((router) => {
    router.service(AdminService, {
      listTasks: () => ({ tasks: rows, nextPageToken: "" }),
      deleteTask: (req) => {
        rows = rows.filter((row) => row.id !== req.id);
        return {};
      },
    });
  });

  render(
    <ApiProvider api={createApi(transport)}>
      <Tasks />
    </ApiProvider>,
  );

  await userEvent.click(await screen.findByLabelText("Select task 01A"));
  expect(screen.getByText("1 selected")).toBeInTheDocument();

  // Deleting the selected task from its detail panel reloads the list.
  await userEvent.click(screen.getByText("01A"));
  await userEvent.click(within(screen.getByLabelText("Task detail")).getByRole("button", { name: "Delete" }));
  await userEvent.click(screen.getByRole("button", { name: "Confirm delete" }));

  await waitFor(() => expect(screen.queryByText("1 selected")).toBeNull());
});
