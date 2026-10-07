import { expect, test, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { ActionAlert } from "./ActionAlert.tsx";

test("renders nothing without a message", () => {
  const { container } = render(<ActionAlert message={undefined} onDismiss={vi.fn()} />);

  expect(container).toBeEmptyDOMElement();
});

test("shows the message and dismisses on click", async () => {
  const onDismiss = vi.fn();
  render(<ActionAlert message="queue is required" onDismiss={onDismiss} />);

  expect(screen.getByRole("alert")).toHaveTextContent("queue is required");

  await userEvent.click(screen.getByRole("button", { name: "Dismiss" }));
  expect(onDismiss).toHaveBeenCalledOnce();
});
