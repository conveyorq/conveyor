import { expect, test, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { ConfirmButton } from "./ConfirmButton.tsx";

test("fires immediately without confirm", async () => {
  const onConfirm = vi.fn().mockResolvedValue(undefined);
  render(<ConfirmButton label="Pause" onConfirm={onConfirm} />);

  await userEvent.click(screen.getByRole("button", { name: "Pause" }));

  expect(onConfirm).toHaveBeenCalledOnce();
});

test("requires a second click to confirm a destructive action", async () => {
  const onConfirm = vi.fn().mockResolvedValue(undefined);
  render(<ConfirmButton label="Delete" onConfirm={onConfirm} confirm />);

  await userEvent.click(screen.getByRole("button", { name: "Delete" }));
  expect(onConfirm).not.toHaveBeenCalled();

  await userEvent.click(screen.getByRole("button", { name: "Confirm delete" }));
  expect(onConfirm).toHaveBeenCalledOnce();
});

test("back aborts a destructive action", async () => {
  const onConfirm = vi.fn().mockResolvedValue(undefined);
  render(<ConfirmButton label="Delete" onConfirm={onConfirm} confirm />);

  await userEvent.click(screen.getByRole("button", { name: "Delete" }));
  await userEvent.click(screen.getByRole("button", { name: "Back" }));

  expect(onConfirm).not.toHaveBeenCalled();
  expect(screen.getByRole("button", { name: "Delete" })).toBeInTheDocument();
});

test("the confirm pair never reads as a cancel action", async () => {
  // Next to a "Cancel" task action, a "Cancel" dismiss button would be
  // ambiguous; the pair names the action and dismisses with "Back".
  render(<ConfirmButton label="Cancel" onConfirm={vi.fn().mockResolvedValue(undefined)} confirm />);

  await userEvent.click(screen.getByRole("button", { name: "Cancel" }));

  expect(screen.getByRole("button", { name: "Confirm cancel" })).toBeInTheDocument();
  expect(screen.getByRole("button", { name: "Back" })).toBeInTheDocument();
  expect(screen.queryByRole("button", { name: "Cancel" })).toBeNull();
});
