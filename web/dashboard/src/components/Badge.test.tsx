import { expect, test } from "vitest";
import { render, screen } from "@testing-library/react";
import { Badge } from "./Badge.tsx";

test("renders its label", () => {
  render(<Badge tone="emerald">completed</Badge>);
  expect(screen.getByText("completed")).toBeInTheDocument();
});

test("applies the tone classes", () => {
  render(<Badge tone="rose">archived</Badge>);
  expect(screen.getByText("archived").className).toContain("rose");
});
