import { expect, test } from "vitest";
import { render, screen } from "@testing-library/react";
import { Panel } from "./Panel.tsx";

test("renders the title and children", () => {
  render(
    <Panel title="Queues">
      <span>body</span>
    </Panel>,
  );

  expect(screen.getByRole("heading", { name: "Queues" })).toBeInTheDocument();
  expect(screen.getByText("body")).toBeInTheDocument();
});
