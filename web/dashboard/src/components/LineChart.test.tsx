import { expect, test } from "vitest";
import { render, screen } from "@testing-library/react";
import { LineChart } from "./LineChart.tsx";

test("renders a labelled line per series with the latest value", () => {
  render(
    <LineChart
      ariaLabel="Backlog"
      series={[
        { label: "Pending", color: "#0ea5e9", values: [1, 4, 9] },
        { label: "Active", color: "#f59e0b", values: [0, 2, 3] },
      ]}
    />,
  );

  const chart = screen.getByRole("img", { name: "Backlog" });
  expect(chart.querySelectorAll("polyline")).toHaveLength(2);

  // The legend shows each series' most recent value.
  expect(screen.getByText("Pending")).toBeInTheDocument();
  expect(screen.getByText("9")).toBeInTheDocument();
  expect(screen.getByText("3")).toBeInTheDocument();
});

test("renders without error when a series is empty", () => {
  render(<LineChart ariaLabel="Empty" series={[{ label: "Pending", color: "#0ea5e9", values: [] }]} />);
  expect(screen.getByRole("img", { name: "Empty" })).toBeInTheDocument();
});
