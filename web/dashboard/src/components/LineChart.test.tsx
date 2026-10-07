import { expect, test } from "vitest";
import { render, screen } from "@testing-library/react";
import { LineChart, niceMax } from "./LineChart.tsx";

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

test("rounds the axis maximum up to a readable value", () => {
  expect(niceMax(1)).toBe(1);
  expect(niceMax(7)).toBe(8);
  expect(niceMax(270)).toBe(300);
  expect(niceMax(1825)).toBe(2000);
  expect(niceMax(9100)).toBe(10000);
});

test("labels the y axis and the sampled time span", () => {
  const start = new Date("2026-10-07T19:40:00Z").getTime();
  const end = new Date("2026-10-07T19:45:30Z").getTime();

  render(
    <LineChart
      ariaLabel="Backlog"
      times={[start, start + 1000, end]}
      series={[{ label: "Pending", color: "#0ea5e9", values: [10, 270, 40] }]}
    />,
  );

  // The axis tops out at the rounded maximum and starts at zero.
  expect(screen.getByText("300")).toBeInTheDocument();
  expect(screen.getByText("0")).toBeInTheDocument();

  // The span runs from the first sample to the last.
  expect(screen.getByText(new Date(start).toLocaleTimeString())).toBeInTheDocument();
  expect(screen.getByText(new Date(end).toLocaleTimeString())).toBeInTheDocument();
});

test("omits the time span without sample times", () => {
  render(<LineChart ariaLabel="Backlog" series={[{ label: "Pending", color: "#0ea5e9", values: [1, 2] }]} />);

  expect(screen.queryByText(/:\d\d/)).toBeNull();
});
