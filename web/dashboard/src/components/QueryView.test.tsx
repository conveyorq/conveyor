import { expect, test } from "vitest";
import { render, screen } from "@testing-library/react";
import { QueryView } from "./QueryView.tsx";
import type { QueryState } from "../api/useQuery.ts";

const reload = () => {};

test("shows loading when there is no data yet", () => {
  const query: QueryState<string> = { loading: true, reload };
  render(<QueryView query={query}>{(d) => <span>{d}</span>}</QueryView>);
  expect(screen.getByText("Loading…")).toBeInTheDocument();
});

test("shows an error alert", () => {
  const query: QueryState<string> = { loading: false, error: "boom", reload };
  render(<QueryView query={query}>{(d) => <span>{d}</span>}</QueryView>);
  expect(screen.getByRole("alert")).toHaveTextContent("boom");
});

test("renders children with resolved data", () => {
  const query: QueryState<string> = { loading: false, data: "hello", reload };
  render(<QueryView query={query}>{(d) => <span>{d}</span>}</QueryView>);
  expect(screen.getByText("hello")).toBeInTheDocument();
});
