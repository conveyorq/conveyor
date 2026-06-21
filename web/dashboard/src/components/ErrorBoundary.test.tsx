import { afterEach, beforeEach, expect, test, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { useState } from "react";
import { ErrorBoundary } from "./ErrorBoundary.tsx";

// Boom throws on render while shouldThrow is true, modelling a view that
// crashes. A control flips the flag so we can test recovery after reset.
function Boom({ shouldThrow }: { shouldThrow: boolean }) {
  if (shouldThrow) {
    throw new Error("kaboom");
  }

  return <p>recovered</p>;
}

beforeEach(() => {
  // React logs caught render errors; silence it so the test output stays clean.
  vi.spyOn(console, "error").mockImplementation(() => {});
});

afterEach(() => {
  vi.restoreAllMocks();
});

test("renders children when they do not throw", () => {
  render(
    <ErrorBoundary>
      <p>healthy</p>
    </ErrorBoundary>,
  );

  expect(screen.getByText("healthy")).toBeInTheDocument();
});

test("shows a recoverable fallback with the error message when a child throws", () => {
  render(
    <ErrorBoundary>
      <Boom shouldThrow />
    </ErrorBoundary>,
  );

  expect(screen.getByRole("alert")).toHaveTextContent("This view hit an unexpected error.");
  expect(screen.getByText("kaboom")).toBeInTheDocument();
  expect(screen.getByRole("button", { name: "Try again" })).toBeInTheDocument();
});

test("recovers when the underlying problem is fixed and the user retries", async () => {
  function Harness() {
    const [broken, setBroken] = useState(true);

    return (
      <div>
        <button type="button" onClick={() => setBroken(false)}>
          fix
        </button>
        <ErrorBoundary>
          <Boom shouldThrow={broken} />
        </ErrorBoundary>
      </div>
    );
  }

  render(<Harness />);

  expect(screen.getByRole("alert")).toBeInTheDocument();

  // Fix the underlying cause, then retry: the boundary re-renders its children.
  await userEvent.click(screen.getByRole("button", { name: "fix" }));
  await userEvent.click(screen.getByRole("button", { name: "Try again" }));

  expect(screen.getByText("recovered")).toBeInTheDocument();
  expect(screen.queryByRole("alert")).not.toBeInTheDocument();
});
