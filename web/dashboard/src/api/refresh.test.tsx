import type { ReactNode } from "react";
import { expect, test } from "vitest";
import { renderHook } from "@testing-library/react";
import { RefreshTickContext, useRefreshTick } from "./refresh.ts";

test("defaults to zero with no provider", () => {
  const { result } = renderHook(() => useRefreshTick());
  expect(result.current).toBe(0);
});

test("reads the provided tick", () => {
  const wrapper = ({ children }: { children: ReactNode }) => (
    <RefreshTickContext.Provider value={7}>{children}</RefreshTickContext.Provider>
  );
  const { result } = renderHook(() => useRefreshTick(), { wrapper });
  expect(result.current).toBe(7);
});
