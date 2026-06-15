import { expect, test } from "vitest";
import { renderHook, waitFor } from "@testing-library/react";
import { useQuery } from "./useQuery.ts";

test("resolves data and clears loading", async () => {
  const { result } = renderHook(() => useQuery(() => Promise.resolve(42), []));

  expect(result.current.loading).toBe(true);

  await waitFor(() => expect(result.current.loading).toBe(false));
  expect(result.current.data).toBe(42);
  expect(result.current.error).toBeUndefined();
});

test("captures an error message", async () => {
  const { result } = renderHook(() => useQuery(() => Promise.reject(new Error("nope")), []));

  await waitFor(() => expect(result.current.loading).toBe(false));
  expect(result.current.error).toContain("nope");
});
