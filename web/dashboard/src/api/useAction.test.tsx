import { expect, test, vi } from "vitest";
import { renderHook, waitFor, act } from "@testing-library/react";
import { useAction } from "./useAction.ts";

test("reloads on success", async () => {
  const reload = vi.fn();
  const { result } = renderHook(() => useAction(reload));

  await act(async () => {
    await result.current.run(() => Promise.resolve());
  });

  expect(reload).toHaveBeenCalledOnce();
  expect(result.current.error).toBeUndefined();
});

test("captures the error and does not reload on failure", async () => {
  const reload = vi.fn();
  const { result } = renderHook(() => useAction(reload));

  await act(async () => {
    await result.current.run(() => Promise.reject(new Error("denied")));
  });

  expect(reload).not.toHaveBeenCalled();
  await waitFor(() => expect(result.current.error).toContain("denied"));
});
