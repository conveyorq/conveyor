import { expect, test } from "vitest";
import { ConnectError, Code } from "@connectrpc/connect";
import { errorMessage } from "./errors.ts";

test("shows a plain error's message as written", () => {
  expect(errorMessage(new Error("boom"))).toBe("boom");
});

test("keeps the code prefix on a ConnectError", () => {
  expect(errorMessage(new ConnectError("denied", Code.Unauthenticated))).toBe("[unauthenticated] denied");
});

test("normalizes a non-Error throw", () => {
  expect(errorMessage("boom")).toContain("boom");
});
