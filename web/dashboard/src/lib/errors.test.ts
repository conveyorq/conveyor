import { expect, test } from "vitest";
import { ConnectError, Code } from "@connectrpc/connect";
import { errorMessage } from "./errors.ts";

test("extracts the message from a plain error", () => {
  expect(errorMessage(new Error("boom"))).toContain("boom");
});

test("extracts the message from a ConnectError", () => {
  expect(errorMessage(new ConnectError("denied", Code.Unauthenticated))).toContain("denied");
});
