import { afterEach, expect, test } from "vitest";
import { getToken, setToken } from "./token.ts";

afterEach(() => {
  localStorage.clear();
});

test("returns empty when unset", () => {
  expect(getToken()).toBe("");
});

test("round-trips a token", () => {
  setToken("secret");
  expect(getToken()).toBe("secret");
});

test("clears a token when set empty", () => {
  setToken("secret");
  setToken("");
  expect(getToken()).toBe("");
});
