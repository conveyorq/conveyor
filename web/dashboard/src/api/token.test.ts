import { afterEach, expect, test } from "vitest";
import { getToken, ingestTokenFromURL, setToken } from "./token.ts";

afterEach(() => {
  localStorage.clear();
  window.history.replaceState(null, "", "/");
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

test("ingests a token from the URL and strips it", () => {
  window.history.replaceState(null, "", "/?token=from-url");
  ingestTokenFromURL();

  expect(getToken()).toBe("from-url");
  expect(window.location.search).toBe("");
});

test("ingest preserves other query parameters", () => {
  window.history.replaceState(null, "", "/?api=https://other&token=from-url");
  ingestTokenFromURL();

  expect(getToken()).toBe("from-url");
  expect(new URLSearchParams(window.location.search).get("api")).toBe("https://other");
  expect(new URLSearchParams(window.location.search).get("token")).toBeNull();
});

test("ingest is a no-op when no token parameter is present", () => {
  setToken("typed");
  window.history.replaceState(null, "", "/?api=https://other");
  ingestTokenFromURL();

  expect(getToken()).toBe("typed");
});
