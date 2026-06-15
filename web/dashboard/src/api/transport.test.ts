import { afterEach, expect, test } from "vitest";
import { apiBaseUrl } from "./transport.ts";

afterEach(() => {
  delete (window as { CONVEYOR_API_BASE?: string }).CONVEYOR_API_BASE;
});

test("defaults to the page origin", () => {
  expect(apiBaseUrl()).toBe(window.location.origin);
});

test("honors a global override", () => {
  (window as { CONVEYOR_API_BASE?: string }).CONVEYOR_API_BASE = "https://api.example.com";
  expect(apiBaseUrl()).toBe("https://api.example.com");
});
