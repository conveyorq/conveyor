import { afterEach, expect, test } from "vitest";
import { applyTheme, getTheme, setTheme } from "./theme.ts";

afterEach(() => {
  localStorage.clear();
  document.documentElement.classList.remove("dark");
});

test("defaults to dark", () => {
  expect(getTheme()).toBe("dark");
});

test("round-trips the stored theme", () => {
  setTheme("light");
  expect(getTheme()).toBe("light");
});

test("applyTheme toggles the dark class", () => {
  applyTheme("dark");
  expect(document.documentElement.classList.contains("dark")).toBe(true);

  applyTheme("light");
  expect(document.documentElement.classList.contains("dark")).toBe(false);
});
