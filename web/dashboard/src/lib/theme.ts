// Theme is the dashboard color scheme.
export type Theme = "light" | "dark";

// themeKey is the localStorage key holding the chosen theme.
const themeKey = "conveyor.theme";

// getTheme returns the stored theme, defaulting to dark.
export function getTheme(): Theme {
  try {
    return localStorage.getItem(themeKey) === "light" ? "light" : "dark";
  } catch {
    return "dark";
  }
}

// setTheme persists the theme.
export function setTheme(theme: Theme): void {
  try {
    localStorage.setItem(themeKey, theme);
  } catch {
    // Storage unavailable: the theme simply does not persist across reloads.
  }
}

// applyTheme reflects the theme onto the document by toggling the `dark` class
// the CSS tokens key off.
export function applyTheme(theme: Theme): void {
  document.documentElement.classList.toggle("dark", theme === "dark");
}
