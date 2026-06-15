import { createContext, useContext } from "react";

// RefreshTickContext carries a counter that increments on each auto-refresh
// interval. Queries include it in their dependencies, so a bump re-runs every
// active query — giving live-updating views without a push stream.
export const RefreshTickContext = createContext(0);

// useRefreshTick returns the current auto-refresh tick (0 when disabled).
export function useRefreshTick(): number {
  return useContext(RefreshTickContext);
}
