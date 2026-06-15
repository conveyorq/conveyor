import { createContext, useContext, type ReactNode } from "react";

// ReadOnlyContext carries whether the dashboard is in read-only mode. Views
// read it to hide their mutation controls; the server enforces the mode
// independently, so a stale UI can never actually mutate state.
const ReadOnlyContext = createContext(false);

// ReadOnlyProvider makes the read-only flag available to the component tree.
export function ReadOnlyProvider({ value, children }: { value: boolean; children: ReactNode }) {
  return <ReadOnlyContext.Provider value={value}>{children}</ReadOnlyContext.Provider>;
}

// useReadOnly returns whether mutation controls should be hidden.
export function useReadOnly(): boolean {
  return useContext(ReadOnlyContext);
}
