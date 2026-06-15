import { useCallback, useState } from "react";
import { errorMessage } from "../lib/errors.ts";

// ActionState runs a one-shot mutation, surfacing its error and reloading the
// underlying query on success.
export interface ActionState {
  error?: string;
  run: (fn: () => Promise<unknown>) => Promise<void>;
}

// useAction wraps a mutation call: on success it triggers reload so the view
// reflects the change; on failure it captures the error message for display.
export function useAction(reload: () => void): ActionState {
  const [error, setError] = useState<string | undefined>(undefined);

  const run = useCallback(
    async (fn: () => Promise<unknown>) => {
      setError(undefined);

      try {
        await fn();
        reload();
      } catch (err: unknown) {
        setError(errorMessage(err));
      }
    },
    [reload],
  );

  return { error, run };
}
