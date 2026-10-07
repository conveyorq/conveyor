import { useCallback, useState } from "react";
import { errorMessage } from "../lib/errors.ts";

// ActionState runs a one-shot mutation, surfacing its error and reloading the
// underlying query on success.
export interface ActionState {
  // error is the last failed mutation's message, until the next run or a
  // dismiss clears it.
  error?: string;
  // run executes one mutation.
  run: (fn: () => Promise<unknown>) => Promise<void>;
  // dismiss clears the shown error.
  dismiss: () => void;
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

  const dismiss = useCallback(() => setError(undefined), []);

  return { error, run, dismiss };
}
