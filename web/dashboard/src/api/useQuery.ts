import { useEffect, useState } from "react";
import { errorMessage } from "../lib/errors.ts";
import { useRefreshTick } from "./refresh.ts";

// QueryState is the result of a one-shot async query: the data once resolved,
// an error message on failure, a loading flag, and a manual reload trigger.
export interface QueryState<T> {
  data?: T;
  error?: string;
  loading: boolean;
  reload: () => void;
}

// useQuery runs an async function on mount and whenever deps change, tracking
// loading and error state. reload() re-runs it on demand (after a mutation, or
// for manual refresh). Results from a superseded run are discarded.
export function useQuery<T>(fn: () => Promise<T>, deps: unknown[]): QueryState<T> {
  const [data, setData] = useState<T | undefined>(undefined);
  const [error, setError] = useState<string | undefined>(undefined);
  const [loading, setLoading] = useState(true);
  const [nonce, setNonce] = useState(0);
  const tick = useRefreshTick();

  useEffect(() => {
    let active = true;

    // loading is never re-raised here: the first load shows the loading state,
    // but background refreshes (auto-refresh, reload, dep changes) keep the
    // current data on screen, so the view never blinks.
    fn()
      .then((result) => {
        if (active) {
          setData(result);
          setError(undefined);
          setLoading(false);
        }
      })
      .catch((err: unknown) => {
        if (active) {
          setError(errorMessage(err));
          setLoading(false);
        }
      });

    return () => {
      active = false;
    };
    // fn identity is intentionally excluded; deps, nonce, and the auto-refresh
    // tick drive re-runs.
  }, [...deps, nonce, tick]);

  return { data, error, loading, reload: () => setNonce((n) => n + 1) };
}
