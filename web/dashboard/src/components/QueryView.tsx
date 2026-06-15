import type { ReactNode } from "react";
import type { QueryState } from "../api/useQuery.ts";

// QueryView renders the standard loading / error / data states for a useQuery
// result, calling children with the resolved data. It keeps showing data
// during a background reload so the table doesn't flash on refresh.
export function QueryView<T>({
  query,
  children,
}: {
  query: QueryState<T>;
  children: (data: T) => ReactNode;
}) {
  // Once data has loaded, keep showing it — even while a background refresh is
  // in flight or transiently failing — so the view never blinks. The error and
  // loading states only show before the first successful load.
  if (query.data !== undefined) {
    return <>{children(query.data)}</>;
  }

  if (query.error !== undefined) {
    return (
      <p role="alert" className="px-5 py-8 text-sm text-rose-600 dark:text-rose-300">
        {query.error}
      </p>
    );
  }

  return (
    <div className="flex items-center gap-2 px-5 py-8 text-sm text-[var(--muted)]">
      <span className="size-4 animate-spin rounded-full border-2 border-[var(--border)] border-t-[var(--muted)]" />
      Loading…
    </div>
  );
}
