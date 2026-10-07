import { IconClose } from "./icons.tsx";

// ActionAlert shows the message of a failed action with a button to dismiss
// it, so a stale failure does not linger after the operator has read it. It
// renders nothing when there is no message.
export function ActionAlert({ message, onDismiss }: { message: string | undefined; onDismiss: () => void }) {
  if (message === undefined) {
    return null;
  }

  return (
    <div
      role="alert"
      className="flex items-start gap-3 rounded-lg border border-rose-500/30 bg-rose-50 px-4 py-2.5 text-sm text-rose-700 dark:bg-rose-500/10 dark:text-rose-300"
    >
      <span className="min-w-0 flex-1 break-words">{message}</span>
      <button
        type="button"
        aria-label="Dismiss"
        onClick={onDismiss}
        className="shrink-0 rounded p-0.5 text-rose-500 hover:bg-rose-100 hover:text-rose-700 dark:hover:bg-rose-500/20 dark:hover:text-rose-200"
      >
        <IconClose width="14" height="14" />
      </button>
    </div>
  );
}
