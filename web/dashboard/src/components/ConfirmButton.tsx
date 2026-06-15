import { useState } from "react";

// ConfirmButton runs an action on click. When confirm is set, the first click
// reveals an inline Confirm/Cancel pair so a destructive action (cancel,
// delete) is never a single misclick; a non-confirm button fires immediately.
export function ConfirmButton({
  label,
  onConfirm,
  confirm = false,
  danger = false,
}: {
  label: string;
  onConfirm: () => Promise<void>;
  confirm?: boolean;
  danger?: boolean;
}) {
  const [confirming, setConfirming] = useState(false);
  const [busy, setBusy] = useState(false);

  async function run() {
    setBusy(true);

    try {
      await onConfirm();
    } finally {
      setBusy(false);
      setConfirming(false);
    }
  }

  const base = "rounded px-2 py-0.5 text-xs disabled:opacity-50";
  const tone = danger
    ? "bg-rose-100 text-rose-700 hover:bg-rose-200 dark:bg-rose-900 dark:text-rose-100 dark:hover:bg-rose-800"
    : "bg-[var(--btn-bg)] text-[var(--text-soft)] hover:bg-[var(--btn-hover)]";

  if (confirm && confirming) {
    return (
      <span className="inline-flex gap-1">
        <button type="button" disabled={busy} onClick={() => void run()} className={`${base} bg-rose-600 text-white hover:bg-rose-500`}>
          Confirm
        </button>
        <button
          type="button"
          disabled={busy}
          onClick={() => setConfirming(false)}
          className={`${base} bg-[var(--btn-bg)] text-[var(--text-soft)] hover:bg-[var(--btn-hover)]`}
        >
          Cancel
        </button>
      </span>
    );
  }

  return (
    <button
      type="button"
      disabled={busy}
      onClick={() => (confirm ? setConfirming(true) : void run())}
      className={`${base} ${tone}`}
    >
      {label}
    </button>
  );
}
