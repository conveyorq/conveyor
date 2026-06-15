import type { ReactNode } from "react";

// Tone selects a badge's semantic color.
export type Tone = "zinc" | "sky" | "violet" | "amber" | "orange" | "emerald" | "rose" | "indigo";

// tones maps each tone to its full Tailwind classes. The strings are written
// out in full (not composed) so Tailwind's scanner keeps them.
const tones: Record<Tone, string> = {
  zinc: "bg-zinc-100 text-zinc-700 ring-zinc-600/20 dark:bg-zinc-500/15 dark:text-zinc-300 dark:ring-zinc-500/30",
  sky: "bg-sky-50 text-sky-700 ring-sky-600/20 dark:bg-sky-500/15 dark:text-sky-300 dark:ring-sky-500/30",
  violet: "bg-violet-50 text-violet-700 ring-violet-600/20 dark:bg-violet-500/15 dark:text-violet-300 dark:ring-violet-500/30",
  amber: "bg-amber-50 text-amber-700 ring-amber-600/20 dark:bg-amber-500/15 dark:text-amber-300 dark:ring-amber-500/30",
  orange: "bg-orange-50 text-orange-700 ring-orange-600/20 dark:bg-orange-500/15 dark:text-orange-300 dark:ring-orange-500/30",
  emerald: "bg-emerald-50 text-emerald-700 ring-emerald-600/20 dark:bg-emerald-500/15 dark:text-emerald-300 dark:ring-emerald-500/30",
  rose: "bg-rose-50 text-rose-700 ring-rose-600/20 dark:bg-rose-500/15 dark:text-rose-300 dark:ring-rose-500/30",
  indigo: "bg-indigo-50 text-indigo-700 ring-indigo-600/20 dark:bg-indigo-500/15 dark:text-indigo-300 dark:ring-indigo-500/30",
};

// Badge is a small rounded status pill in the given semantic tone.
export function Badge({ tone = "zinc", children }: { tone?: Tone; children: ReactNode }) {
  return (
    <span
      className={`inline-flex items-center rounded-full px-2 py-0.5 text-xs font-medium ring-1 ring-inset ${tones[tone]}`}
    >
      {children}
    </span>
  );
}
