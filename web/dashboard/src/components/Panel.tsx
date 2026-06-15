import type { ReactNode } from "react";

// Panel is the standard card container: a titled, bordered surface. Its body is
// flush (no padding) so tables reach the edges; text content adds its own.
// An optional actions slot holds controls (filters, buttons) in the header.
export function Panel({
  title,
  actions,
  children,
}: {
  title: string;
  actions?: ReactNode;
  children: ReactNode;
}) {
  return (
    <section className="overflow-hidden rounded-xl border border-[var(--border)] bg-[var(--surface)]">
      <header className="flex items-center gap-3 border-b border-[var(--border)] px-5 py-3">
        <h3 className="text-sm font-semibold">{title}</h3>
        {actions !== undefined && <div className="ml-auto flex items-center gap-2">{actions}</div>}
      </header>
      {children}
    </section>
  );
}
