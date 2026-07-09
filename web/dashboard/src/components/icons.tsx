import type { SVGProps } from "react";

// Icons are inline stroke SVGs (no icon dependency). They inherit the current
// text color and are hidden from the accessibility tree — labels come from the
// surrounding text.
function Svg({ children, ...props }: SVGProps<SVGSVGElement>) {
  return (
    <svg
      viewBox="0 0 24 24"
      width="18"
      height="18"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.8"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
      {...props}
    >
      {children}
    </svg>
  );
}

// IconLogo is the Conveyor mark: a stylized conveyor belt.
export function IconLogo(props: SVGProps<SVGSVGElement>) {
  return (
    <Svg {...props}>
      <circle cx="6" cy="15" r="2.5" />
      <circle cx="18" cy="15" r="2.5" />
      <path d="M6 12.5h12M4 9h6l2-3h6" />
    </Svg>
  );
}

// IconOverview is a 2x2 grid (the overview).
export function IconOverview(props: SVGProps<SVGSVGElement>) {
  return (
    <Svg {...props}>
      <rect x="3" y="3" width="7" height="7" rx="1.5" />
      <rect x="14" y="3" width="7" height="7" rx="1.5" />
      <rect x="3" y="14" width="7" height="7" rx="1.5" />
      <rect x="14" y="14" width="7" height="7" rx="1.5" />
    </Svg>
  );
}

// IconQueues is stacked layers (queues).
export function IconQueues(props: SVGProps<SVGSVGElement>) {
  return (
    <Svg {...props}>
      <path d="M12 3 3 7.5 12 12l9-4.5L12 3Z" />
      <path d="m3 12 9 4.5L21 12" />
      <path d="m3 16.5 9 4.5 9-4.5" />
    </Svg>
  );
}

// IconTasks is a checklist (tasks).
export function IconTasks(props: SVGProps<SVGSVGElement>) {
  return (
    <Svg {...props}>
      <path d="M9 6h12M9 12h12M9 18h12" />
      <path d="m3 6 1.5 1.5L7 5M3 12l1.5 1.5L7 11M3 18l1.5 1.5L7 17" />
    </Svg>
  );
}

// IconCron is a clock (cron schedules).
export function IconCron(props: SVGProps<SVGSVGElement>) {
  return (
    <Svg {...props}>
      <circle cx="12" cy="12" r="9" />
      <path d="M12 7v5l3 2" />
    </Svg>
  );
}

// IconWorkers is a server stack (worker sessions).
export function IconWorkers(props: SVGProps<SVGSVGElement>) {
  return (
    <Svg {...props}>
      <rect x="3" y="4" width="18" height="7" rx="1.5" />
      <rect x="3" y="13" width="18" height="7" rx="1.5" />
      <path d="M7 7.5h.01M7 16.5h.01" />
    </Svg>
  );
}

// IconMetrics is a line chart (metrics over time).
export function IconMetrics(props: SVGProps<SVGSVGElement>) {
  return (
    <Svg {...props}>
      <path d="M3 3v18h18" />
      <path d="m7 14 3-4 3 3 4-6" />
    </Svg>
  );
}

// IconBroker is a database cylinder (the storage engine).
export function IconBroker(props: SVGProps<SVGSVGElement>) {
  return (
    <Svg {...props}>
      <ellipse cx="12" cy="5" rx="8" ry="3" />
      <path d="M4 5v6c0 1.7 3.6 3 8 3s8-1.3 8-3V5" />
      <path d="M4 11v6c0 1.7 3.6 3 8 3s8-1.3 8-3v-6" />
    </Svg>
  );
}

// IconLimits is a speedometer gauge (dispatch rate limits).
export function IconLimits(props: SVGProps<SVGSVGElement>) {
  return (
    <Svg {...props}>
      <path d="M4 18a8 8 0 1 1 16 0" />
      <path d="m12 18 4-5" />
    </Svg>
  );
}

// IconConcurrency is stacked layers (per-key concurrency limits).
export function IconConcurrency(props: SVGProps<SVGSVGElement>) {
  return (
    <Svg {...props}>
      <path d="M12 3 3 8l9 5 9-5-9-5Z" />
      <path d="m3 12 9 5 9-5" />
      <path d="m3 16 9 5 9-5" />
    </Svg>
  );
}

// IconGroups is items coalescing into a batch (per-group aggregation config).
export function IconGroups(props: SVGProps<SVGSVGElement>) {
  return (
    <Svg {...props}>
      <rect x="3" y="3" width="7" height="7" rx="1" />
      <rect x="14" y="3" width="7" height="7" rx="1" />
      <rect x="8" y="14" width="8" height="7" rx="1" />
    </Svg>
  );
}

// IconExternal is an external-link arrow.
export function IconExternal(props: SVGProps<SVGSVGElement>) {
  return (
    <Svg {...props}>
      <path d="M14 5h5v5M19 5l-8 8" />
      <path d="M19 14v4a2 2 0 0 1-2 2H6a2 2 0 0 1-2-2V7a2 2 0 0 1 2-2h4" />
    </Svg>
  );
}

// IconWebhooks is an outgoing bolt into a node (webhook push delivery).
export function IconWebhooks(props: SVGProps<SVGSVGElement>) {
  return (
    <Svg {...props}>
      <path d="M13 3l-5 8h4l-2 7 7-10h-4l3-5z" />
      <circle cx="19" cy="19" r="2.5" />
    </Svg>
  );
}

// IconSun is a sun (switch to light mode).
export function IconSun(props: SVGProps<SVGSVGElement>) {
  return (
    <Svg {...props}>
      <circle cx="12" cy="12" r="4" />
      <path d="M12 2v2M12 20v2M4.9 4.9l1.4 1.4M17.7 17.7l1.4 1.4M2 12h2M20 12h2M4.9 19.1l1.4-1.4M17.7 6.3l1.4-1.4" />
    </Svg>
  );
}

// IconMoon is a moon (switch to dark mode).
export function IconMoon(props: SVGProps<SVGSVGElement>) {
  return (
    <Svg {...props}>
      <path d="M21 12.8A9 9 0 1 1 11.2 3a7 7 0 0 0 9.8 9.8Z" />
    </Svg>
  );
}
