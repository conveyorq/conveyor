import { memo } from "react";
import { formatNumber } from "../lib/format.ts";

// Series is one labelled line in a LineChart.
export interface Series {
  label: string;
  // color is any CSS color used for the stroke and legend swatch.
  color: string;
  values: number[];
}

// chartWidth and chartHeight are the SVG viewBox dimensions; the chart scales
// to its container width via preserveAspectRatio.
const chartWidth = 640;
const chartHeight = 200;
const padding = 8;

// niceSteps are the leading digits an axis maximum is rounded up to, so the
// top label reads as a round number (270 becomes 300, not 270).
const niceSteps = [1, 1.5, 2, 2.5, 3, 4, 5, 6, 8, 10];

// niceMax rounds a positive value up to the next readable axis maximum.
export function niceMax(value: number): number {
  const magnitude = 10 ** Math.floor(Math.log10(value));
  const leading = value / magnitude;
  const step = niceSteps.find((candidate) => candidate >= leading) ?? 10;

  return step * magnitude;
}

// formatClock renders a sample instant as a wall-clock time for the x axis.
function formatClock(time: number): string {
  return new Date(time).toLocaleTimeString();
}

// linePoints maps a value series to an SVG polyline points string across the
// plot area, scaling y by the shared maximum.
function linePoints(values: number[], max: number): string {
  if (values.length === 0) {
    return "";
  }

  const plotWidth = chartWidth - padding * 2;
  const plotHeight = chartHeight - padding * 2;
  const step = values.length > 1 ? plotWidth / (values.length - 1) : 0;

  return values
    .map((value, index) => {
      const x = padding + index * step;
      // Clamp into [0, max] so an out-of-range value (a negative, or a stale
      // sample above the current max) never draws outside the plot area.
      const clamped = Math.max(0, Math.min(value, max));
      const y = padding + plotHeight - (clamped / max) * plotHeight;

      return `${x.toFixed(1)},${y.toFixed(1)}`;
    })
    .join(" ");
}

// LineChart renders one or more value series as overlaid SVG lines with a
// y axis (zero, a midline, and a rounded maximum), the time span of the samples
// when times are given, and a legend. It is dependency-free: a hand-rolled SVG
// keeps the bundle small. The axis labels are HTML beside and below the SVG,
// because the SVG stretches to its container and would distort text drawn
// inside it. It is memoized so a parent re-render (e.g. the 2s refresh tick)
// only repaints the chart when the series array identity actually changes;
// callers should pass memoized arrays.
export const LineChart = memo(function LineChart({
  series,
  ariaLabel,
  times,
}: {
  series: Series[];
  ariaLabel: string;
  // times are the sample instants in epoch milliseconds, one per value.
  times?: number[];
}) {
  // A shared max keeps the lines comparable; the floor of 1 avoids divide-by-zero
  // and gives an empty chart a flat baseline. Rounding it up gives a readable
  // top label, and the lines are scaled to that label so they line up with it.
  const max = niceMax(Math.max(1, ...series.flatMap((line) => line.values)));
  const plotHeight = chartHeight - padding * 2;
  const gridLines = [padding, padding + plotHeight / 2, padding + plotHeight];

  return (
    <div>
      <div className="flex gap-2">
        <div aria-hidden="true" className="flex h-48 w-12 shrink-0 flex-col justify-between text-right text-[10px] tabular-nums text-[var(--muted)]">
          <span>{formatNumber(max)}</span>
          <span>0</span>
        </div>
        <div className="min-w-0 flex-1">
          <svg
            role="img"
            aria-label={ariaLabel}
            viewBox={`0 0 ${chartWidth} ${chartHeight}`}
            preserveAspectRatio="none"
            className="h-48 w-full"
          >
            <rect x={0} y={0} width={chartWidth} height={chartHeight} className="fill-[var(--bg)]" />
            {gridLines.map((y) => (
              <line
                key={y}
                x1={0}
                x2={chartWidth}
                y1={y}
                y2={y}
                className="stroke-[var(--border)]"
                strokeDasharray="4 4"
                vectorEffect="non-scaling-stroke"
              />
            ))}
            {series.map((line) => (
              <polyline
                key={line.label}
                points={linePoints(line.values, max)}
                fill="none"
                stroke={line.color}
                strokeWidth={1.5}
                vectorEffect="non-scaling-stroke"
              />
            ))}
          </svg>
          {times !== undefined && times.length > 1 && (
            <div className="mt-1 flex justify-between text-[10px] tabular-nums text-[var(--muted)]">
              <span>{formatClock(times[0])}</span>
              <span>{formatClock(times[times.length - 1])}</span>
            </div>
          )}
        </div>
      </div>

      <div className="mt-2 flex flex-wrap gap-x-4 gap-y-1 text-xs text-[var(--muted)]">
        {series.map((line) => (
          <span key={line.label} className="inline-flex items-center gap-1.5">
            <span className="inline-block size-2 rounded-sm" style={{ backgroundColor: line.color }} />
            {line.label}
            <span className="tabular-nums text-[var(--text-soft)]">
              {line.values.length > 0 ? line.values[line.values.length - 1] : 0}
            </span>
          </span>
        ))}
      </div>
    </div>
  );
});
