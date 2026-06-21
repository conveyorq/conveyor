import { memo } from "react";

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
// legend. It is dependency-free: a hand-rolled SVG keeps the bundle small. It is
// memoized so a parent re-render (e.g. the 2s refresh tick) only repaints the
// chart when the series array identity actually changes; callers should pass a
// memoized series array.
export const LineChart = memo(function LineChart({ series, ariaLabel }: { series: Series[]; ariaLabel: string }) {
  // A shared max keeps the lines comparable; the floor of 1 avoids divide-by-zero
  // and gives an empty chart a flat baseline.
  const max = Math.max(1, ...series.flatMap((line) => line.values));

  return (
    <div>
      <svg
        role="img"
        aria-label={ariaLabel}
        viewBox={`0 0 ${chartWidth} ${chartHeight}`}
        preserveAspectRatio="none"
        className="h-48 w-full"
      >
        <rect x={0} y={0} width={chartWidth} height={chartHeight} className="fill-[var(--bg)]" />
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
