// QueueSample is one point-in-time snapshot of the cluster-wide task totals,
// summed across every queue. The Metrics view accumulates these as the
// dashboard polls, giving native time-series charts without an external
// metrics store.
export interface QueueSample {
  // time is the sample instant in epoch milliseconds.
  time: number;
  scheduled: number;
  pending: number;
  active: number;
  retry: number;
  completed: number;
  archived: number;
}

// maxSamples bounds the retained history (a rolling window).
const maxSamples = 180;

// samples is the module-level ring so history survives view remounts within a
// session; it resets on a full page reload.
const samples: QueueSample[] = [];

// recordSample appends a sample, drops the oldest past the window, and returns
// a snapshot of the current history.
export function recordSample(sample: QueueSample): QueueSample[] {
  samples.push(sample);

  if (samples.length > maxSamples) {
    samples.shift();
  }

  return samples.slice();
}

// history returns a snapshot of the retained samples.
export function history(): QueueSample[] {
  return samples.slice();
}

// resetHistory clears the retained samples (used by tests).
export function resetHistory(): void {
  samples.length = 0;
}
