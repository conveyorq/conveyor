// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

import { create } from "@bufbuild/protobuf";
import { type Duration, DurationSchema, type Timestamp, timestampDate, timestampFromDate } from "@bufbuild/protobuf/wkt";

const MILLIS_PER_SECOND = 1000;
const NANOS_PER_MILLI = 1_000_000;

/** durationFromMs builds a protobuf Duration from a millisecond count. */
export function durationFromMs(ms: number): Duration {
  return create(DurationSchema, {
    seconds: BigInt(Math.trunc(ms / MILLIS_PER_SECOND)),
    nanos: Math.round((ms % MILLIS_PER_SECOND) * NANOS_PER_MILLI),
  });
}

/** durationToMs reads a protobuf Duration as milliseconds; undefined is 0. */
export function durationToMs(duration: Duration | undefined): number {
  if (duration === undefined) {
    return 0;
  }

  return Number(duration.seconds) * MILLIS_PER_SECOND + duration.nanos / NANOS_PER_MILLI;
}

/** dateFromTimestamp converts an optional protobuf Timestamp to a Date. */
export function dateFromTimestamp(timestamp: Timestamp | undefined): Date | undefined {
  return timestamp === undefined ? undefined : timestampDate(timestamp);
}

export { timestampFromDate };
