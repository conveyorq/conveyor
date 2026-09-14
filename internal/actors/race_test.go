// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

//go:build race

package actors

// raceEnabled reports whether the test binary was built with the race detector.
// The throughput and weighted-drain gates skip under it: race instrumentation
// slows the sync paths roughly tenfold, so their rate targets are unreachable on
// a CI runner no matter the deadline. A nightly non-race job runs them instead.
const raceEnabled = true
