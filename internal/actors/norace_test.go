// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

//go:build !race

package actors

// raceEnabled reports whether the test binary was built with the race detector.
// See race_test.go for why the throughput and weighted-drain gates consult it.
const raceEnabled = false
