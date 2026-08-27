// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

//go:build windows

package main

import "os"

// Windows has no SIGUSR1 and no portable user-defined signal, so the manual
// outage toggle is unavailable here. Use --outage-every / --outage-for to drive
// outages on a schedule instead.
var outageToggleSignals []os.Signal
