// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

//go:build !windows

package main

import (
	"os"
	"syscall"
)

// outageToggleSignals are the OS signals that toggle the simulated outage.
// SIGUSR1 is the conventional user-defined signal on Unix.
var outageToggleSignals = []os.Signal{syscall.SIGUSR1}
