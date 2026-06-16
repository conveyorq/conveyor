// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package api

import "runtime/debug"

// modulePath identifies the Conveyor module in build info, used to resolve the
// running server version for the Hello/Welcome version handshake.
const modulePath = "github.com/conveyorq/conveyor"

// unknownServerVersion is reported when build info carries no module version,
// e.g. a `go run` or test binary. It is not valid semver, so the
// min_server_version gate treats it as "unknown" and never rejects on it.
const unknownServerVersion = "devel"

// serverVersion is this conveyord build. It is resolved once from the binary's
// build info, surfaced to workers in Welcome.server_version, and backs the
// Hello.min_server_version compatibility gate. A variable, not a constant, so
// tests can exercise the gate.
var serverVersion = resolveServerVersion()

// resolveServerVersion reads the Conveyor module version from build info,
// mirroring how the SDK stamps its own version. It returns unknownServerVersion
// when build info is unavailable or carries no version (dev builds, tests).
func resolveServerVersion() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return unknownServerVersion
	}

	if info.Main.Path == modulePath && info.Main.Version != "" {
		return info.Main.Version
	}

	for _, dependency := range info.Deps {
		if dependency.Path == modulePath {
			return dependency.Version
		}
	}

	return unknownServerVersion
}
