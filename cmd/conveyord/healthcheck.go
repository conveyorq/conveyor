// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/spf13/cobra"
)

const (
	// healthAddrEnv is the environment variable the running server reads its API
	// listen address from; the probe honors it so a non-default port still works.
	healthAddrEnv = "CONVEYOR_API__LISTEN"
	// defaultHealthAddr mirrors the server's default api.listen.
	defaultHealthAddr = ":8080"
	// healthzPath is the liveness endpoint the server serves.
	healthzPath = "/healthz"
	// healthcheckTimeout bounds a single probe.
	healthcheckTimeout = 3 * time.Second
)

// newHealthcheckCommand builds the `conveyord healthcheck` subcommand. It is a
// self-probe for a container HEALTHCHECK: the distroless image ships no shell or
// curl, so the binary checks its own /healthz and exits 0 when healthy, non-zero
// otherwise. It does not load the full configuration (so an incomplete broker
// config never masks a liveness answer); it only needs the API address.
func newHealthcheckCommand() *cobra.Command {
	var addr string

	cmd := &cobra.Command{
		Use:   "healthcheck",
		Short: "Probe this node's /healthz endpoint; exit 0 if healthy",
		Long: `conveyord healthcheck — probe the local node's liveness endpoint.

Intended for a container HEALTHCHECK in the distroless image, which has no shell.
The address is resolved from --addr, else $CONVEYOR_API__LISTEN, else :8080, and
a bind-all address (":8080", "0.0.0.0:8080") is probed over loopback.`,
		Args:          cobra.NoArgs,
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(_ *cobra.Command, _ []string) error {
			return runHealthcheck(resolveHealthAddr(addr, os.Getenv(healthAddrEnv)))
		},
	}

	cmd.Flags().StringVar(&addr, "addr", "", "API address to probe (default: $CONVEYOR_API__LISTEN or :8080)")

	return cmd
}

// resolveHealthAddr picks the address to probe with flag-over-env-over-default
// precedence and rewrites a bind-all host to loopback, since the probe runs on
// the same host as the server.
func resolveHealthAddr(flagAddr, envAddr string) string {
	addr := flagAddr
	if addr == "" {
		addr = envAddr
	}

	if addr == "" {
		addr = defaultHealthAddr
	}

	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}

	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}

	return net.JoinHostPort(host, port)
}

// runHealthcheck issues one GET to the node's /healthz and returns an error
// unless it answers 200.
func runHealthcheck(addr string) error {
	ctx, cancel := context.WithTimeout(context.Background(), healthcheckTimeout)
	defer cancel()

	url := "http://" + addr + healthzPath

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return err
	}

	defer func() { _ = response.Body.Close() }()

	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("health check failed: %s returned status %d", url, response.StatusCode)
	}

	return nil
}
