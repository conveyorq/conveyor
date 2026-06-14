// MIT License
//
// Copyright (c) 2026 ConveyorQ
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

// Command conveyord runs a Conveyor server node.
//
// Usage:
//
//	conveyord --config=/etc/conveyor/conveyor.yaml [--mode=kubernetes]
//	conveyord --dev    # standalone + in-memory broker + auth off + debug logs
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/conveyorq/conveyor/server"
)

func main() {
	if err := newRootCommand().Execute(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "conveyord:", err)
		os.Exit(1)
	}
}

// newRootCommand assembles the conveyord command tree: a single root
// command that loads configuration and supervises the server node.
func newRootCommand() *cobra.Command {
	var (
		configPath string
		mode       string
		dev        bool
	)

	root := &cobra.Command{
		Use:   "conveyord",
		Short: "Run a Conveyor server node",
		Long: `conveyord — run a Conveyor server node

The node is configured from --config (a conveyor.yaml file) with --mode
overriding the deployment mode. --dev selects the development preset:
standalone, in-memory broker, auth disabled, and debug logs.`,
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(_ *cobra.Command, _ []string) error {
			return runNode(configPath, mode, dev)
		},
	}

	flags := root.Flags()
	flags.StringVar(&configPath, "config", "", "path to conveyor.yaml")
	flags.StringVar(&mode, "mode", "", "deployment mode: standalone | cluster | kubernetes (overrides config)")
	flags.BoolVar(&dev, "dev", false, "development mode: standalone, in-memory broker, auth disabled, debug logs")

	return root
}

// runNode loads configuration and supervises the server until a
// termination signal arrives.
func runNode(configPath, mode string, dev bool) error {
	config, err := loadConfig(configPath, mode, dev)
	if err != nil {
		return err
	}

	logger := server.NewLogger(config.Log)

	node, err := server.New(config, logger)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := node.Start(ctx); err != nil {
		return err
	}

	<-ctx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), config.Engine.ShutdownTimeout)
	defer cancel()

	return node.Stop(shutdownCtx)
}

// loadConfig resolves the effective configuration with flag precedence:
// --dev swaps the defaults for the dev preset (env still overrides);
// --mode overrides whatever the file or environment chose.
func loadConfig(path, mode string, dev bool) (*server.Config, error) {
	if dev {
		if path != "" || mode != "" {
			return nil, fmt.Errorf("--dev cannot be combined with --config or --mode")
		}

		return server.LoadDevConfig()
	}

	config, err := server.LoadConfig(path)
	if err != nil {
		return nil, err
	}

	if mode != "" {
		config.Mode = mode

		if err := config.Validate(); err != nil {
			return nil, err
		}
	}

	return config, nil
}
