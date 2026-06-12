// Command conveyord runs a Conveyor server node.
//
// Usage:
//
//	conveyord --config=/etc/conveyor/conveyor.yaml [--mode=kubernetes]
//	conveyord --dev    # standalone + in-memory broker + auth off + debug logs
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/tochemey/conveyor/server"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "conveyord:", err)
		os.Exit(1)
	}
}

// run parses flags, loads configuration, and supervises the server until a
// termination signal arrives.
func run() error {
	var (
		configPath = flag.String("config", "", "path to conveyor.yaml")
		mode       = flag.String("mode", "", "deployment mode: standalone | cluster | kubernetes (overrides config)")
		dev        = flag.Bool("dev", false, "development mode: standalone, in-memory broker, auth disabled, debug logs")
	)

	flag.Parse()

	config, err := loadConfig(*configPath, *mode, *dev)
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
