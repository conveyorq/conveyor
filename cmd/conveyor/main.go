// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

// Command conveyor is the command-line client of a Conveyor server: a
// thin wrapper over the public API.
//
// Usage:
//
//	conveyor [--addr URL] [--token TOKEN] <command> [arguments]
//
// Commands:
//
//	enqueue <type> [--queue NAME] [--json PAYLOAD] [--id ID] [--in DUR]
//	               [--at RFC3339] [--max-retry N] [--priority N]
//	               [--retention DUR] [--unique DUR] [--unique-key KEY]
//	stats
//	queues pause|resume <name>
//	tasks get <id>
//	tasks list [--queue NAME] [--state STATE] [--limit N] [--page TOKEN]
//	tasks run|cancel|delete <id>
//	cron list | pause <id> | resume <id>
//	cluster info
//
// The server address and token come from --addr/--token or the
// CONVEYOR_ADDR/CONVEYOR_TOKEN environment variables; flags win.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/spf13/cobra"

	conveyor "github.com/conveyorq/conveyor/sdk"
)

// defaultAddr is the server base URL used when neither --addr nor
// CONVEYOR_ADDR is set.
const defaultAddr = "http://localhost:8080"

// Environment variables read for connection settings.
const (
	// envAddr overrides the default server base URL.
	envAddr = "CONVEYOR_ADDR"
	// envToken supplies the bearer token.
	envToken = "CONVEYOR_TOKEN"
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "conveyor:", err)
		os.Exit(1)
	}
}

// run builds a fresh command tree and executes the requested command,
// writing human-readable output to stdout. A fresh tree per invocation
// keeps command state out of package globals, which tests rely on.
func run(args []string, stdout io.Writer) error {
	root := newRootCommand()
	root.SetArgs(args)
	root.SetOut(stdout)

	return root.Execute()
}

// connection carries the resolved server connection settings shared by
// every subcommand.
type connection struct {
	// addr is the --addr flag value; empty falls back to the environment.
	addr string
	// token is the --token flag value; empty falls back to the environment.
	token string
}

// baseURL resolves the server base URL with flag > environment > default
// precedence.
func (c *connection) baseURL() string {
	return firstNonEmpty(c.addr, os.Getenv(envAddr), defaultAddr)
}

// bearerToken resolves the bearer token with flag > environment
// precedence.
func (c *connection) bearerToken() string {
	return firstNonEmpty(c.token, os.Getenv(envToken))
}

// client builds the SDK client for the enqueue-side commands.
func (c *connection) client() (*conveyor.Client, error) {
	return conveyor.NewClient(c.baseURL(), conveyor.WithToken(c.bearerToken()))
}

// newRootCommand assembles the full conveyor command tree.
func newRootCommand() *cobra.Command {
	conn := &connection{}

	root := &cobra.Command{
		Use:   "conveyor",
		Short: "Command-line client for a Conveyor server",
		Long: `conveyor — command-line client for a Conveyor server

The server address and token come from --addr/--token or the
CONVEYOR_ADDR/CONVEYOR_TOKEN environment variables; flags win.`,
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			_ = cmd.Usage()

			return errors.New("a command is required")
		},
	}

	root.PersistentFlags().StringVar(&conn.addr, "addr", "", "server base URL (default $CONVEYOR_ADDR or "+defaultAddr+")")
	root.PersistentFlags().StringVar(&conn.token, "token", "", "bearer token (default $CONVEYOR_TOKEN)")

	root.AddCommand(
		newEnqueueCommand(conn),
		newTasksCommand(conn),
		newStatsCommand(conn),
		newQueuesCommand(conn),
		newCronCommand(conn),
		newClusterCommand(conn),
	)

	return root
}

// newEnqueueCommand builds the enqueue command.
func newEnqueueCommand(conn *connection) *cobra.Command {
	var (
		queue     string
		payload   string
		taskID    string
		processIn time.Duration
		processAt string
		maxRetry  int
		priority  int
		retention time.Duration
		unique    time.Duration
		uniqueKey string
	)

	command := &cobra.Command{
		Use:     "enqueue <type>",
		Short:   "Commit one task",
		Example: `  conveyor enqueue email:welcome --queue critical --json '{"user_id":42}' --in 5m`,
		Args: func(_ *cobra.Command, args []string) error {
			if len(args) != 1 || args[0] == "" {
				return errors.New("enqueue: a task type is required, e.g. conveyor enqueue email:welcome --json '{...}'")
			}

			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			task, err := buildTask(args[0], payload)
			if err != nil {
				return err
			}

			options, err := buildEnqueueOptions(queue, taskID, processAt, uniqueKey, processIn, retention, unique, maxRetry, priority)
			if err != nil {
				return err
			}

			client, err := conn.client()
			if err != nil {
				return err
			}

			info, err := client.Enqueue(context.Background(), task, options...)
			if err != nil {
				return err
			}

			fmt.Fprintf(cmd.OutOrStdout(), "enqueued %s (queue=%s, state=%s)\n", info.ID, info.Queue, info.State)

			return nil
		},
	}

	flags := command.Flags()
	flags.StringVar(&queue, "queue", "", "target queue (server default when empty)")
	flags.StringVar(&payload, "json", "", "JSON payload")
	flags.StringVar(&taskID, "id", "", "client-assigned task id for idempotent retries")
	flags.DurationVar(&processIn, "in", 0, "delay execution by duration, e.g. 5m")
	flags.StringVar(&processAt, "at", "", "delay execution until an RFC3339 time")
	flags.IntVar(&maxRetry, "max-retry", 0, "retry budget (server default when 0)")
	flags.IntVar(&priority, "priority", 0, "dispatch priority 1..9 (server default when 0)")
	flags.DurationVar(&retention, "retention", 0, "keep the completed task visible for this long")
	flags.DurationVar(&unique, "unique", 0, "reject duplicates of this task for the given TTL")
	flags.StringVar(&uniqueKey, "unique-key", "", "explicit uniqueness key (default: type + payload hash)")

	return command
}

// newTasksCommand groups the task inspection and operation subcommands.
func newTasksCommand(conn *connection) *cobra.Command {
	command := &cobra.Command{
		Use:   "tasks",
		Short: "Inspect and operate on tasks",
		RunE: func(cmd *cobra.Command, args []string) error {
			_ = cmd.Usage()

			if len(args) > 0 {
				return fmt.Errorf("tasks: unknown subcommand %q", args[0])
			}

			return errors.New("tasks: a subcommand is required, e.g. conveyor tasks get <id>")
		},
	}

	command.AddCommand(
		newTasksGetCommand(conn),
		newTasksListCommand(conn),
		newTasksRunCommand(conn),
		newTasksCancelCommand(conn),
		newTasksDeleteCommand(conn),
	)

	return command
}

// newTasksGetCommand builds the tasks get subcommand.
func newTasksGetCommand(conn *connection) *cobra.Command {
	return &cobra.Command{
		Use:   "get <id>",
		Short: "Print the current state of one task",
		Args:  exactTaskID("tasks get"),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := conn.client()
			if err != nil {
				return err
			}

			info, err := client.GetTask(context.Background(), args[0])
			if err != nil {
				return err
			}

			stdout := cmd.OutOrStdout()
			_, _ = fmt.Fprintf(stdout, "id:          %s\n", info.ID)
			_, _ = fmt.Fprintf(stdout, "queue:       %s\n", info.Queue)
			_, _ = fmt.Fprintf(stdout, "type:        %s\n", info.Type)
			_, _ = fmt.Fprintf(stdout, "state:       %s\n", info.State)
			_, _ = fmt.Fprintf(stdout, "priority:    %d\n", info.Priority)
			_, _ = fmt.Fprintf(stdout, "retried:     %d/%d\n", info.Retried, info.MaxRetry)
			_, _ = fmt.Fprintf(stdout, "last_error:  %s\n", orDash(info.LastError))
			_, _ = fmt.Fprintf(stdout, "enqueued_at: %s\n", formatTime(info.EnqueuedAt))
			_, _ = fmt.Fprintf(stdout, "process_at:  %s\n", formatTime(info.ProcessAt))

			return nil
		},
	}
}

// exactTaskID validates that exactly one task id argument is present.
func exactTaskID(command string) cobra.PositionalArgs {
	return func(_ *cobra.Command, args []string) error {
		if len(args) != 1 {
			return fmt.Errorf("%s: exactly one task id is required", command)
		}

		return nil
	}
}

// buildTask assembles the enqueueable task from the type and the optional
// JSON payload string.
func buildTask(taskType, payload string) (*conveyor.Task, error) {
	if payload == "" {
		return conveyor.NewTask(taskType, conveyor.Bytes(nil)), nil
	}

	if !json.Valid([]byte(payload)) {
		return nil, errors.New("enqueue: --json payload is not valid JSON")
	}

	return conveyor.NewTask(taskType, conveyor.JSON(json.RawMessage(payload))), nil
}

// buildEnqueueOptions maps the enqueue flags to SDK options, leaving
// server defaults in charge of everything unset.
func buildEnqueueOptions(queue, taskID, processAt, uniqueKey string, processIn, retention, unique time.Duration, maxRetry, priority int) ([]conveyor.EnqueueOption, error) {
	var options []conveyor.EnqueueOption

	if queue != "" {
		options = append(options, conveyor.Queue(queue))
	}

	if taskID != "" {
		options = append(options, conveyor.TaskID(taskID))
	}

	if processIn > 0 {
		options = append(options, conveyor.ProcessIn(processIn))
	}

	if processAt != "" {
		at, err := time.Parse(time.RFC3339, processAt)
		if err != nil {
			return nil, fmt.Errorf("enqueue: parsing --at: %w", err)
		}

		options = append(options, conveyor.ProcessAt(at))
	}

	if maxRetry > 0 {
		options = append(options, conveyor.MaxRetry(maxRetry))
	}

	if priority > 0 {
		options = append(options, conveyor.Priority(priority))
	}

	if retention > 0 {
		options = append(options, conveyor.Retention(retention))
	}

	if unique > 0 {
		options = append(options, conveyor.Unique(unique))
	}

	if uniqueKey != "" {
		options = append(options, conveyor.UniqueKey(uniqueKey))
	}

	return options, nil
}

// firstNonEmpty returns the first non-empty value, encoding the
// flag > environment > default precedence.
func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}

	return ""
}

// orDash substitutes a dash for an empty value in tabular output.
func orDash(value string) string {
	if value == "" {
		return "-"
	}

	return value
}

// formatTime renders a timestamp for tabular output; the zero time is a
// dash.
func formatTime(t time.Time) string {
	if t.IsZero() {
		return "-"
	}

	return t.Local().Format(time.RFC3339)
}
