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
//	tasks get <id>
//
// The server address and token come from --addr/--token or the
// CONVEYOR_ADDR/CONVEYOR_TOKEN environment variables; flags win.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	conveyor "github.com/tochemey/conveyor/sdk"
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

// Command names dispatched by run.
const (
	// commandEnqueue commits one task.
	commandEnqueue = "enqueue"
	// commandTasks groups task inspection subcommands.
	commandTasks = "tasks"
	// subcommandGet fetches one task's state.
	subcommandGet = "get"
)

// usage is the top-level help text.
const usage = `conveyor — command-line client for a Conveyor server

Usage:

  conveyor [--addr URL] [--token TOKEN] <command> [arguments]

Commands:

  enqueue <type>   commit one task, e.g.
                   conveyor enqueue email:welcome --queue critical --json '{"user_id":42}' --in 5m
  tasks get <id>   print the current state of one task

The server address and token come from --addr/--token or the
CONVEYOR_ADDR/CONVEYOR_TOKEN environment variables; flags win.
`

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "conveyor:", err)
		os.Exit(1)
	}
}

// run parses the global flags, builds the API client, and dispatches the
// requested command, writing human-readable output to stdout.
func run(args []string, stdout io.Writer) error {
	globals := flag.NewFlagSet("conveyor", flag.ContinueOnError)
	addr := globals.String("addr", "", "server base URL (default $CONVEYOR_ADDR or "+defaultAddr+")")
	token := globals.String("token", "", "bearer token (default $CONVEYOR_TOKEN)")

	globals.Usage = func() { fmt.Fprint(globals.Output(), usage) }

	if err := globals.Parse(args); err != nil {
		return err
	}

	remaining := globals.Args()
	if len(remaining) == 0 {
		globals.Usage()

		return errors.New("a command is required")
	}

	client, err := conveyor.NewClient(firstNonEmpty(*addr, os.Getenv(envAddr), defaultAddr),
		conveyor.WithToken(firstNonEmpty(*token, os.Getenv(envToken))),
	)
	if err != nil {
		return err
	}

	switch remaining[0] {
	case commandEnqueue:
		return runEnqueue(client, remaining[1:], stdout)

	case commandTasks:
		return runTasks(client, remaining[1:], stdout)

	default:
		return fmt.Errorf("unknown command %q (run conveyor with no arguments for usage)", remaining[0])
	}
}

// runEnqueue commits one task from the command line.
func runEnqueue(client *conveyor.Client, args []string, stdout io.Writer) error {
	if len(args) == 0 || len(args[0]) == 0 || args[0][0] == '-' {
		return errors.New("enqueue: a task type is required, e.g. conveyor enqueue email:welcome --json '{...}'")
	}

	taskType := args[0]

	flags := flag.NewFlagSet(commandEnqueue, flag.ContinueOnError)
	queue := flags.String("queue", "", "target queue (server default when empty)")
	payload := flags.String("json", "", "JSON payload")
	taskID := flags.String("id", "", "client-assigned task id for idempotent retries")
	processIn := flags.Duration("in", 0, "delay execution by duration, e.g. 5m")
	processAt := flags.String("at", "", "delay execution until an RFC3339 time")
	maxRetry := flags.Int("max-retry", 0, "retry budget (server default when 0)")
	priority := flags.Int("priority", 0, "dispatch priority 1..9 (server default when 0)")
	retention := flags.Duration("retention", 0, "keep the completed task visible for this long")
	unique := flags.Duration("unique", 0, "reject duplicates of this task for the given TTL")
	uniqueKey := flags.String("unique-key", "", "explicit uniqueness key (default: type + payload hash)")

	if err := flags.Parse(args[1:]); err != nil {
		return err
	}

	task, err := buildTask(taskType, *payload)
	if err != nil {
		return err
	}

	options, err := buildEnqueueOptions(*queue, *taskID, *processAt, *uniqueKey, *processIn, *retention, *unique, *maxRetry, *priority)
	if err != nil {
		return err
	}

	info, err := client.Enqueue(context.Background(), task, options...)
	if err != nil {
		return err
	}

	fmt.Fprintf(stdout, "enqueued %s (queue=%s, state=%s)\n", info.ID, info.Queue, info.State)

	return nil
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

// runTasks dispatches the task inspection subcommands.
func runTasks(client *conveyor.Client, args []string, stdout io.Writer) error {
	if len(args) == 0 {
		return errors.New("tasks: a subcommand is required, e.g. conveyor tasks get <id>")
	}

	switch args[0] {
	case subcommandGet:
		return runTasksGet(client, args[1:], stdout)

	default:
		return fmt.Errorf("tasks: unknown subcommand %q", args[0])
	}
}

// runTasksGet prints the current state of one task.
func runTasksGet(client *conveyor.Client, args []string, stdout io.Writer) error {
	if len(args) != 1 {
		return errors.New("tasks get: exactly one task id is required")
	}

	info, err := client.GetTask(context.Background(), args[0])
	if err != nil {
		return err
	}

	fmt.Fprintf(stdout, "id:          %s\n", info.ID)
	fmt.Fprintf(stdout, "queue:       %s\n", info.Queue)
	fmt.Fprintf(stdout, "type:        %s\n", info.Type)
	fmt.Fprintf(stdout, "state:       %s\n", info.State)
	fmt.Fprintf(stdout, "priority:    %d\n", info.Priority)
	fmt.Fprintf(stdout, "retried:     %d/%d\n", info.Retried, info.MaxRetry)
	fmt.Fprintf(stdout, "last_error:  %s\n", orDash(info.LastError))
	fmt.Fprintf(stdout, "enqueued_at: %s\n", formatTime(info.EnqueuedAt))
	fmt.Fprintf(stdout, "process_at:  %s\n", formatTime(info.ProcessAt))

	return nil
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
