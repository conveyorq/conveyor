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
//	               [--at RFC3339] [--expires-in DUR] [--expires-at RFC3339]
//	               [--max-retry N] [--priority N] [--retention DUR]
//	               [--unique DUR] [--unique-key KEY] [--encryption-key ID:SECRET]
//	enqueue-tx --file PATH [--encryption-key ID:SECRET]
//	stats
//	queues pause|resume <name>
//	ratelimit set <queue> --rate N [--burst N] | rm <queue> | ls
//	concurrency set <queue> --max N | rm <queue> | ls
//	tasks get <id>
//	tasks list [--queue NAME] [--state STATE] [--limit N] [--page TOKEN]
//	tasks run|cancel|delete <id>
//	cron list | pause <id> | resume <id>
//	cluster info
//	events [--queue NAME]... [--type TYPE]...
//
// The server address and token come from --addr/--token or the
// CONVEYOR_ADDR/CONVEYOR_TOKEN environment variables; flags win. The
// enqueue payload-encryption key comes from --encryption-key or
// CONVEYOR_ENCRYPTION_KEY.
package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/conveyorq/conveyor/encryption"
	conveyor "github.com/conveyorq/conveyor/sdks/go"
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
	// envEncryptionKey supplies the AES-256-GCM key used to encrypt
	// enqueued payloads, in the same "<id>:<base64-secret>" form as
	// --encryption-key.
	envEncryptionKey = "CONVEYOR_ENCRYPTION_KEY"
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

// client builds the SDK client for the enqueue-side commands, applying any
// extra options (e.g. payload encryption) on top of the resolved connection
// settings.
func (c *connection) client(extra ...conveyor.Option) (*conveyor.Client, error) {
	options := append([]conveyor.Option{conveyor.WithToken(c.bearerToken())}, extra...)

	return conveyor.NewClient(c.baseURL(), options...)
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
		newEnqueueTxCommand(conn),
		newTasksCommand(conn),
		newStatsCommand(conn),
		newQueuesCommand(conn),
		newRateLimitCommand(conn),
		newConcurrencyLimitCommand(conn),
		newGroupConfigCommand(conn),
		newCronCommand(conn),
		newWebhooksCommand(conn),
		newClusterCommand(conn),
		newEventsCommand(conn),
	)

	return root
}

// newEnqueueCommand builds the enqueue command.
func newEnqueueCommand(conn *connection) *cobra.Command {
	var (
		queue         string
		payload       string
		taskID        string
		processIn     time.Duration
		processAt     string
		expiresIn     time.Duration
		expiresAt     string
		maxRetry      int
		priority      int
		retention     time.Duration
		unique        time.Duration
		uniqueKey     string
		encryptionKey string
		retryStrategy string
		retryBase     time.Duration
		retryMax      time.Duration
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

			options, err := buildEnqueueOptions(queue, taskID, processAt, expiresAt, uniqueKey, processIn, expiresIn, retention, unique, maxRetry, priority)
			if err != nil {
				return err
			}

			retryOption, err := buildRetryPolicy(retryStrategy, retryBase, retryMax)
			if err != nil {
				return err
			}

			if retryOption != nil {
				options = append(options, retryOption)
			}

			encryptor, err := buildEncryptor(encryptionKey)
			if err != nil {
				return err
			}

			client, err := conn.client(conveyor.WithEncryption(encryptor))
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
	flags.DurationVar(&expiresIn, "expires-in", 0, "archive the task if not dispatched within this duration of enqueue")
	flags.StringVar(&expiresAt, "expires-at", "", "archive the task if not dispatched by this RFC3339 time")
	flags.IntVar(&maxRetry, "max-retry", 0, "retry budget (server default when 0)")
	flags.IntVar(&priority, "priority", 0, "dispatch priority 1..9 (server default when 0)")
	flags.DurationVar(&retention, "retention", 0, "keep the completed task visible for this long")
	flags.DurationVar(&unique, "unique", 0, "reject duplicates of this task for the given TTL")
	flags.StringVar(&uniqueKey, "unique-key", "", "explicit uniqueness key (default: type + payload hash)")
	flags.StringVar(&encryptionKey, "encryption-key", "", `encrypt the payload with AES-256-GCM, as "<id>:<base64-secret>" (default $CONVEYOR_ENCRYPTION_KEY)`)
	flags.StringVar(&retryStrategy, "retry-strategy", "", "retry backoff strategy: exponential|linear|fixed (server default when empty)")
	flags.DurationVar(&retryBase, "retry-base", 0, "first-retry delay ceiling (server default when 0)")
	flags.DurationVar(&retryMax, "retry-max", 0, "overall retry delay cap (server default when 0)")

	return command
}

// txTaskSpec is one task in an enqueue-tx input file. Its fields mirror the
// enqueue flags; duration fields ("in", "expires_in", "retention", "unique")
// accept Go duration strings such as "5m", and time fields ("at", "expires_at")
// accept RFC3339 timestamps.
type txTaskSpec struct {
	// Type is the handler routing key; required.
	Type string `json:"type"`
	// Queue routes the task; empty selects the server default.
	Queue string `json:"queue"`
	// JSON is the task's JSON payload; omitted for an empty payload.
	JSON json.RawMessage `json:"json"`
	// ID is an optional client-assigned id for idempotent retries.
	ID string `json:"id"`
	// In delays execution by a duration, e.g. "5m".
	In string `json:"in"`
	// At delays execution until an RFC3339 time.
	At string `json:"at"`
	// ExpiresIn archives the task if not dispatched within this duration.
	ExpiresIn string `json:"expires_in"`
	// ExpiresAt archives the task if not dispatched by this RFC3339 time.
	ExpiresAt string `json:"expires_at"`
	// MaxRetry is the retry budget; 0 selects the server default.
	MaxRetry int `json:"max_retry"`
	// Priority is the dispatch priority 1..9; 0 selects the server default.
	Priority int `json:"priority"`
	// Retention keeps the completed task visible for this duration.
	Retention string `json:"retention"`
	// Unique rejects duplicates of this task for the given TTL.
	Unique string `json:"unique"`
	// UniqueKey is an explicit uniqueness key.
	UniqueKey string `json:"unique_key"`
}

// newEnqueueTxCommand builds the transactional (atomic) enqueue command. It
// reads a JSON array of task specs from a file and commits them all-or-nothing:
// every task is enqueued or none is.
func newEnqueueTxCommand(conn *connection) *cobra.Command {
	var (
		file          string
		encryptionKey string
	)

	command := &cobra.Command{
		Use:   "enqueue-tx --file <path>",
		Short: "Commit many tasks atomically (all-or-nothing)",
		Example: `  conveyor enqueue-tx --file tasks.json

  where tasks.json is a JSON array:
  [
    {"type": "order:charge",  "queue": "billing", "json": {"id": "order-42"}, "priority": 7},
    {"type": "email:receipt", "queue": "mail",    "json": {"id": "order-42"}},
    {"type": "ledger:post",                        "json": {"id": "order-42"}}
  ]`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			tasks, err := readTxTasks(file)
			if err != nil {
				return err
			}

			encryptor, err := buildEncryptor(encryptionKey)
			if err != nil {
				return err
			}

			client, err := conn.client(conveyor.WithEncryption(encryptor))
			if err != nil {
				return err
			}

			infos, err := client.EnqueueTx(context.Background(), tasks)
			if err != nil {
				return err
			}

			for _, info := range infos {
				fmt.Fprintf(cmd.OutOrStdout(), "enqueued %s (queue=%s, state=%s)\n", info.ID, info.Queue, info.State)
			}

			return nil
		},
	}

	flags := command.Flags()
	flags.StringVar(&file, "file", "", "path to a JSON array of task specs (required)")
	flags.StringVar(&encryptionKey, "encryption-key", "", `encrypt every payload with AES-256-GCM, as "<id>:<base64-secret>" (default $CONVEYOR_ENCRYPTION_KEY)`)
	_ = command.MarkFlagRequired("file")

	return command
}

// readTxTasks reads and validates the enqueue-tx input file into TxTask values,
// mapping each spec through the same task and option builders the single-task
// enqueue command uses.
func readTxTasks(file string) ([]conveyor.TxTask, error) {
	if file == "" {
		return nil, errors.New("enqueue-tx: --file is required")
	}

	raw, err := os.ReadFile(file)
	if err != nil {
		return nil, fmt.Errorf("enqueue-tx: reading %s: %w", file, err)
	}

	var specs []txTaskSpec
	if err := json.Unmarshal(raw, &specs); err != nil {
		return nil, fmt.Errorf("enqueue-tx: %s is not a JSON array of task specs: %w", file, err)
	}

	if len(specs) == 0 {
		return nil, errors.New("enqueue-tx: the task list is empty")
	}

	tasks := make([]conveyor.TxTask, len(specs))

	for index, spec := range specs {
		if spec.Type == "" {
			return nil, fmt.Errorf("enqueue-tx: task %d: a type is required", index)
		}

		task, err := buildTask(spec.Type, string(spec.JSON))
		if err != nil {
			return nil, fmt.Errorf("enqueue-tx: task %d: %w", index, err)
		}

		processIn, err := parseSpecDuration(index, "in", spec.In)
		if err != nil {
			return nil, err
		}

		expiresIn, err := parseSpecDuration(index, "expires_in", spec.ExpiresIn)
		if err != nil {
			return nil, err
		}

		retention, err := parseSpecDuration(index, "retention", spec.Retention)
		if err != nil {
			return nil, err
		}

		unique, err := parseSpecDuration(index, "unique", spec.Unique)
		if err != nil {
			return nil, err
		}

		options, err := buildEnqueueOptions(spec.Queue, spec.ID, spec.At, spec.ExpiresAt, spec.UniqueKey,
			processIn, expiresIn, retention, unique, spec.MaxRetry, spec.Priority)
		if err != nil {
			return nil, fmt.Errorf("enqueue-tx: task %d: %w", index, err)
		}

		tasks[index] = conveyor.Tx(task, options...)
	}

	return tasks, nil
}

// parseSpecDuration parses an optional duration field of one task spec, reporting
// the offending task and field on a malformed value. An empty value is zero.
func parseSpecDuration(index int, field, value string) (time.Duration, error) {
	if value == "" {
		return 0, nil
	}

	parsed, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("enqueue-tx: task %d: parsing %q: %w", index, field, err)
	}

	return parsed, nil
}

// buildRetryPolicy turns the retry flags into an enqueue option, or nil when no
// override was requested. An unknown strategy name is rejected.
func buildRetryPolicy(strategy string, base, maxDelay time.Duration) (conveyor.EnqueueOption, error) {
	if strategy == "" && base == 0 && maxDelay == 0 {
		return nil, nil
	}

	parsed := conveyor.RetryDefault

	switch strategy {
	case "", "default":
		parsed = conveyor.RetryDefault

	case "exponential":
		parsed = conveyor.RetryExponential

	case "linear":
		parsed = conveyor.RetryLinear

	case "fixed":
		parsed = conveyor.RetryFixed

	default:
		return nil, fmt.Errorf("enqueue: --retry-strategy %q is not one of exponential, linear, fixed", strategy)
	}

	return conveyor.RetryPolicy(parsed, base, maxDelay), nil
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
		newTasksRescheduleCommand(conn),
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
func buildEnqueueOptions(queue, taskID, processAt, expiresAt, uniqueKey string, processIn, expiresIn, retention, unique time.Duration, maxRetry, priority int) ([]conveyor.EnqueueOption, error) {
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

	if expiresIn > 0 {
		options = append(options, conveyor.ExpiresIn(expiresIn))
	}

	if expiresAt != "" {
		at, err := time.Parse(time.RFC3339, expiresAt)
		if err != nil {
			return nil, fmt.Errorf("enqueue: parsing --expires-at: %w", err)
		}

		options = append(options, conveyor.ExpiresAt(at))
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

// buildEncryptor resolves the payload-encryption key with flag > environment
// precedence and builds the built-in AES-256-GCM encryptor from it, so the
// enqueued payload is sealed before it leaves the CLI. It returns a nil
// Encryptor — encryption off — when no key is configured.
//
// The key is "<id>:<base64-secret>": an id that labels the ciphertext so a
// worker holding the same id can find the matching secret to decrypt it, and a
// standard-base64-encoded 32-byte AES-256 secret. The id must not contain a
// colon.
func buildEncryptor(key string) (encryption.Encryptor, error) {
	key = firstNonEmpty(key, os.Getenv(envEncryptionKey))
	if key == "" {
		return nil, nil
	}

	id, encodedSecret, found := strings.Cut(key, ":")
	if !found || id == "" || encodedSecret == "" {
		return nil, errors.New(`enqueue: --encryption-key must be "<id>:<base64-secret>"`)
	}

	secret, err := base64.StdEncoding.DecodeString(encodedSecret)
	if err != nil {
		return nil, fmt.Errorf("enqueue: decoding --encryption-key secret: %w", err)
	}

	encryptor, err := encryption.NewAESGCM(id, encryption.Key{ID: id, Secret: secret})
	if err != nil {
		return nil, fmt.Errorf("enqueue: %w", err)
	}

	return encryptor, nil
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
