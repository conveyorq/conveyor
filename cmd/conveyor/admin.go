// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
	"github.com/conveyorq/conveyor/internal/proto/conveyor/v1/conveyorv1connect"
	"github.com/conveyorq/conveyor/internal/wire"
)

// taskStatePrefix is stripped from TaskState enum names for CLI input and
// output, mapping TASK_STATE_PENDING to "pending".
const taskStatePrefix = "TASK_STATE_"

// jsonContentType is the content type stamped on a cron entry carrying a
// JSON payload from the CLI.
const jsonContentType = "application/json"

// admin builds the CLI's direct line to the AdminService. Admin
// operations are intentionally not part of the public SDK surface, so the
// CLI speaks the wire protocol itself.
func (c *connection) admin() conveyorv1connect.AdminServiceClient {
	var options []connect.ClientOption
	if token := c.bearerToken(); token != "" {
		options = append(options, connect.WithInterceptors(wire.NewBearerInterceptor(token)))
	}

	return conveyorv1connect.NewAdminServiceClient(wire.NewH2CClient(), c.baseURL(), options...)
}

// newStatsCommand builds the stats command.
func newStatsCommand(conn *connection) *cobra.Command {
	return &cobra.Command{
		Use:   "stats",
		Short: "Print the per-queue state counts and pause flags",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			response, err := conn.admin().ListQueues(context.Background(), connect.NewRequest(&conveyorv1.ListQueuesRequest{}))
			if err != nil {
				return err
			}

			table := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 2, ' ', 0)
			fmt.Fprintln(table, "QUEUE\tPAUSED\tSCHEDULED\tPENDING\tACTIVE\tRETRY\tCOMPLETED\tARCHIVED")

			for _, queue := range response.Msg.GetQueues() {
				fmt.Fprintf(table, "%s\t%t\t%d\t%d\t%d\t%d\t%d\t%d\n",
					queue.GetName(), queue.GetPaused(), queue.GetScheduled(), queue.GetPending(),
					queue.GetActive(), queue.GetRetry(), queue.GetCompleted(), queue.GetArchived())
			}

			return table.Flush()
		},
	}
}

// newQueuesCommand groups the queue pause and resume subcommands.
func newQueuesCommand(conn *connection) *cobra.Command {
	command := &cobra.Command{
		Use:   "queues",
		Short: "Pause and resume queues",
		RunE: func(cmd *cobra.Command, args []string) error {
			_ = cmd.Usage()

			if len(args) > 0 {
				return fmt.Errorf("queues: unknown subcommand %q", args[0])
			}

			return errors.New("queues: usage: conveyor queues pause|resume <queue>")
		},
	}

	pause := &cobra.Command{
		Use:   "pause <queue>",
		Short: "Stop dispatching a queue (queued work stays durable)",
		Args:  exactQueueName("queues pause"),
		RunE: func(cmd *cobra.Command, args []string) error {
			_, err := conn.admin().PauseQueue(context.Background(), connect.NewRequest(&conveyorv1.PauseQueueRequest{Queue: args[0]}))
			if err != nil {
				return err
			}

			fmt.Fprintf(cmd.OutOrStdout(), "queue %s paused\n", args[0])

			return nil
		},
	}

	resume := &cobra.Command{
		Use:   "resume <queue>",
		Short: "Resume dispatching a queue",
		Args:  exactQueueName("queues resume"),
		RunE: func(cmd *cobra.Command, args []string) error {
			_, err := conn.admin().ResumeQueue(context.Background(), connect.NewRequest(&conveyorv1.ResumeQueueRequest{Queue: args[0]}))
			if err != nil {
				return err
			}

			fmt.Fprintf(cmd.OutOrStdout(), "queue %s resumed\n", args[0])

			return nil
		},
	}

	command.AddCommand(pause, resume)

	return command
}

// newRateLimitCommand groups the per-queue dispatch rate-limit subcommands.
func newRateLimitCommand(conn *connection) *cobra.Command {
	command := &cobra.Command{
		Use:   "ratelimit",
		Short: "Set, clear, and list per-queue dispatch rate limits",
		RunE: func(cmd *cobra.Command, args []string) error {
			_ = cmd.Usage()

			if len(args) > 0 {
				return fmt.Errorf("ratelimit: unknown subcommand %q", args[0])
			}

			return errors.New("ratelimit: usage: conveyor ratelimit set|rm|ls")
		},
	}

	command.AddCommand(newRateLimitSetCommand(conn), newRateLimitRemoveCommand(conn), newRateLimitListCommand(conn))

	return command
}

// newRateLimitSetCommand builds the rate-limit set subcommand.
func newRateLimitSetCommand(conn *connection) *cobra.Command {
	var (
		rate  float64
		burst int
	)

	command := &cobra.Command{
		Use:     "set <queue>",
		Short:   "Limit a queue to rate tasks/second with a burst allowance",
		Example: `  conveyor ratelimit set email --rate 50 --burst 10`,
		Args:    exactQueueName("ratelimit set"),
		RunE: func(cmd *cobra.Command, args []string) error {
			_, err := conn.admin().SetQueueRateLimit(context.Background(), connect.NewRequest(&conveyorv1.SetQueueRateLimitRequest{
				Queue:      args[0],
				RatePerSec: rate,
				Burst:      int32(burst),
			}))
			if err != nil {
				return err
			}

			fmt.Fprintf(cmd.OutOrStdout(), "queue %s limited to %g/s (burst %d)\n", args[0], rate, burst)

			return nil
		},
	}

	flags := command.Flags()
	flags.Float64Var(&rate, "rate", 0, "sustained dispatch rate in tasks per second (required, > 0)")
	flags.IntVar(&burst, "burst", 1, "token-bucket depth: the largest instantaneous burst (>= 1)")
	_ = command.MarkFlagRequired("rate")

	return command
}

// newRateLimitRemoveCommand builds the rate-limit rm subcommand.
func newRateLimitRemoveCommand(conn *connection) *cobra.Command {
	return &cobra.Command{
		Use:   "rm <queue>",
		Short: "Clear a queue's override, reverting it to the global default",
		Args:  exactQueueName("ratelimit rm"),
		RunE: func(cmd *cobra.Command, args []string) error {
			_, err := conn.admin().DeleteQueueRateLimit(context.Background(), connect.NewRequest(&conveyorv1.DeleteQueueRateLimitRequest{Queue: args[0]}))
			if err != nil {
				return err
			}

			fmt.Fprintf(cmd.OutOrStdout(), "queue %s rate limit cleared\n", args[0])

			return nil
		},
	}
}

// newRateLimitListCommand builds the rate-limit ls subcommand.
func newRateLimitListCommand(conn *connection) *cobra.Command {
	return &cobra.Command{
		Use:   "ls",
		Short: "List per-queue rate-limit overrides",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			response, err := conn.admin().ListRateLimits(context.Background(), connect.NewRequest(&conveyorv1.ListRateLimitsRequest{}))
			if err != nil {
				return err
			}

			stdout := cmd.OutOrStdout()
			table := tabwriter.NewWriter(stdout, 0, 0, 2, ' ', 0)
			fmt.Fprintln(table, "QUEUE\tRATE/S\tBURST")

			for _, limit := range response.Msg.GetLimits() {
				fmt.Fprintf(table, "%s\t%g\t%d\n", limit.GetQueue(), limit.GetRatePerSec(), limit.GetBurst())
			}

			return table.Flush()
		},
	}
}

// newConcurrencyLimitCommand groups the per-queue concurrency-limit subcommands.
func newConcurrencyLimitCommand(conn *connection) *cobra.Command {
	command := &cobra.Command{
		Use:   "concurrency",
		Short: "Set, clear, and list per-queue per-key concurrency limits",
		RunE: func(cmd *cobra.Command, args []string) error {
			_ = cmd.Usage()

			if len(args) > 0 {
				return fmt.Errorf("concurrency: unknown subcommand %q", args[0])
			}

			return errors.New("concurrency: usage: conveyor concurrency set|rm|ls")
		},
	}

	command.AddCommand(newConcurrencyLimitSetCommand(conn), newConcurrencyLimitRemoveCommand(conn), newConcurrencyLimitListCommand(conn))

	return command
}

// newConcurrencyLimitSetCommand builds the concurrency set subcommand.
func newConcurrencyLimitSetCommand(conn *connection) *cobra.Command {
	var maxActive int

	command := &cobra.Command{
		Use:     "set <queue>",
		Short:   "Limit a queue to max-active tasks per concurrency key",
		Example: `  conveyor concurrency set email --max 5`,
		Args:    exactQueueName("concurrency set"),
		RunE: func(cmd *cobra.Command, args []string) error {
			_, err := conn.admin().SetQueueConcurrencyLimit(context.Background(), connect.NewRequest(&conveyorv1.SetQueueConcurrencyLimitRequest{
				Queue:     args[0],
				MaxActive: int32(maxActive),
			}))
			if err != nil {
				return err
			}

			fmt.Fprintf(cmd.OutOrStdout(), "queue %s limited to %d active per concurrency key\n", args[0], maxActive)

			return nil
		},
	}

	command.Flags().IntVar(&maxActive, "max", 0, "most tasks sharing a concurrency key that may be active at once (required, >= 1)")
	_ = command.MarkFlagRequired("max")

	return command
}

// newConcurrencyLimitRemoveCommand builds the concurrency rm subcommand.
func newConcurrencyLimitRemoveCommand(conn *connection) *cobra.Command {
	return &cobra.Command{
		Use:   "rm <queue>",
		Short: "Clear a queue's concurrency limit, leaving its keys unbounded",
		Args:  exactQueueName("concurrency rm"),
		RunE: func(cmd *cobra.Command, args []string) error {
			_, err := conn.admin().DeleteQueueConcurrencyLimit(context.Background(), connect.NewRequest(&conveyorv1.DeleteQueueConcurrencyLimitRequest{Queue: args[0]}))
			if err != nil {
				return err
			}

			fmt.Fprintf(cmd.OutOrStdout(), "queue %s concurrency limit cleared\n", args[0])

			return nil
		},
	}
}

// newConcurrencyLimitListCommand builds the concurrency ls subcommand.
func newConcurrencyLimitListCommand(conn *connection) *cobra.Command {
	return &cobra.Command{
		Use:   "ls",
		Short: "List per-queue concurrency limits",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			response, err := conn.admin().ListConcurrencyLimits(context.Background(), connect.NewRequest(&conveyorv1.ListConcurrencyLimitsRequest{}))
			if err != nil {
				return err
			}

			stdout := cmd.OutOrStdout()
			table := tabwriter.NewWriter(stdout, 0, 0, 2, ' ', 0)
			fmt.Fprintln(table, "QUEUE\tMAX/KEY")

			for _, limit := range response.Msg.GetLimits() {
				fmt.Fprintf(table, "%s\t%d\n", limit.GetQueue(), limit.GetMaxActive())
			}

			return table.Flush()
		},
	}
}

// newTasksListCommand builds the tasks list subcommand.
func newTasksListCommand(conn *connection) *cobra.Command {
	var (
		queue     string
		state     string
		limit     int32
		pageToken string
	)

	command := &cobra.Command{
		Use:   "list",
		Short: "List tasks, newest first",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			taskState, err := parseTaskState(state)
			if err != nil {
				return err
			}

			response, err := conn.admin().ListTasks(context.Background(), connect.NewRequest(&conveyorv1.ListTasksRequest{
				Queue:     queue,
				State:     taskState,
				Limit:     limit,
				PageToken: pageToken,
			}))
			if err != nil {
				return err
			}

			stdout := cmd.OutOrStdout()
			table := tabwriter.NewWriter(stdout, 0, 0, 2, ' ', 0)
			fmt.Fprintln(table, "ID\tTYPE\tQUEUE\tSTATE\tRETRIED\tLAST_ERROR")

			for _, task := range response.Msg.GetTasks() {
				fmt.Fprintf(table, "%s\t%s\t%s\t%s\t%d/%d\t%s\n",
					task.GetId(), task.GetType(), task.GetQueue(), stateName(task.GetState()),
					task.GetRetried(), task.GetMaxRetry(), orDash(task.GetLastError()))
			}

			if err := table.Flush(); err != nil {
				return err
			}

			if token := response.Msg.GetNextPageToken(); token != "" {
				fmt.Fprintf(stdout, "\nnext page: conveyor tasks list --page %s\n", token)
			}

			return nil
		},
	}

	flags := command.Flags()
	flags.StringVar(&queue, "queue", "", "restrict to one queue")
	flags.StringVar(&state, "state", "", "restrict to one state: scheduled|pending|active|retry|completed|archived|canceled")
	flags.Int32Var(&limit, "limit", 0, "page size (server default when 0)")
	flags.StringVar(&pageToken, "page", "", "page token from a previous listing")

	return command
}

// taskOperation is one id-addressed admin task call.
type taskOperation func(ctx context.Context, admin conveyorv1connect.AdminServiceClient, id string) error

// newTaskOperationCommand builds one of the id-addressed task
// subcommands around the admin call it performs.
func newTaskOperationCommand(conn *connection, name, short string, operation taskOperation) *cobra.Command {
	return &cobra.Command{
		Use:   name + " <id>",
		Short: short,
		Args:  exactTaskID("tasks " + name),
		RunE: func(cmd *cobra.Command, args []string) error {
			id := args[0]

			if err := operation(context.Background(), conn.admin(), id); err != nil {
				return err
			}

			fmt.Fprintf(cmd.OutOrStdout(), "task %s: %s requested\n", id, name)

			return nil
		},
	}
}

// newTasksRunCommand builds the tasks run subcommand.
func newTasksRunCommand(conn *connection) *cobra.Command {
	return newTaskOperationCommand(conn, "run", "Make a scheduled or retry task due immediately",
		func(ctx context.Context, admin conveyorv1connect.AdminServiceClient, id string) error {
			_, err := admin.RunTask(ctx, connect.NewRequest(&conveyorv1.RunTaskRequest{Id: id}))

			return err
		})
}

// newTasksCancelCommand builds the tasks cancel subcommand.
func newTasksCancelCommand(conn *connection) *cobra.Command {
	return newTaskOperationCommand(conn, "cancel", "Cancel a task (best-effort for executing tasks)",
		func(ctx context.Context, admin conveyorv1connect.AdminServiceClient, id string) error {
			_, err := admin.CancelTask(ctx, connect.NewRequest(&conveyorv1.CancelTaskRequest{Id: id}))

			return err
		})
}

// newTasksDeleteCommand builds the tasks delete subcommand.
func newTasksDeleteCommand(conn *connection) *cobra.Command {
	return newTaskOperationCommand(conn, "delete", "Delete a non-active task",
		func(ctx context.Context, admin conveyorv1connect.AdminServiceClient, id string) error {
			_, err := admin.DeleteTask(ctx, connect.NewRequest(&conveyorv1.DeleteTaskRequest{Id: id}))

			return err
		})
}

// newCronCommand groups the cron entry subcommands.
func newCronCommand(conn *connection) *cobra.Command {
	command := &cobra.Command{
		Use:   "cron",
		Short: "Inspect and control cron entries",
		RunE: func(cmd *cobra.Command, args []string) error {
			_ = cmd.Usage()

			if len(args) > 0 {
				return fmt.Errorf("cron: unknown subcommand %q", args[0])
			}

			return errors.New("cron: usage: conveyor cron add <id> <spec> <type> | list | pause <id> | resume <id> | delete <id>")
		},
	}

	command.AddCommand(newCronAddCommand(conn))

	list := &cobra.Command{
		Use:   "list",
		Short: "List cron entries",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runCronList(conn, cmd.OutOrStdout())
		},
	}

	pause := &cobra.Command{
		Use:   "pause <id>",
		Short: "Suspend one cron entry",
		Args:  exactCronID("cron pause"),
		RunE: func(cmd *cobra.Command, args []string) error {
			_, err := conn.admin().PauseCron(context.Background(), connect.NewRequest(&conveyorv1.PauseCronRequest{Id: args[0]}))
			if err != nil {
				return err
			}

			fmt.Fprintf(cmd.OutOrStdout(), "cron entry %s paused\n", args[0])

			return nil
		},
	}

	resume := &cobra.Command{
		Use:   "resume <id>",
		Short: "Resume one cron entry",
		Args:  exactCronID("cron resume"),
		RunE: func(cmd *cobra.Command, args []string) error {
			_, err := conn.admin().ResumeCron(context.Background(), connect.NewRequest(&conveyorv1.ResumeCronRequest{Id: args[0]}))
			if err != nil {
				return err
			}

			fmt.Fprintf(cmd.OutOrStdout(), "cron entry %s resumed\n", args[0])

			return nil
		},
	}

	command.AddCommand(list, pause, resume)

	return command
}

// newCronAddCommand builds the cron entry create/replace command.
func newCronAddCommand(conn *connection) *cobra.Command {
	var (
		queue    string
		payload  string
		priority int
		maxRetry int
	)

	command := &cobra.Command{
		Use:     "add <id> <spec> <type>",
		Short:   "Create or replace a cron entry",
		Example: `  conveyor cron add nightly-report "0 0 2 * * *" report:daily --queue reports`,
		Args: func(_ *cobra.Command, args []string) error {
			if len(args) != 3 {
				return errors.New("cron add: usage: conveyor cron add <id> <spec> <type>")
			}

			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			entry := &conveyorv1.CronEntry{
				Id:       args[0],
				Spec:     args[1],
				TaskType: args[2],
				Queue:    queue,
				Options:  &conveyorv1.TaskOptions{MaxRetry: int32(maxRetry), Priority: int32(priority)},
			}

			if payload != "" {
				entry.Payload = []byte(payload)
				entry.ContentType = jsonContentType
			}

			_, err := conn.admin().UpsertCron(context.Background(), connect.NewRequest(&conveyorv1.UpsertCronRequest{Entry: entry}))
			if err != nil {
				return err
			}

			fmt.Fprintf(cmd.OutOrStdout(), "cron entry %s saved\n", args[0])

			return nil
		},
	}

	flags := command.Flags()
	flags.StringVar(&queue, "queue", "", "target queue (server default when empty)")
	flags.StringVar(&payload, "json", "", "JSON payload for materialized tasks")
	flags.IntVar(&priority, "priority", 0, "dispatch priority 1..9 (server default when 0)")
	flags.IntVar(&maxRetry, "max-retry", 0, "retry budget (server default when 0)")

	return command
}

// runCronList prints all persisted cron entries.
func runCronList(conn *connection, stdout io.Writer) error {
	response, err := conn.admin().ListCron(context.Background(), connect.NewRequest(&conveyorv1.ListCronRequest{}))
	if err != nil {
		return err
	}

	table := tabwriter.NewWriter(stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(table, "ID\tSPEC\tTYPE\tQUEUE\tPAUSED")

	for _, entry := range response.Msg.GetEntries() {
		fmt.Fprintf(table, "%s\t%s\t%s\t%s\t%t\n",
			entry.GetId(), entry.GetSpec(), entry.GetTaskType(), entry.GetQueue(), entry.GetPaused())
	}

	return table.Flush()
}

// newClusterCommand groups the cluster inspection subcommands.
func newClusterCommand(conn *connection) *cobra.Command {
	command := &cobra.Command{
		Use:   "cluster",
		Short: "Inspect cluster membership",
		RunE: func(cmd *cobra.Command, args []string) error {
			_ = cmd.Usage()

			if len(args) > 0 {
				return fmt.Errorf("cluster: unknown subcommand %q", args[0])
			}

			return errors.New("cluster: usage: conveyor cluster info")
		},
	}

	info := &cobra.Command{
		Use:   "info",
		Short: "Print cluster membership",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			response, err := conn.admin().ClusterInfo(context.Background(), connect.NewRequest(&conveyorv1.ClusterInfoRequest{}))
			if err != nil {
				return err
			}

			table := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 2, ' ', 0)
			fmt.Fprintln(table, "ADDRESS\tSTARTED_AT")

			for _, node := range response.Msg.GetNodes() {
				startedAt := "-"
				if node.GetStartedAt().IsValid() {
					startedAt = formatTime(node.GetStartedAt().AsTime())
				}

				fmt.Fprintf(table, "%s\t%s\n", node.GetAddress(), startedAt)
			}

			return table.Flush()
		},
	}

	command.AddCommand(info)

	return command
}

// exactQueueName validates that exactly one queue name argument is
// present.
func exactQueueName(command string) cobra.PositionalArgs {
	return func(_ *cobra.Command, args []string) error {
		if len(args) != 1 {
			return fmt.Errorf("%s: exactly one queue name is required", command)
		}

		return nil
	}
}

// exactCronID validates that exactly one cron entry id argument is
// present.
func exactCronID(command string) cobra.PositionalArgs {
	return func(_ *cobra.Command, args []string) error {
		if len(args) != 1 {
			return fmt.Errorf("%s: exactly one entry id is required", command)
		}

		return nil
	}
}

// parseTaskState maps a CLI state name to the wire enum; empty means no
// filter.
func parseTaskState(state string) (conveyorv1.TaskState, error) {
	if state == "" {
		return conveyorv1.TaskState_TASK_STATE_UNSPECIFIED, nil
	}

	enumName := taskStatePrefix + strings.ToUpper(state)
	if value, ok := conveyorv1.TaskState_value[enumName]; ok && value != int32(conveyorv1.TaskState_TASK_STATE_UNSPECIFIED) {
		return conveyorv1.TaskState(value), nil
	}

	return conveyorv1.TaskState_TASK_STATE_UNSPECIFIED,
		fmt.Errorf("unknown state %q (use scheduled|pending|active|retry|completed|archived|canceled)", state)
}

// stateName renders a wire task state as the CLI's lowercase name.
func stateName(state conveyorv1.TaskState) string {
	return strings.ToLower(strings.TrimPrefix(state.String(), taskStatePrefix))
}
