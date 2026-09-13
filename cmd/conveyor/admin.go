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
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
	"github.com/conveyorq/conveyor/internal/proto/conveyor/v1/conveyorv1connect"
	"github.com/conveyorq/conveyor/internal/wire"
)

// printJSON writes a protobuf message as indented JSON — the --output json
// rendering. It marshals the wire response directly, so the JSON shape follows
// the protos.
func printJSON(w io.Writer, message proto.Message) error {
	marshaled, err := protojson.MarshalOptions{Multiline: true, Indent: "  "}.Marshal(message)
	if err != nil {
		return err
	}

	_, err = fmt.Fprintln(w, string(marshaled))

	return err
}

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
		Use:   cmdStats,
		Short: "Print the per-queue state counts and pause flags",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			response, err := conn.admin().ListQueues(context.Background(), connect.NewRequest(&conveyorv1.ListQueuesRequest{}))
			if err != nil {
				return err
			}

			if conn.jsonOutput() {
				return printJSON(cmd.OutOrStdout(), response.Msg)
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
		Use:   cmdQueues,
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
		Use:   cmdPause + " <queue>",
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
		Use:   cmdResume + " <queue>",
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
		Use:   cmdRateLimit,
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
		Use:     cmdSet + " <queue>",
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
	flags.Float64Var(&rate, flagRate, 0, "sustained dispatch rate in tasks per second (required, > 0)")
	flags.IntVar(&burst, flagBurst, 1, "token-bucket depth: the largest instantaneous burst (>= 1)")
	_ = command.MarkFlagRequired(flagRate)

	return command
}

// newRateLimitRemoveCommand builds the rate-limit rm subcommand.
func newRateLimitRemoveCommand(conn *connection) *cobra.Command {
	return &cobra.Command{
		Use:   cmdRemove + " <queue>",
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
		Use:   cmdLs,
		Short: "List per-queue rate-limit overrides",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			response, err := conn.admin().ListRateLimits(context.Background(), connect.NewRequest(&conveyorv1.ListRateLimitsRequest{}))
			if err != nil {
				return err
			}

			stdout := cmd.OutOrStdout()

			if conn.jsonOutput() {
				return printJSON(stdout, response.Msg)
			}

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
		Use:   cmdConcurrency,
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
		Use:     cmdSet + " <queue>",
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

	command.Flags().IntVar(&maxActive, flagMax, 0, "most tasks sharing a concurrency key that may be active at once (required, >= 1)")
	_ = command.MarkFlagRequired(flagMax)

	return command
}

// newConcurrencyLimitRemoveCommand builds the concurrency rm subcommand.
func newConcurrencyLimitRemoveCommand(conn *connection) *cobra.Command {
	return &cobra.Command{
		Use:   cmdRemove + " <queue>",
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
		Use:   cmdLs,
		Short: "List per-queue concurrency limits",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			response, err := conn.admin().ListConcurrencyLimits(context.Background(), connect.NewRequest(&conveyorv1.ListConcurrencyLimitsRequest{}))
			if err != nil {
				return err
			}

			stdout := cmd.OutOrStdout()

			if conn.jsonOutput() {
				return printJSON(stdout, response.Msg)
			}

			table := tabwriter.NewWriter(stdout, 0, 0, 2, ' ', 0)
			fmt.Fprintln(table, "QUEUE\tMAX/KEY")

			for _, limit := range response.Msg.GetLimits() {
				fmt.Fprintf(table, "%s\t%d\n", limit.GetQueue(), limit.GetMaxActive())
			}

			return table.Flush()
		},
	}
}

// newGroupConfigCommand groups the per-group aggregation-config subcommands.
func newGroupConfigCommand(conn *connection) *cobra.Command {
	command := &cobra.Command{
		Use:   cmdGroup,
		Short: "Set, clear, and list per-group aggregation overrides",
		RunE: func(cmd *cobra.Command, args []string) error {
			_ = cmd.Usage()

			if len(args) > 0 {
				return fmt.Errorf("group: unknown subcommand %q", args[0])
			}

			return errors.New("group: usage: conveyor group set|rm|ls")
		},
	}

	command.AddCommand(newGroupConfigSetCommand(conn), newGroupConfigRemoveCommand(conn), newGroupConfigListCommand(conn))

	return command
}

// newGroupConfigSetCommand builds the group set subcommand. An empty --group
// sets the queue-wide default applied to every group on the queue without its
// own override.
func newGroupConfigSetCommand(conn *connection) *cobra.Command {
	var (
		group       string
		maxSize     int
		maxDelay    time.Duration
		gracePeriod time.Duration
	)

	command := &cobra.Command{
		Use:     cmdSet + " <queue>",
		Short:   "Override a group's aggregation thresholds (max size, max delay, grace period)",
		Example: `  conveyor group set email --group welcome --max-size 20 --max-delay 2m --grace 5s`,
		Args:    exactQueueName("group set"),
		RunE: func(cmd *cobra.Command, args []string) error {
			_, err := conn.admin().SetGroupConfig(context.Background(), connect.NewRequest(&conveyorv1.SetGroupConfigRequest{
				Queue:       args[0],
				Group:       group,
				MaxSize:     int32(maxSize),
				MaxDelay:    durationpb.New(maxDelay),
				GracePeriod: durationpb.New(gracePeriod),
			}))
			if err != nil {
				return err
			}

			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "queue %s group %q set to max-size %d, max-delay %s, grace %s\n",
				args[0], group, maxSize, maxDelay, gracePeriod)

			return nil
		},
	}

	flags := command.Flags()
	flags.StringVar(&group, flagGroup, "", "aggregation group key to configure; empty sets the queue-wide default")
	flags.IntVar(&maxSize, flagMaxSize, 0, "fire the group once this many members accumulate (required, >= 1)")
	flags.DurationVar(&maxDelay, flagMaxDelay, 0, "fire the group this long after its first member (required, > 0)")
	flags.DurationVar(&gracePeriod, flagGrace, 0, "fire the group this long after its most recent member (required, > 0)")
	_ = command.MarkFlagRequired(flagMaxSize)
	_ = command.MarkFlagRequired(flagMaxDelay)
	_ = command.MarkFlagRequired(flagGrace)

	return command
}

// newGroupConfigRemoveCommand builds the group rm subcommand.
func newGroupConfigRemoveCommand(conn *connection) *cobra.Command {
	var group string

	command := &cobra.Command{
		Use:   cmdRemove + " <queue>",
		Short: "Clear a group's override, reverting it to the queue-wide or global default",
		Args:  exactQueueName("group rm"),
		RunE: func(cmd *cobra.Command, args []string) error {
			_, err := conn.admin().DeleteGroupConfig(context.Background(), connect.NewRequest(&conveyorv1.DeleteGroupConfigRequest{
				Queue: args[0],
				Group: group,
			}))
			if err != nil {
				return err
			}

			fmt.Fprintf(cmd.OutOrStdout(), "queue %s group %q override cleared\n", args[0], group)

			return nil
		},
	}

	command.Flags().StringVar(&group, flagGroup, "", "aggregation group key whose override to clear; empty clears the queue-wide default")

	return command
}

// newGroupConfigListCommand builds the group ls subcommand.
func newGroupConfigListCommand(conn *connection) *cobra.Command {
	return &cobra.Command{
		Use:   cmdLs,
		Short: "List per-group aggregation overrides",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			response, err := conn.admin().ListGroupConfigs(context.Background(), connect.NewRequest(&conveyorv1.ListGroupConfigsRequest{}))
			if err != nil {
				return err
			}

			stdout := cmd.OutOrStdout()

			if conn.jsonOutput() {
				return printJSON(stdout, response.Msg)
			}

			table := tabwriter.NewWriter(stdout, 0, 0, 2, ' ', 0)
			fmt.Fprintln(table, "QUEUE\tGROUP\tMAX-SIZE\tMAX-DELAY\tGRACE")

			for _, config := range response.Msg.GetConfigs() {
				fmt.Fprintf(table, "%s\t%s\t%d\t%s\t%s\n", config.GetQueue(), config.GetGroup(),
					config.GetMaxSize(), config.GetMaxDelay().AsDuration(), config.GetGracePeriod().AsDuration())
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
		Use:   cmdList,
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

			if conn.jsonOutput() {
				return printJSON(stdout, response.Msg)
			}

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
	flags.StringVar(&queue, flagQueue, "", "restrict to one queue")
	flags.StringVar(&state, flagState, "", "restrict to one state: scheduled|pending|active|retry|completed|archived|canceled")
	flags.Int32Var(&limit, flagLimit, 0, "page size (server default when 0)")
	flags.StringVar(&pageToken, flagPage, "", "page token from a previous listing")

	return command
}

// taskOperation is one id-addressed admin task call.
type taskOperation func(ctx context.Context, admin conveyorv1connect.AdminServiceClient, id string) error

// taskBatchOperation is the batch form of an id-addressed admin task call; it
// returns the per-id outcomes in request order.
type taskBatchOperation func(ctx context.Context, admin conveyorv1connect.AdminServiceClient, ids []string) (*conveyorv1.BatchTasksResponse, error)

// newTaskOperationCommand builds one of the id-addressed task subcommands
// around the admin calls it performs. It accepts one or more ids: a single id
// runs the unary call, several run the batch call and report the per-id
// outcome.
func newTaskOperationCommand(conn *connection, name, short string, single taskOperation, batch taskBatchOperation) *cobra.Command {
	return &cobra.Command{
		Use:   name + " <id> [<id>...]",
		Short: short,
		Args: func(_ *cobra.Command, args []string) error {
			if len(args) < 1 {
				return fmt.Errorf("tasks %s: at least one task id is required", name)
			}

			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			admin := conn.admin()
			stdout := cmd.OutOrStdout()

			if len(args) == 1 {
				if err := single(context.Background(), admin, args[0]); err != nil {
					return err
				}

				fmt.Fprintf(stdout, "task %s: %s requested\n", args[0], name)

				return nil
			}

			response, err := batch(context.Background(), admin, args)
			if err != nil {
				return err
			}

			return renderBatchResults(conn, stdout, name, response)
		},
	}
}

// renderBatchResults prints the per-id outcome of a batch task operation as a
// table, or as the raw wire response under --output json.
func renderBatchResults(conn *connection, stdout io.Writer, name string, response *conveyorv1.BatchTasksResponse) error {
	if conn.jsonOutput() {
		return printJSON(stdout, response)
	}

	table := tabwriter.NewWriter(stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(table, "ID\tRESULT")

	for _, result := range response.GetResults() {
		outcome := name + " requested"
		if result.GetError() != "" {
			outcome = "error: " + result.GetError()
		}

		fmt.Fprintf(table, "%s\t%s\n", result.GetId(), outcome)
	}

	return table.Flush()
}

// newTasksRunCommand builds the tasks run subcommand.
func newTasksRunCommand(conn *connection) *cobra.Command {
	return newTaskOperationCommand(conn, cmdRun, "Make one or more scheduled or retry tasks due immediately",
		func(ctx context.Context, admin conveyorv1connect.AdminServiceClient, id string) error {
			_, err := admin.RunTask(ctx, connect.NewRequest(&conveyorv1.RunTaskRequest{Id: id}))

			return err
		},
		func(ctx context.Context, admin conveyorv1connect.AdminServiceClient, ids []string) (*conveyorv1.BatchTasksResponse, error) {
			response, err := admin.BatchRunTasks(ctx, connect.NewRequest(&conveyorv1.BatchTasksRequest{Ids: ids}))
			if err != nil {
				return nil, err
			}

			return response.Msg, nil
		})
}

// newTasksRescheduleCommand builds the tasks reschedule subcommand: it moves a
// waiting task's due time, given either as a delay from now (--in) or an
// absolute RFC3339 instant (--at).
func newTasksRescheduleCommand(conn *connection) *cobra.Command {
	var (
		in time.Duration
		at string
	)

	command := &cobra.Command{
		Use:   cmdReschedule + " <id>",
		Short: "Move a scheduled, pending, or retry task's due time",
		Args:  exactTaskID("tasks reschedule"),
		RunE: func(cmd *cobra.Command, args []string) error {
			id := args[0]

			request, err := rescheduleRequest(id, in, at)
			if err != nil {
				return err
			}

			if _, err := conn.admin().RescheduleTask(context.Background(), connect.NewRequest(request)); err != nil {
				return err
			}

			fmt.Fprintf(cmd.OutOrStdout(), "task %s: reschedule requested\n", id)

			return nil
		},
	}

	flags := command.Flags()
	flags.DurationVar(&in, flagIn, 0, "make the task due this duration from now, e.g. 5m")
	flags.StringVar(&at, flagAt, "", "make the task due at an RFC3339 time")

	return command
}

// rescheduleRequest builds the reschedule request from the mutually exclusive
// --in and --at flags; exactly one must be set. A relative delay is sent as
// process_in so the server resolves it against its own clock.
func rescheduleRequest(id string, in time.Duration, at string) (*conveyorv1.RescheduleTaskRequest, error) {
	switch {
	case in > 0 && at != "":
		return nil, errors.New("tasks reschedule: set only one of --in or --at")

	case in > 0:
		return &conveyorv1.RescheduleTaskRequest{Id: id, ProcessIn: durationpb.New(in)}, nil

	case at != "":
		parsed, err := time.Parse(time.RFC3339, at)
		if err != nil {
			return nil, fmt.Errorf("tasks reschedule: parsing --at: %w", err)
		}

		return &conveyorv1.RescheduleTaskRequest{Id: id, ProcessAt: timestamppb.New(parsed)}, nil

	default:
		return nil, errors.New("tasks reschedule: set one of --in or --at")
	}
}

// newTasksCancelCommand builds the tasks cancel subcommand.
func newTasksCancelCommand(conn *connection) *cobra.Command {
	return newTaskOperationCommand(conn, cmdCancel, "Cancel one or more tasks (best-effort for executing tasks)",
		func(ctx context.Context, admin conveyorv1connect.AdminServiceClient, id string) error {
			_, err := admin.CancelTask(ctx, connect.NewRequest(&conveyorv1.CancelTaskRequest{Id: id}))

			return err
		},
		func(ctx context.Context, admin conveyorv1connect.AdminServiceClient, ids []string) (*conveyorv1.BatchTasksResponse, error) {
			response, err := admin.BatchCancelTasks(ctx, connect.NewRequest(&conveyorv1.BatchTasksRequest{Ids: ids}))
			if err != nil {
				return nil, err
			}

			return response.Msg, nil
		})
}

// newTasksDeleteCommand builds the tasks delete subcommand.
func newTasksDeleteCommand(conn *connection) *cobra.Command {
	return newTaskOperationCommand(conn, cmdDelete, "Delete one or more non-active tasks",
		func(ctx context.Context, admin conveyorv1connect.AdminServiceClient, id string) error {
			_, err := admin.DeleteTask(ctx, connect.NewRequest(&conveyorv1.DeleteTaskRequest{Id: id}))

			return err
		},
		func(ctx context.Context, admin conveyorv1connect.AdminServiceClient, ids []string) (*conveyorv1.BatchTasksResponse, error) {
			response, err := admin.BatchDeleteTasks(ctx, connect.NewRequest(&conveyorv1.BatchTasksRequest{Ids: ids}))
			if err != nil {
				return nil, err
			}

			return response.Msg, nil
		})
}

// newTasksArchiveCommand builds the tasks archive subcommand.
func newTasksArchiveCommand(conn *connection) *cobra.Command {
	return newTaskOperationCommand(conn, cmdArchive, "Move one or more tasks to the archive (dead-letter)",
		func(ctx context.Context, admin conveyorv1connect.AdminServiceClient, id string) error {
			_, err := admin.ArchiveTask(ctx, connect.NewRequest(&conveyorv1.ArchiveTaskRequest{Id: id}))

			return err
		},
		func(ctx context.Context, admin conveyorv1connect.AdminServiceClient, ids []string) (*conveyorv1.BatchTasksResponse, error) {
			response, err := admin.BatchArchiveTasks(ctx, connect.NewRequest(&conveyorv1.BatchTasksRequest{Ids: ids}))
			if err != nil {
				return nil, err
			}

			return response.Msg, nil
		})
}

// newCronCommand groups the cron entry subcommands.
func newCronCommand(conn *connection) *cobra.Command {
	command := &cobra.Command{
		Use:   cmdCron,
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
		Use:   cmdList,
		Short: "List cron entries",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runCronList(conn, cmd.OutOrStdout())
		},
	}

	pause := &cobra.Command{
		Use:   cmdPause + " <id>",
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
		Use:   cmdResume + " <id>",
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

	remove := &cobra.Command{
		Use:   cmdDelete + " <id>",
		Short: "Delete one cron entry",
		Args:  exactCronID("cron delete"),
		RunE: func(cmd *cobra.Command, args []string) error {
			_, err := conn.admin().DeleteCron(context.Background(), connect.NewRequest(&conveyorv1.DeleteCronRequest{Id: args[0]}))
			if err != nil {
				return err
			}

			fmt.Fprintf(cmd.OutOrStdout(), "cron entry %s deleted\n", args[0])

			return nil
		},
	}

	command.AddCommand(list, pause, resume, remove)

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
		Use:     cmdAdd + " <id> <spec> <type>",
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
	flags.StringVar(&queue, flagQueue, "", "target queue (server default when empty)")
	flags.StringVar(&payload, flagJSON, "", "JSON payload for materialized tasks")
	flags.IntVar(&priority, flagPriority, 0, "dispatch priority 1..9 (server default when 0)")
	flags.IntVar(&maxRetry, flagMaxRetry, 0, "retry budget (server default when 0)")

	return command
}

// runCronList prints all persisted cron entries.
func runCronList(conn *connection, stdout io.Writer) error {
	response, err := conn.admin().ListCron(context.Background(), connect.NewRequest(&conveyorv1.ListCronRequest{}))
	if err != nil {
		return err
	}

	if conn.jsonOutput() {
		return printJSON(stdout, response.Msg)
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
		Use:   cmdCluster,
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
		Use:   cmdInfo,
		Short: "Print cluster membership",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			response, err := conn.admin().ClusterInfo(context.Background(), connect.NewRequest(&conveyorv1.ClusterInfoRequest{}))
			if err != nil {
				return err
			}

			if conn.jsonOutput() {
				return printJSON(cmd.OutOrStdout(), response.Msg)
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

	sessions := &cobra.Command{
		Use:   cmdSessions,
		Short: "List the worker sessions connected to the reachable node",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			response, err := conn.admin().ListWorkerSessions(context.Background(), connect.NewRequest(&conveyorv1.ListWorkerSessionsRequest{}))
			if err != nil {
				return err
			}

			stdout := cmd.OutOrStdout()

			if conn.jsonOutput() {
				return printJSON(stdout, response.Msg)
			}

			table := tabwriter.NewWriter(stdout, 0, 0, 2, ' ', 0)
			fmt.Fprintln(table, "ID\tQUEUES\tCONCURRENCY\tSDK\tCONNECTED_AT")

			for _, session := range response.Msg.GetSessions() {
				connectedAt := "-"
				if session.GetConnectedAt().IsValid() {
					connectedAt = formatTime(session.GetConnectedAt().AsTime())
				}

				fmt.Fprintf(table, "%s\t%s\t%d\t%s\t%s\n", session.GetId(), strings.Join(session.GetQueues(), ","),
					session.GetConcurrency(), orDash(session.GetSdkVersion()), connectedAt)
			}

			return table.Flush()
		},
	}

	command.AddCommand(info, sessions)

	return command
}

// newBrokerCommand groups the broker inspection subcommands.
func newBrokerCommand(conn *connection) *cobra.Command {
	command := &cobra.Command{
		Use:   cmdBroker,
		Short: "Inspect the storage backend",
		RunE: func(cmd *cobra.Command, args []string) error {
			_ = cmd.Usage()

			if len(args) > 0 {
				return fmt.Errorf("broker: unknown subcommand %q", args[0])
			}

			return errors.New("broker: usage: conveyor broker info")
		},
	}

	info := &cobra.Command{
		Use:   cmdInfo,
		Short: "Print the broker driver and its engine statistics",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			response, err := conn.admin().BrokerInfo(context.Background(), connect.NewRequest(&conveyorv1.BrokerInfoRequest{}))
			if err != nil {
				return err
			}

			stdout := cmd.OutOrStdout()

			if conn.jsonOutput() {
				return printJSON(stdout, response.Msg)
			}

			table := tabwriter.NewWriter(stdout, 0, 0, 2, ' ', 0)
			fmt.Fprintf(table, "driver\t%s\n", response.Msg.GetDriver())

			for key, value := range response.Msg.GetMetrics() {
				fmt.Fprintf(table, "%s\t%s\n", key, value)
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

// taskEventTypePrefix is stripped from TaskEventType enum names for CLI input
// and output, mapping TASK_EVENT_TYPE_LEASED to "leased".
const taskEventTypePrefix = "TASK_EVENT_TYPE_"

// newEventsCommand builds the events tail command: a long-lived subscription to
// the server's lifecycle event stream, printing each transition as it arrives.
func newEventsCommand(conn *connection) *cobra.Command {
	var (
		queues    []string
		typeNames []string
	)

	command := &cobra.Command{
		Use:   cmdEvents,
		Short: "Stream task lifecycle events as they occur (until interrupted)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			eventTypes, err := parseEventTypes(typeNames)
			if err != nil {
				return err
			}

			stream, err := conn.admin().WatchEvents(cmd.Context(), connect.NewRequest(&conveyorv1.WatchEventsRequest{
				Queues:     queues,
				EventTypes: eventTypes,
			}))
			if err != nil {
				return err
			}

			table := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 2, ' ', 0)
			fmt.Fprintln(table, "TIME\tEVENT\tQUEUE\tTYPE\tID\tSTATE\tATTEMPT\tLAST_ERROR")
			_ = table.Flush()

			for stream.Receive() {
				event := stream.Msg()
				fmt.Fprintf(table, "%s\t%s\t%s\t%s\t%s\t%s\t%d\t%s\n",
					event.GetOccurredAt().AsTime().Format(time.RFC3339), eventTypeName(event.GetEventType()),
					event.GetQueue(), event.GetType(), event.GetId(), stateName(event.GetState()),
					event.GetAttempt(), orDash(event.GetLastError()))
				_ = table.Flush()
			}

			return stream.Err()
		},
	}

	flags := command.Flags()
	flags.StringSliceVar(&queues, flagQueue, nil, "restrict to these queues (repeatable)")
	flags.StringSliceVar(&typeNames, flagType, nil,
		"restrict to these event types: enqueued|scheduled|leased|completed|retried|archived|canceled|released (repeatable)")

	return command
}

// parseEventTypes maps CLI event-type names to wire enums; an empty list means
// no filter.
func parseEventTypes(names []string) ([]conveyorv1.TaskEventType, error) {
	eventTypes := make([]conveyorv1.TaskEventType, 0, len(names))

	for _, name := range names {
		enumName := taskEventTypePrefix + strings.ToUpper(name)

		value, ok := conveyorv1.TaskEventType_value[enumName]
		if !ok || value == int32(conveyorv1.TaskEventType_TASK_EVENT_TYPE_UNSPECIFIED) {
			return nil, fmt.Errorf("unknown event type %q (use enqueued|scheduled|leased|completed|retried|archived|canceled|released)", name)
		}

		eventTypes = append(eventTypes, conveyorv1.TaskEventType(value))
	}

	return eventTypes, nil
}

// eventTypeName renders a wire event type as the CLI's lowercase name.
func eventTypeName(eventType conveyorv1.TaskEventType) string {
	return strings.ToLower(strings.TrimPrefix(eventType.String(), taskEventTypePrefix))
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
