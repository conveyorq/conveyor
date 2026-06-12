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

	conveyorv1 "github.com/tochemey/conveyor/internal/proto/conveyor/v1"
	"github.com/tochemey/conveyor/internal/proto/conveyor/v1/conveyorv1connect"
	"github.com/tochemey/conveyor/internal/wire"
)

// taskStatePrefix is stripped from TaskState enum names for CLI input and
// output, mapping TASK_STATE_PENDING to "pending".
const taskStatePrefix = "TASK_STATE_"

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

			return errors.New("cron: usage: conveyor cron list | pause <id> | resume <id>")
		},
	}

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
