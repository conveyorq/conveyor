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
	"google.golang.org/protobuf/types/known/durationpb"

	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
)

// queueWeightSeparator splits a --queue value into its name and weight.
const queueWeightSeparator = "="

// newWebhooksCommand groups the webhook worker subcommands.
func newWebhooksCommand(conn *connection) *cobra.Command {
	command := &cobra.Command{
		Use:   "webhooks",
		Short: "Inspect and control webhook worker registrations",
		RunE: func(cmd *cobra.Command, args []string) error {
			_ = cmd.Usage()

			if len(args) > 0 {
				return fmt.Errorf("webhooks: unknown subcommand %q", args[0])
			}

			return errors.New("webhooks: usage: conveyor webhooks add <name> <url> | list | pause <name> | resume <name> | delete <name>")
		},
	}

	command.AddCommand(newWebhooksAddCommand(conn))

	list := &cobra.Command{
		Use:   "list",
		Short: "List webhook worker registrations",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runWebhooksList(conn, cmd.OutOrStdout())
		},
	}

	pause := &cobra.Command{
		Use:   "pause <name>",
		Short: "Suspend delivery to one registration",
		Args:  exactWebhookName("webhooks pause"),
		RunE: func(cmd *cobra.Command, args []string) error {
			_, err := conn.admin().PauseWebhookWorker(context.Background(), connect.NewRequest(&conveyorv1.PauseWebhookWorkerRequest{Name: args[0]}))
			if err != nil {
				return err
			}

			fmt.Fprintf(cmd.OutOrStdout(), "webhook worker %s paused\n", args[0])

			return nil
		},
	}

	resume := &cobra.Command{
		Use:   "resume <name>",
		Short: "Resume delivery to one paused registration",
		Args:  exactWebhookName("webhooks resume"),
		RunE: func(cmd *cobra.Command, args []string) error {
			_, err := conn.admin().ResumeWebhookWorker(context.Background(), connect.NewRequest(&conveyorv1.ResumeWebhookWorkerRequest{Name: args[0]}))
			if err != nil {
				return err
			}

			fmt.Fprintf(cmd.OutOrStdout(), "webhook worker %s resumed\n", args[0])

			return nil
		},
	}

	remove := &cobra.Command{
		Use:   "delete <name>",
		Short: "Remove one registration",
		Args:  exactWebhookName("webhooks delete"),
		RunE: func(cmd *cobra.Command, args []string) error {
			_, err := conn.admin().DeleteWebhookWorker(context.Background(), connect.NewRequest(&conveyorv1.DeleteWebhookWorkerRequest{Name: args[0]}))
			if err != nil {
				return err
			}

			fmt.Fprintf(cmd.OutOrStdout(), "webhook worker %s deleted\n", args[0])

			return nil
		},
	}

	command.AddCommand(list, pause, resume, remove)

	return command
}

// newWebhooksAddCommand builds the registration create/replace command.
func newWebhooksAddCommand(conn *connection) *cobra.Command {
	var (
		queues         []string
		secrets        []string
		batchTypes     []string
		concurrency    int
		requestTimeout time.Duration
		paused         bool
	)

	command := &cobra.Command{
		Use:   "add <name> <url>",
		Short: "Create or replace a webhook worker registration",
		Example: `  conveyor webhooks add billing-hooks https://hooks.example.com/tasks \
    --queue billing=3 --queue default=1 --secret "$WEBHOOK_SECRET" --concurrency 8`,
		Args: func(_ *cobra.Command, args []string) error {
			if len(args) != 2 {
				return errors.New("webhooks add: usage: conveyor webhooks add <name> <url>")
			}

			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			parsedQueues, err := parseQueueWeights(queues)
			if err != nil {
				return err
			}

			worker := &conveyorv1.WebhookWorker{
				Name:        args[0],
				Url:         args[1],
				Queues:      parsedQueues,
				Concurrency: int32(concurrency),
				Secrets:     secrets,
				BatchTypes:  batchTypes,
				Paused:      paused,
			}

			if requestTimeout > 0 {
				worker.RequestTimeout = durationpb.New(requestTimeout)
			}

			_, err = conn.admin().UpsertWebhookWorker(context.Background(), connect.NewRequest(&conveyorv1.UpsertWebhookWorkerRequest{Worker: worker}))
			if err != nil {
				return err
			}

			fmt.Fprintf(cmd.OutOrStdout(), "webhook worker %s saved\n", args[0])

			return nil
		},
	}

	flags := command.Flags()
	flags.StringArrayVar(&queues, "queue", nil, "served queue as name or name=weight; repeatable")
	flags.StringArrayVar(&secrets, "secret", nil, "delivery-signing secret, newest first; repeatable (two during rotation)")
	flags.StringArrayVar(&batchTypes, "batch-type", nil, "task type delivered as one batch when its group fires; repeatable")
	flags.IntVar(&concurrency, "concurrency", 1, "max in-flight tasks on this endpoint")
	flags.DurationVar(&requestTimeout, "request-timeout", 0, "synchronous delivery timeout (server default when 0)")
	flags.BoolVar(&paused, "paused", false, "register without delivering")

	return command
}

// parseQueueWeights parses repeated --queue values ("name" or "name=weight").
func parseQueueWeights(values []string) (map[string]int32, error) {
	if len(values) == 0 {
		return nil, errors.New("webhooks add: at least one --queue is required")
	}

	queues := make(map[string]int32, len(values))

	for _, value := range values {
		name, weightText, weighted := strings.Cut(value, queueWeightSeparator)

		weight := int32(1)
		if weighted {
			var parsed int
			if _, err := fmt.Sscanf(weightText, "%d", &parsed); err != nil || parsed < 1 {
				return nil, fmt.Errorf("webhooks add: invalid queue weight in %q", value)
			}

			weight = int32(parsed)
		}

		queues[name] = weight
	}

	return queues, nil
}

// exactWebhookName validates the single registration-name argument.
func exactWebhookName(command string) cobra.PositionalArgs {
	return func(_ *cobra.Command, args []string) error {
		if len(args) != 1 {
			return fmt.Errorf("%s: usage: conveyor %s <name>", command, command)
		}

		return nil
	}
}

// runWebhooksList prints every registration; secrets never appear.
func runWebhooksList(conn *connection, stdout io.Writer) error {
	response, err := conn.admin().ListWebhookWorkers(context.Background(), connect.NewRequest(&conveyorv1.ListWebhookWorkersRequest{}))
	if err != nil {
		return err
	}

	table := tabwriter.NewWriter(stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(table, "NAME\tURL\tQUEUES\tCONCURRENCY\tPAUSED")

	for _, worker := range response.Msg.GetWorkers() {
		queues := make([]string, 0, len(worker.GetQueues()))
		for queue, weight := range worker.GetQueues() {
			queues = append(queues, fmt.Sprintf("%s=%d", queue, weight))
		}

		fmt.Fprintf(table, "%s\t%s\t%s\t%d\t%t\n",
			worker.GetName(), worker.GetUrl(), strings.Join(queues, ","), worker.GetConcurrency(), worker.GetPaused())
	}

	return table.Flush()
}
