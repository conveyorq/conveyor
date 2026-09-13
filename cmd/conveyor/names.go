// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package main

// CLI flag names. Each is defined once here so a flag's registration and every
// later reference to it (a required-flag mark, a lookup) share one spelling.
const (
	flagAddr           = "addr"
	flagToken          = "token"
	flagOutput         = "output"
	flagOutputShort    = "o"
	flagQueue          = "queue"
	flagJSON           = "json"
	flagID             = "id"
	flagIn             = "in"
	flagAt             = "at"
	flagExpiresIn      = "expires-in"
	flagExpiresAt      = "expires-at"
	flagMaxRetry       = "max-retry"
	flagPriority       = "priority"
	flagRetention      = "retention"
	flagUnique         = "unique"
	flagUniqueKey      = "unique-key"
	flagEncryptionKey  = "encryption-key"
	flagRetryStrategy  = "retry-strategy"
	flagRetryBase      = "retry-base"
	flagRetryMax       = "retry-max"
	flagFile           = "file"
	flagState          = "state"
	flagLimit          = "limit"
	flagPage           = "page"
	flagRate           = "rate"
	flagBurst          = "burst"
	flagMax            = "max"
	flagGroup          = "group"
	flagMaxSize        = "max-size"
	flagMaxDelay       = "max-delay"
	flagGrace          = "grace"
	flagType           = "type"
	flagSecret         = "secret"
	flagBatchType      = "batch-type"
	flagConcurrency    = "concurrency"
	flagRequestTimeout = "request-timeout"
	flagPaused         = "paused"
)

// CLI command and subcommand names. Top-level groups first, then the shared
// verbs their subcommands reuse.
const (
	cmdEnqueue     = "enqueue"
	cmdEnqueueTx   = "enqueue-tx"
	cmdTasks       = "tasks"
	cmdStats       = "stats"
	cmdQueues      = "queues"
	cmdRateLimit   = "ratelimit"
	cmdConcurrency = "concurrency"
	cmdGroup       = "group"
	cmdCron        = "cron"
	cmdWebhooks    = "webhooks"
	cmdCluster     = "cluster"
	cmdBroker      = "broker"
	cmdEvents      = "events"

	cmdGet        = "get"
	cmdList       = "list"
	cmdRun        = "run"
	cmdReschedule = "reschedule"
	cmdCancel     = "cancel"
	cmdDelete     = "delete"
	cmdArchive    = "archive"
	cmdSet        = "set"
	cmdRemove     = "rm"
	cmdLs         = "ls"
	cmdAdd        = "add"
	cmdPause      = "pause"
	cmdResume     = "resume"
	cmdInfo       = "info"
	cmdSessions   = "sessions"
)

// Retry-strategy names accepted by the enqueue --retry-strategy flag. They are
// an enumeration vocabulary selected in a switch, so they are named constants
// rather than bare strings.
const (
	retryStrategyDefault     = "default"
	retryStrategyExponential = "exponential"
	retryStrategyLinear      = "linear"
	retryStrategyFixed       = "fixed"
)
