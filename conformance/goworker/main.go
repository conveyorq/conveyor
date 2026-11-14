// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

// Command goworker is the Go conformance worker for the cross-SDK protocol
// suite (see the conformance package). It is not a usage example: the harness
// spawns it, drives it through one scenario chosen by env vars, and asserts
// protocol behavior by observing the server. Kept deliberately minimal and
// parallel to the TypeScript and Python conformance workers.
//
//	SCENARIO       "sleep" (default): run handlers that sleep SLEEP_MS then succeed.
//	               "bad-hello": construct a worker with an invalid queue weight,
//	               which must be rejected locally (exit 1), never a reconnect loop.
//	SLEEP_MS       handler sleep in milliseconds (sleep scenario).
//	QUEUE          queue to serve (default "default").
//	CONVEYOR_ADDR  server base URL.
//	EXPECTED_SHA256      hex SHA-256 the conformance:large and
//	                     conformance:large-stream payloads must hash to.
//	ENCRYPTION_KEY_ID    with ENCRYPTION_KEY_HEX (a 32-byte key in hex), runs the
//	ENCRYPTION_KEY_HEX   worker with end-to-end AES-256-GCM under that key.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/conveyorq/conveyor/encryption"
	conveyor "github.com/conveyorq/conveyor/sdks/go"
)

func main() {
	addr := envOr("CONVEYOR_ADDR", "http://localhost:8080")
	scenario := envOr("SCENARIO", "sleep")
	queue := envOr("QUEUE", "default")

	sleepMS, _ := strconv.Atoi(os.Getenv("SLEEP_MS"))
	sleepFor := time.Duration(sleepMS) * time.Millisecond

	if scenario == "bad-hello" {
		// A zero weight is invalid; NewWorker must reject it locally.
		_, err := conveyor.NewWorker(addr,
			conveyor.WithQueues(map[string]int{queue: 0}), conveyor.WithConcurrency(1))
		if err != nil {
			os.Exit(1)
		}

		log.Println("bad-hello: an invalid queue weight was accepted")
		os.Exit(2)
	}

	options := []conveyor.Option{conveyor.WithQueues(map[string]int{queue: 1}), conveyor.WithConcurrency(8)}

	if keyHex := os.Getenv("ENCRYPTION_KEY_HEX"); keyHex != "" {
		encryptor, err := aesGCM(os.Getenv("ENCRYPTION_KEY_ID"), keyHex)
		if err != nil {
			log.Fatalf("encryption: %v", err)
		}

		options = append(options, conveyor.WithEncryption(encryptor))
	}

	worker, err := conveyor.NewWorker(addr, options...)
	if err != nil {
		log.Fatalf("worker: %v", err)
	}

	expectedDigest := os.Getenv("EXPECTED_SHA256")

	mux := conveyor.NewMux()

	// Sleep the full duration, ignoring cancellation on purpose: the suite checks
	// that a handler outliving the lease keeps its lease via heartbeats, and that
	// a drain waits for it rather than losing it.
	mux.HandleFunc("conformance:task", func(_ context.Context, _ *conveyor.Task) error {
		time.Sleep(sleepFor)

		return nil
	})

	mux.HandleBatch("conformance:batch", func(_ context.Context, _ []*conveyor.Task) error {
		time.Sleep(sleepFor)

		return nil
	})

	// A large payload must arrive intact, reassembled for a buffered handler
	// and as a stream for a streaming one; a wrong digest archives the task.
	mux.HandleFunc("conformance:large", func(_ context.Context, task *conveyor.Task) error {
		return checkDigest(task.Payload(), expectedDigest)
	})

	mux.HandleStreamFunc("conformance:large-stream", func(_ context.Context, task *conveyor.Task) error {
		payload, err := io.ReadAll(task.PayloadReader())
		if err != nil {
			return err
		}

		return checkDigest(payload, expectedDigest)
	})

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	if err := worker.Run(ctx, mux); err != nil {
		log.Fatalf("worker run: %v", err)
	}
}

// envOr returns the environment variable value, or fallback when it is unset.
func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}

	return fallback
}

// checkDigest fails, without retry, unless payload hashes to the expected
// SHA-256 hex digest.
func checkDigest(payload []byte, expected string) error {
	digest := sha256.Sum256(payload)
	if got := hex.EncodeToString(digest[:]); got != expected {
		return conveyor.SkipRetry(fmt.Errorf("payload digest %s, want %s", got, expected))
	}

	return nil
}

// aesGCM builds the worker's AES-256-GCM encryptor from a key id and a
// hex-encoded 32-byte secret.
func aesGCM(keyID, keyHex string) (encryption.Encryptor, error) {
	secret, err := hex.DecodeString(keyHex)
	if err != nil {
		return nil, err
	}

	return encryption.NewAESGCM(keyID, encryption.Key{ID: keyID, Secret: secret})
}
