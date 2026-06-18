// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

// Command client enqueues a batch of welcome-email tasks against a
// conveyord node and prints their ids.
//
// Configuration comes from CONVEYOR_ADDR (default http://localhost:8080)
// and CONVEYOR_TOKEN (empty for --dev servers).
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	conveyor "github.com/conveyorq/conveyor/sdks/go"
)

// defaultAddr is the conveyord API address used when CONVEYOR_ADDR is unset.
const defaultAddr = "http://localhost:8080"

// taskCount is how many welcome emails this run enqueues.
const taskCount = 10

// WelcomeEmail is the payload of an email:welcome task.
type WelcomeEmail struct {
	// UserID identifies the recipient.
	UserID int `json:"user_id"`
}

func main() {
	addr := os.Getenv("CONVEYOR_ADDR")
	if addr == "" {
		addr = defaultAddr
	}

	client, err := conveyor.NewClient(addr, conveyor.WithToken(os.Getenv("CONVEYOR_TOKEN")))
	if err != nil {
		log.Fatalf("client: %v", err)
	}

	ctx := context.Background()

	for userID := range taskCount {
		info, err := client.Enqueue(ctx,
			conveyor.NewTask("email:welcome", conveyor.JSON(WelcomeEmail{UserID: userID})),
			conveyor.Retention(time.Hour),
		)
		if err != nil {
			log.Fatalf("enqueue user %d: %v", userID, err)
		}

		fmt.Printf("enqueued %s for user %d (%s)\n", info.ID, userID, info.State)
	}
}
