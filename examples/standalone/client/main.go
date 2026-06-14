// MIT License
//
// Copyright (c) 2026 ConveyorQ
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

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

	conveyor "github.com/conveyorq/conveyor/sdk"
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
