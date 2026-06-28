// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

// Command transactional enqueues several related tasks atomically with
// EnqueueTx: either every task is committed or none is. It models publishing a
// unit of work — charge, receipt, and ledger entry — that must never be left
// with an orphaned or missing member.
//
// Configuration comes from CONVEYOR_ADDR (default http://localhost:8080)
// and CONVEYOR_TOKEN (empty for --dev servers).
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	conveyor "github.com/conveyorq/conveyor/sdks/go"
)

// defaultAddr is the conveyord API address used when CONVEYOR_ADDR is unset.
const defaultAddr = "http://localhost:8080"

// Order is the payload shared by the tasks of one checkout.
type Order struct {
	// ID identifies the order across its tasks.
	ID string `json:"id"`
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

	order := Order{ID: "order-42"}

	// All three tasks commit together, or none do. They may span queues and
	// carry their own options.
	infos, err := client.EnqueueTx(context.Background(), []conveyor.TxTask{
		conveyor.Tx(conveyor.NewTask("order:charge", conveyor.JSON(order)), conveyor.Queue("billing"), conveyor.Priority(7)),
		conveyor.Tx(conveyor.NewTask("email:receipt", conveyor.JSON(order)), conveyor.Queue("mail")),
		conveyor.Tx(conveyor.NewTask("ledger:post", conveyor.JSON(order))),
	})
	if err != nil {
		log.Fatalf("enqueue tx: %v", err)
	}

	for _, info := range infos {
		fmt.Printf("committed %s (%s) on queue %s\n", info.ID, info.Type, info.Queue)
	}
}
