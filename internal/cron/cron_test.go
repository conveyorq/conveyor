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

package cron_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/conveyorq/conveyor/internal/broker"
	"github.com/conveyorq/conveyor/internal/cron"
	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
)

func TestNextFire(t *testing.T) {
	// Every two seconds; the next fire strictly after :05 is :06.
	after := time.Date(2026, 6, 14, 12, 0, 5, 0, time.UTC)

	next, err := cron.NextFire("*/2 * * * * *", after)
	require.NoError(t, err)
	require.Equal(t, time.Date(2026, 6, 14, 12, 0, 6, 0, time.UTC).Unix(), next.Unix())

	_, err = cron.NextFire("not a cron spec", after)
	require.Error(t, err)
}

func TestUniqueKeyIsDeterministicPerSlot(t *testing.T) {
	slot := time.Unix(1_700_000_000, 0)

	require.Equal(t, cron.UniqueKey("daily", slot), cron.UniqueKey("daily", slot))
	require.NotEqual(t, cron.UniqueKey("daily", slot), cron.UniqueKey("daily", slot.Add(time.Second)))
	require.NotEqual(t, cron.UniqueKey("daily", slot), cron.UniqueKey("hourly", slot))
}

// TestMaterializeTaskStampsSlotUniqueKey is the no-double-fire guard at the
// data level: two materializations of the same entry and slot carry the same
// per-slot unique key (so the broker admits exactly one) while keeping the
// entry's other options.
func TestMaterializeTaskStampsSlotUniqueKey(t *testing.T) {
	entry := &broker.CronEntry{
		ID:          "c1",
		TaskType:    "test:ok",
		Queue:       "cronq",
		Payload:     []byte(`{}`),
		ContentType: "application/json",
		Options:     &conveyorv1.TaskOptions{MaxRetry: 7},
	}
	slot := time.Unix(1_700_000_000, 0)

	first := cron.MaterializeTask(entry, slot, "task-a")
	second := cron.MaterializeTask(entry, slot, "task-b")

	require.Equal(t, first.GetOptions().GetUniqueKey(), second.GetOptions().GetUniqueKey())
	require.Equal(t, "cron:c1:1700000000", first.GetOptions().GetUniqueKey())
	require.Equal(t, int32(7), first.GetOptions().GetMaxRetry(), "entry options must carry over")
	require.Equal(t, "cronq", first.GetQueue())
	require.Equal(t, "test:ok", first.GetType())

	// The entry's options must not be mutated by materialization.
	require.Empty(t, entry.Options.GetUniqueKey())
}
