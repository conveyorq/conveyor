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

// Package cron turns persisted cron entries into materialized tasks: it parses
// quartz cron specs, computes fire times, and builds the task for one fire slot
// with a deterministic per-slot uniqueness key. It holds no actor or scheduling
// behavior — the scheduler decides when to call it.
package cron

import (
	"fmt"
	"time"

	"github.com/reugn/go-quartz/quartz"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/conveyorq/conveyor/internal/broker"
	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
)

// UniqueTTL backstops the per-slot uniqueness claim of a materialized cron
// task. The real guard against a double fire is the deterministic per-slot
// unique key combined with the persisted next-run cursor; this TTL only has to
// outlive the brief window in which a relocating singleton could fire the same
// slot twice. Distinct slots carry distinct keys, so it never blocks the next
// legitimate fire.
const UniqueTTL = time.Minute

// NextFire returns the first time the spec fires strictly after the given
// instant. The spec is the quartz cron format (seconds-first); it is validated
// when the entry is upserted, so a parse error here means a corrupt entry.
func NextFire(spec string, after time.Time) (time.Time, error) {
	trigger, err := quartz.NewCronTrigger(spec)
	if err != nil {
		return time.Time{}, fmt.Errorf("parsing cron spec %q: %w", spec, err)
	}

	next, err := trigger.NextFireTime(after.UnixNano())
	if err != nil {
		return time.Time{}, fmt.Errorf("computing next fire for %q: %w", spec, err)
	}

	return time.Unix(0, next), nil
}

// UniqueKey is the uniqueness key of the task materialized for one fire slot.
// It is deterministic in the entry id and the slot's second, so two schedulers
// racing across a singleton failover produce the same key and the broker admits
// exactly one task per slot.
func UniqueKey(entryID string, slot time.Time) string {
	return fmt.Sprintf("cron:%s:%d", entryID, slot.Unix())
}

// MaterializeTask builds the task an entry produces for one fire slot. The
// entry's execution options carry over, but the uniqueness key is replaced with
// the per-slot key so firing is exactly-once per slot.
func MaterializeTask(entry *broker.CronEntry, slot time.Time, id string) *conveyorv1.TaskEnvelope {
	options := &conveyorv1.TaskOptions{}
	if entry.Options != nil {
		options, _ = proto.Clone(entry.Options).(*conveyorv1.TaskOptions)
	}

	options.UniqueKey = UniqueKey(entry.ID, slot)
	options.UniqueTtl = durationpb.New(UniqueTTL)

	return &conveyorv1.TaskEnvelope{
		Id:          id,
		Queue:       entry.Queue,
		Type:        entry.TaskType,
		Payload:     entry.Payload,
		ContentType: entry.ContentType,
		Options:     options,
	}
}
