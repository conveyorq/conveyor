// Copyright 2026 ConveyorQ
//
// SPDX-License-Identifier: Apache-2.0

package events

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"google.golang.org/protobuf/encoding/protojson"

	"github.com/conveyorq/conveyor/internal/backoff"
	conveyorv1 "github.com/conveyorq/conveyor/internal/proto/conveyor/v1"
)

// Webhook defaults.
const (
	// DefaultWebhookTimeout bounds a single delivery attempt.
	DefaultWebhookTimeout = 10 * time.Second
	// DefaultWebhookMaxRetries is the number of retries after a failed delivery.
	DefaultWebhookMaxRetries = 3
	// webhookBackoffBase is the first-retry backoff ceiling for deliveries.
	webhookBackoffBase = 500 * time.Millisecond
	// webhookBackoffCap bounds the delivery retry backoff.
	webhookBackoffCap = 30 * time.Second
	// retryableStatus is the HTTP status at or above which a response is retried;
	// 4xx responses (except 429) signal a client error retrying will not fix.
	retryableStatus = 500
	// tooManyRequests is the one 4xx status worth retrying (rate limited).
	tooManyRequests = 429
)

// WebhookConfig configures the optional HTTP webhook sink.
type WebhookConfig struct {
	// URL is the endpoint events are POSTed to. Empty disables the sink.
	URL string
	// Timeout bounds one delivery attempt.
	Timeout time.Duration
	// MaxRetries is the number of retries after a failed delivery.
	MaxRetries int
	// Secret, when set, is sent as an Authorization: Bearer header.
	Secret string
	// Filter narrows which events are delivered.
	Filter Filter
}

// Webhook delivers task lifecycle events to a configured HTTP endpoint. It
// subscribes to an EventBus and POSTs each matching event as JSON with retry
// and backoff. Delivery is best-effort: a stalled endpoint only fills this
// sink's bounded buffer, after which events are dropped rather than backing up
// the producer.
type Webhook struct {
	// config is the validated webhook configuration.
	config WebhookConfig
	// client issues the HTTP requests.
	client *http.Client
	// logger reports delivery failures.
	logger *slog.Logger
	// strategy computes retry backoff delays.
	strategy backoff.Strategy
	// cancel unsubscribes from the bus, ending the delivery loop.
	cancel func()
	// stop interrupts an in-flight backoff so Stop returns promptly.
	stop chan struct{}
	// done closes when the delivery loop has exited.
	done chan struct{}
}

// NewWebhook builds a webhook sink for the given configuration and logger.
// Zero Timeout and MaxRetries take their defaults.
func NewWebhook(config WebhookConfig, logger *slog.Logger) *Webhook {
	if config.Timeout <= 0 {
		config.Timeout = DefaultWebhookTimeout
	}

	if config.MaxRetries < 0 {
		config.MaxRetries = DefaultWebhookMaxRetries
	}

	return &Webhook{
		config:   config,
		client:   &http.Client{Timeout: config.Timeout},
		logger:   logger,
		strategy: backoff.New(webhookBackoffBase, webhookBackoffCap),
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
}

// Start subscribes to the bus and begins delivering events in a background
// goroutine. It must be called once.
func (w *Webhook) Start(bus *EventBus) {
	channel, cancel := bus.Subscribe(w.config.Filter)
	w.cancel = cancel

	go w.run(channel)
}

// Stop ends the subscription, interrupts any in-flight retry, and waits for the
// delivery loop to exit. It is safe to call once after Start.
func (w *Webhook) Stop() {
	if w.cancel != nil {
		w.cancel()
	}

	close(w.stop)
	<-w.done
}

// run drains the subscription channel until it is closed, delivering each event.
func (w *Webhook) run(channel <-chan *conveyorv1.TaskEvent) {
	defer close(w.done)

	for event := range channel {
		w.deliver(event)
	}
}

// deliver POSTs one event, retrying on transport errors, 5xx, and 429 up to the
// configured retry budget. A non-retryable response (2xx success, or a 4xx the
// caller cannot fix) ends the attempt; exhausting retries drops the event with a
// warning, the best-effort contract.
func (w *Webhook) deliver(event *conveyorv1.TaskEvent) {
	body, err := protojson.Marshal(event)
	if err != nil {
		w.logger.Warn("webhook event marshal failed; dropped", "task_id", event.GetId(), "error", err)

		return
	}

	for attempt := 0; ; attempt++ {
		retryable, deliverErr := w.post(body)
		if deliverErr == nil {
			return
		}

		if !retryable || attempt >= w.config.MaxRetries {
			w.logger.Warn("webhook delivery failed; event dropped", "task_id", event.GetId(),
				"event_type", event.GetEventType().String(), "attempts", attempt+1, "error", deliverErr)

			return
		}

		select {
		case <-time.After(w.strategy.Delay(int32(attempt))):
		case <-w.stop:
			return
		}
	}
}

// post issues one delivery attempt. It returns whether a failure is worth
// retrying and the failure itself (nil on success).
func (w *Webhook) post(body []byte) (retryable bool, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), w.config.Timeout)
	defer cancel()

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, w.config.URL, bytes.NewReader(body))
	if err != nil {
		return false, err
	}

	request.Header.Set("Content-Type", "application/json")

	if w.config.Secret != "" {
		request.Header.Set("Authorization", "Bearer "+w.config.Secret)
	}

	response, err := w.client.Do(request)
	if err != nil {
		return true, err
	}

	// Drain and close the body so the underlying connection can be reused across
	// deliveries (keep-alive); an undrained body forces a new connection each time.
	defer func() {
		_, _ = io.Copy(io.Discard, response.Body)
		_ = response.Body.Close()
	}()

	if response.StatusCode < 300 {
		return false, nil
	}

	retryable = response.StatusCode >= retryableStatus || response.StatusCode == tooManyRequests

	return retryable, fmt.Errorf("webhook endpoint returned status %d", response.StatusCode)
}
