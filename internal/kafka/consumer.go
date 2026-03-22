package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/2beens/simple-go-service/internal/outbound"
	segkafka "github.com/segmentio/kafka-go"
)

const (
	// OutboundPaymentsTopic is the Kafka topic we consume for outbound payment requests.
	OutboundPaymentsTopic = "outbound.payments"
	// ConsumerGroupID is the consumer group for the outbound payments consumer.
	ConsumerGroupID = "simple-go-service-outbound-payments"
	// HeaderIdempotencyKey is the message header key for the idempotency key (UUID).
	HeaderIdempotencyKey = "idempotency-key"
)

// Consumer consumes outbound payment messages and delegates to a handler.
type Consumer struct {
	reader *segkafka.Reader
	log    *slog.Logger
}

// ConsumerConfig holds Kafka consumer configuration.
type ConsumerConfig struct {
	Brokers []string
	GroupID string
	Topic   string
}

// NewConsumer creates a new Kafka consumer for outbound.payments.
func NewConsumer(cfg ConsumerConfig) *Consumer {
	groupID := cfg.GroupID
	if groupID == "" {
		groupID = ConsumerGroupID
	}
	topic := cfg.Topic
	if topic == "" {
		topic = OutboundPaymentsTopic
	}
	r := segkafka.NewReader(segkafka.ReaderConfig{
		Brokers: cfg.Brokers,
		GroupID: groupID,
		Topic:   topic,
	})
	return &Consumer{
		reader: r,
		log:    slog.Default().With("component", "kafka.consumer"),
	}
}

func getHeader(msg segkafka.Message, key string) string {
	for _, h := range msg.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

// FetchMessage reads the next message without committing. Call CommitMessages after successful processing.
func (c *Consumer) FetchMessage(ctx context.Context) (segkafka.Message, error) {
	msg, err := c.reader.FetchMessage(ctx)
	if err != nil {
		return segkafka.Message{}, fmt.Errorf("fetch message: %w", err)
	}
	return msg, nil
}

// CommitMessages commits the given messages' offsets. Call after successful processing for at-least-once delivery.
func (c *Consumer) CommitMessages(ctx context.Context, msgs ...segkafka.Message) error {
	return c.reader.CommitMessages(ctx, msgs...)
}

// Close closes the consumer.
func (c *Consumer) Close() error {
	if err := c.reader.Close(); err != nil {
		return fmt.Errorf("close kafka consumer: %w", err)
	}
	return nil
}

// outboundPaymentHandler is implemented by types that handle outbound payment messages.
// Defined here (consumer) per Go idiom.
type outboundPaymentHandler interface {
	HandleOutboundPayment(ctx context.Context, idempotencyKey string, payload outbound.CreatePayload) (
		outbound.Payment, error)
}

// RunConsumer runs the outbound.payments consumer loop until ctx is cancelled.
// It fetches messages, parses them, calls the handler, and commits on success
// (or on non-duplicate errors to avoid poison pill).
func RunConsumer(ctx context.Context, consumer *Consumer, handler outboundPaymentHandler) {
	for {
		msg, err := consumer.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || ctx.Err() != nil {
				return
			}
			consumer.log.ErrorContext(ctx, "kafka fetch message", "err", err)
			continue
		}

		idempotencyKey := getHeader(msg, HeaderIdempotencyKey)
		if idempotencyKey == "" {
			consumer.log.ErrorContext(ctx, "process kafka message", "err", fmt.Errorf("missing header %q", HeaderIdempotencyKey))
			if err := consumer.CommitMessages(ctx, msg); err != nil {
				consumer.log.ErrorContext(ctx, "commit failed message", "err", err)
			}
			continue
		}

		var payload outbound.CreatePayload
		if err := json.Unmarshal(msg.Value, &payload); err != nil {
			consumer.log.ErrorContext(ctx, "process kafka message", "err", fmt.Errorf("unmarshal message body: %w", err))
			if err := consumer.CommitMessages(ctx, msg); err != nil {
				consumer.log.ErrorContext(ctx, "commit failed message", "err", err)
			}
			continue
		}

		_, err = handler.HandleOutboundPayment(ctx, idempotencyKey, payload)
		if err != nil {
			if errors.Is(err, outbound.ErrDuplicateIdempotency) {
				consumer.log.InfoContext(ctx, "duplicate idempotency key, skipping", "idempotency_key", idempotencyKey)
			} else {
				consumer.log.ErrorContext(ctx, "handle outbound payment", "err", err, "idempotency_key", idempotencyKey)
			}
			if !errors.Is(err, outbound.ErrDuplicateIdempotency) {
				if cErr := consumer.CommitMessages(ctx, msg); cErr != nil {
					consumer.log.ErrorContext(ctx, "commit message after error", "err", cErr)
				}
			}
			continue
		}

		if err := consumer.CommitMessages(ctx, msg); err != nil {
			consumer.log.ErrorContext(ctx, "commit message", "err", err)
		}
	}
}
