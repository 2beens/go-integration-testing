package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/2beens/simple-go-service/internal/outbound"
	segkafka "github.com/segmentio/kafka-go"
)

const (
	// PaymentStatusTopic is the Kafka topic for payment status events (completed/rejected only).
	PaymentStatusTopic = "payment.status"
)

// Producer publishes payment status events to Kafka.
type Producer struct {
	writer *segkafka.Writer
	log    *slog.Logger
}

// NewProducer creates a new Kafka Producer.
func NewProducer(brokers []string) *Producer {
	w := &segkafka.Writer{
		Addr:                   segkafka.TCP(brokers...),
		Topic:                  PaymentStatusTopic,
		Balancer:               &segkafka.LeastBytes{},
		BatchTimeout:           10 * time.Millisecond,
		AllowAutoTopicCreation: true,
	}
	return &Producer{
		writer: w,
		log:    slog.Default().With("component", "kafka.producer"),
	}
}

// PublishPaymentStatus publishes a payment status event (completed or rejected) to payment.status.
func (p *Producer) PublishPaymentStatus(ctx context.Context, payment outbound.Payment) error {
	event := outbound.PaymentStatusEvent{
		PaymentID: payment.ID,
		Status:    string(payment.Status),
		Amount:    payment.Amount,
		Currency:  payment.Currency,
		UpdatedAt: time.Now().UTC(),
	}

	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal payment status event: %w", err)
	}

	msg := segkafka.Message{
		Key:   []byte(payment.ID),
		Value: payload,
		Headers: []segkafka.Header{
			{Key: "event_type", Value: []byte("payment.status")},
			{Key: "content_type", Value: []byte("application/json")},
		},
	}

	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("write kafka message: %w", err)
	}

	p.log.InfoContext(ctx, "payment.status event published",
		"payment_id", payment.ID,
		"status", payment.Status,
		"topic", PaymentStatusTopic)
	return nil
}

// Close flushes and closes the underlying writer.
func (p *Producer) Close() error {
	return p.writer.Close()
}
