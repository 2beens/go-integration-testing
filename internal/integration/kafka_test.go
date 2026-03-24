package integration_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/2beens/simple-go-service/internal/kafka"
	"github.com/2beens/simple-go-service/internal/outbound"

	segkafka "github.com/segmentio/kafka-go"
)

// publishOutboundPayment publishes an outbound payment request to the given Kafka broker.
func publishOutboundPayment(
	t *testing.T,
	ctx context.Context,
	broker string,
	idempotencyKey string,
	payload outbound.CreatePayload,
) {
	t.Helper()

	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal outbound payment payload: %v", err)
	}

	writer := &segkafka.Writer{
		Addr:     segkafka.TCP(broker),
		Topic:    kafka.OutboundPaymentsTopic,
		Balancer: &segkafka.LeastBytes{},
	}
	defer func() {
		if err := writer.Close(); err != nil {
			t.Fatalf("close kafka writer: %v", err)
		}
	}()

	err = writer.WriteMessages(ctx, segkafka.Message{
		Key:   []byte(idempotencyKey),
		Value: body,
		Headers: []segkafka.Header{
			{Key: kafka.HeaderIdempotencyKey, Value: []byte(idempotencyKey)},
		},
	})
	if err != nil {
		t.Fatalf("write outbound payment message: %v", err)
	}
}

// consumeOneMessage consumes a single message from the given Kafka broker and topic at the given offset.
func consumeOneMessage(t *testing.T, broker, topic string, startOffset int64, timeout time.Duration) segkafka.Message {
	t.Helper()

	r := segkafka.NewReader(segkafka.ReaderConfig{
		Brokers:   []string{broker},
		Topic:     topic,
		Partition: 0,
		MaxWait:   500 * time.Millisecond,
	})
	defer r.Close()

	if err := r.SetOffset(startOffset); err != nil {
		t.Fatalf("set kafka reader offset to %d: %v", startOffset, err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()

	msg, err := r.ReadMessage(ctx)
	if err != nil {
		t.Fatalf("consume kafka message from %q at offset %d: %v", topic, startOffset, err)
	}

	return msg
}

// getCurrentKafkaOffset returns the current offset of the given Kafka topic.
func getCurrentKafkaOffset(t *testing.T, broker, topic string) int64 {
	t.Helper()

	ctx := t.Context()
	conn, err := segkafka.DialLeader(ctx, "tcp", broker, topic, 0)
	if err != nil {
		t.Fatalf("dial kafka leader for offset: %v", err)
	}
	defer conn.Close()

	_, last, err := conn.ReadOffsets()
	if err != nil {
		t.Fatalf("read kafka offsets: %v", err)
	}

	return last
}
