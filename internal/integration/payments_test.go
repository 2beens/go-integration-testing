package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"time"

	"github.com/2beens/simple-go-service/internal/kafka"
	"github.com/2beens/simple-go-service/internal/outbound"

	segkafka "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestOutboundPayment_HappyPath: produce outbound payment to Kafka → consumer → Form3 → DB → Redis,
// then webhook completed → DB update → payment.status event. Uses all deps: Kafka, HTTP (Form3 + webhook), DB, Redis.
func (s *PaymentsSuite) TestOutboundPayment_HappyPath() {
	ctx := s.T().Context()

	idempotencyKey := "idempotency-test-happy-1"
	// Confirm the idempotency key was not yet stored in Redis before we publish the outbound payment request.
	existsBefore, err := s.redisClient.IdempotencyKeyExists(ctx, idempotencyKey)
	s.Require().NoError(err)
	s.Assert().False(existsBefore, "idempotency key should not be set in Redis before we publish the outbound payment request")

	payload := outbound.CreatePayload{
		Amount:       1500,
		Currency:     "EUR",
		DebtorName:   "Debtor Inc",
		CreditorName: "Creditor Ltd",
	}

	// Publish/simulate the outbound payment request that this test will later trace through DB, Redis, webhook, and status event assertions.
	publishOutboundPayment(s.T(), ctx, s.kafkaBroker, idempotencyKey, payload)

	// Wait until the payment exists in Postgres before asserting any of its stored fields or follow-up effects.
	// This means that the service has processed the request and created the payment in the database.
	var payment outbound.Payment
	require.Eventually(s.T(), func() bool {
		list, err := s.paymentsRepo.List(ctx)
		s.Require().NoError(err)
		for _, paymentInList := range list {
			if paymentInList.IdempotencyKey == idempotencyKey {
				payment = paymentInList
				return true
			}
		}
		return false
	}, 10*time.Second, 200*time.Millisecond, "payment with idempotency key %q should be created", idempotencyKey)

	// Check the stored payment fields before the webhook changes the status.
	s.Require().NotEmpty(payment.ID, "payment should be created from Kafka message")
	s.Assert().Equal(int64(1500), payment.Amount)
	s.Assert().Equal("EUR", payment.Currency)
	s.Assert().Equal(outbound.StatusCreated, payment.Status)

	// Confirm the outbound request reached Form3 with the expected payment details.
	form3Reqs := s.form3Recorder.Requests()
	i := slices.IndexFunc(form3Reqs, func(r form3CreateRequest) bool {
		return r.ID == payment.ID
	})
	s.Require().GreaterOrEqual(i, 0, "Form3 request for payment %s not found", payment.ID)
	form3Req := &form3Reqs[i]
	s.Assert().Equal(payment.ID, form3Req.ID, "Form3 request payment ID")
	s.Assert().Equal(int64(1500), form3Req.Amount)
	s.Assert().Equal("EUR", form3Req.Currency)
	s.Assert().Equal("Debtor Inc", form3Req.Debtor)
	s.Assert().Equal("Creditor Ltd", form3Req.Creditor)

	// Confirm the idempotency key was stored in Redis because this payment was processed successfully.
	exists, err := s.redisClient.IdempotencyKeyExists(ctx, idempotencyKey)
	s.Require().NoError(err)
	s.Assert().True(exists, "idempotency key should be set in Redis")

	// Capture the current status-topic offset so this test reads only the event emitted after the webhook below.
	kafkaOffset := currentKafkaOffset(s.T(), s.kafkaBroker, kafka.PaymentStatusTopic)

	// Send/simulate the completed webhook that should transition this specific payment to its terminal state (completed).
	webhookBody, err := json.Marshal(map[string]string{"payment_id": payment.ID, "status": "completed"})
	s.Require().NoError(err)
	resp, err := http.Post(s.serverURL+"/webhooks/payments/status", "application/json", bytes.NewReader(webhookBody))
	s.Require().NoError(err)
	defer resp.Body.Close()
	s.Require().Equal(http.StatusOK, resp.StatusCode)

	// Check that the webhook request resulted in the payment status being updated in the database to completed.
	paymentAfterWebhook, err := s.paymentsRepo.FindByID(ctx, payment.ID)
	s.Require().NoError(err)
	s.Assert().Equal(outbound.StatusCompleted, paymentAfterWebhook.Status)

	// Consume the emitted payment.status Kafka event by the service and assert its payload matches the webhook outcome (completed).
	msg := consumeOneMessage(s.T(), s.kafkaBroker, kafka.PaymentStatusTopic, kafkaOffset, 10*time.Second)

	var event outbound.PaymentStatusEvent
	s.Require().NoError(json.Unmarshal(msg.Value, &event))
	s.Assert().Equal(payment.ID, event.PaymentID)
	s.Assert().Equal("completed", event.Status)

	// Verify the emitted Kafka event includes the expected header metadata for downstream consumers.
	i = slices.IndexFunc(msg.Headers, func(h segkafka.Header) bool { return h.Key == "event_type" })
	s.Require().GreaterOrEqual(i, 0, "kafka header %q not found", "event_type")
	s.Assert().Equal("payment.status", string(msg.Headers[i].Value), "kafka header %q", "event_type")

	// Fetch the payment over HTTP to confirm the API exposes the final stored state (completed).
	resp, err = http.Get(fmt.Sprintf("%s/payments/%s", s.serverURL, paymentAfterWebhook.ID))
	s.Require().NoError(err)
	defer resp.Body.Close()
	s.Require().Equal(http.StatusOK, resp.StatusCode)

	var got outbound.Payment
	s.Require().NoError(json.NewDecoder(resp.Body).Decode(&got))
	s.Assert().Equal(paymentAfterWebhook.ID, got.ID)
	s.Assert().Equal(paymentAfterWebhook.Amount, got.Amount)
	s.Assert().Equal(paymentAfterWebhook.Status, got.Status)
}

// TestOutboundPayment_DuplicateIdempotency: same Kafka message key twice results in one payment only.
func (s *PaymentsSuite) TestOutboundPayment_DuplicateIdempotency() {
	ctx := s.T().Context()

	idempotencyKey := "idempotency-test-dup-1"
	// Confirm the idempotency key was not yet stored in Redis before we publish the outbound payment request.
	existsBefore, err := s.redisClient.IdempotencyKeyExists(ctx, idempotencyKey)
	s.Require().NoError(err)
	s.Assert().False(existsBefore, "idempotency key should not be set in Redis before we publish the outbound payment request")

	payload := outbound.CreatePayload{
		Amount:       100,
		Currency:     "USD",
		DebtorName:   "Debtor",
		CreditorName: "Creditor",
	}

	form3RequestsBefore := len(s.form3Recorder.Requests())

	// Publish the first message to create the baseline payment that the duplicate attempts should not replace.
	publishOutboundPayment(s.T(), ctx, s.kafkaBroker, idempotencyKey, payload)

	// Wait for the first payment so the test has a concrete payment ID to protect from duplicate creation.
	var payment outbound.Payment
	require.Eventually(s.T(), func() bool {
		payments, err := s.paymentsByIdempotency(ctx, idempotencyKey)
		s.Require().NoError(err)
		if len(payments) > 0 {
			payment = payments[0]
			return true
		}
		return false
	}, 15*time.Second, 100*time.Millisecond, "payment with idempotency key %q should be created", idempotencyKey)
	s.Require().NotEmpty(payment.ID, "payment should be created from first Kafka message")

	// Wait for the first Form3 call so the test can compare later side effects against this baseline.
	require.Eventually(s.T(), func() bool {
		select {
		case <-ctx.Done():
			require.FailNow(s.T(), "context cancelled while waiting for Form3 requests", "err=%v", ctx.Err())
		default:
		}

		return len(s.form3Recorder.Requests()) >= form3RequestsBefore+1
	}, 10*time.Second, 100*time.Millisecond, "expected at least %d Form3 requests", form3RequestsBefore+1)

	// Confirm Redis now holds the key that should block repeated processing of the same message.
	exists, err := s.redisClient.IdempotencyKeyExists(ctx, idempotencyKey)
	s.Require().NoError(err)
	s.Assert().True(exists, "idempotency key should be set in Redis after first message")

	// Publish duplicates of the same message to verify they do not create more work.
	publishOutboundPayment(s.T(), ctx, s.kafkaBroker, idempotencyKey, payload)
	publishOutboundPayment(s.T(), ctx, s.kafkaBroker, idempotencyKey, payload)

	// Verify the duplicates left the system unchanged: still one payment and still one Form3 call.
	require.Eventually(s.T(), func() bool {
		payments, err := s.paymentsByIdempotency(ctx, idempotencyKey)
		s.Require().NoError(err)
		return assert.Len(s.T(), payments, 1, "expected exactly one payment for idempotency key %q", idempotencyKey) &&
			assert.Equal(s.T(), payment.ID, payments[0].ID, "payment ID for idempotency key %q", idempotencyKey) &&
			assert.Len(s.T(), s.form3Recorder.Requests(), form3RequestsBefore+1, "expected Form3 request count")
	}, 3*time.Second, 100*time.Millisecond)
}

func (s *PaymentsSuite) paymentsByIdempotency(ctx context.Context, idempotencyKey string) ([]outbound.Payment, error) {
	s.T().Helper()

	list, err := s.paymentsRepo.List(ctx)
	if err != nil {
		return nil, err
	}

	var payments []outbound.Payment
	for _, payment := range list {
		if payment.IdempotencyKey == idempotencyKey {
			payments = append(payments, payment)
		}
	}

	return payments, nil
}
