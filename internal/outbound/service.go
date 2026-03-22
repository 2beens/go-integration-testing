package outbound

import (
	"context"
	"fmt"
	"log/slog"
)

//go:generate mockgen -destination=service_mocks_test.go -package=outbound_test -typed=true . idempotencyStore,repository,form3Creator,statusPublisher

// idempotencyStore is used by Service to check and set idempotency keys.
type idempotencyStore interface {
	KeyExists(ctx context.Context, key string) (bool, error)
	SetKey(ctx context.Context, key string) error
}

// repository persists and retrieves outbound payments.
type repository interface {
	Save(ctx context.Context, p Payment) error
	UpdateStatus(ctx context.Context, id string, status Status) error
	FindByID(ctx context.Context, id string) (Payment, error)
	List(ctx context.Context) ([]Payment, error)
}

// form3Creator creates a payment via the Form3 API.
type form3Creator interface {
	CreatePayment(ctx context.Context, p Payment) error
}

// statusPublisher publishes payment status events (completed/rejected) to Kafka.
type statusPublisher interface {
	PublishPaymentStatus(ctx context.Context, p Payment) error
}

// Service processes outbound payment messages (Kafka consume flow) and webhook status updates.
type Service struct {
	repo        repository
	idempotency idempotencyStore
	form3       form3Creator
	publisher   statusPublisher
	log         *slog.Logger
}

// NewService creates a new outbound payment Service.
func NewService(
	repo repository,
	idempotency idempotencyStore,
	form3 form3Creator,
	publisher statusPublisher,
) *Service {
	return &Service{
		repo:        repo,
		idempotency: idempotency,
		form3:       form3,
		publisher:   publisher,
		log:         slog.Default().With("component", "outbound.service"),
	}
}

// HandleOutboundPayment processes a single outbound payment request: idempotency check, validate,
// create via Form3, save to DB, set idempotency key. Call this from the Kafka consumer.
func (s *Service) HandleOutboundPayment(
	ctx context.Context,
	idempotencyKey string,
	payload CreatePayload,
) (Payment, error) {
	exists, err := s.idempotency.KeyExists(ctx, idempotencyKey)
	if err != nil {
		return Payment{}, fmt.Errorf("check idempotency key: %w", err)
	}
	if exists {
		return Payment{}, ErrDuplicateIdempotency
	}

	if err := payload.Validate(); err != nil {
		return Payment{}, fmt.Errorf("validation: %w", err)
	}

	payment := NewPayment(payload, idempotencyKey)

	if err := s.repo.Save(ctx, payment); err != nil {
		return Payment{}, fmt.Errorf("save payment: %w", err)
	}

	if err := s.idempotency.SetKey(ctx, idempotencyKey); err != nil {
		// Non-fatal: payment is already in DB; log and continue (idempotency may allow duplicate on retry).
		s.log.WarnContext(ctx, "failed to set idempotency key in Redis", "idempotency_key", idempotencyKey, "err", err)
	}

	if err := s.form3.CreatePayment(ctx, payment); err != nil {
		return Payment{}, fmt.Errorf("create payment with Form3: %w", err)
	}

	s.log.InfoContext(ctx, fmt.Sprintf("outbound payment [%s] created with idempotency key: %s", payment.ID, idempotencyKey))
	return payment, nil
}

// Get returns a single payment by ID.
func (s *Service) Get(ctx context.Context, id string) (Payment, error) {
	payment, err := s.repo.FindByID(ctx, id)
	if err != nil {
		return Payment{}, fmt.Errorf("find payment: %w", err)
	}
	return payment, nil
}

// List returns all payments.
func (s *Service) List(ctx context.Context) ([]Payment, error) {
	payments, err := s.repo.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("list payments: %w", err)
	}
	return payments, nil
}

// UpdateStatusFromWebhook updates the payment status (completed or rejected) and publishes to payment.status.
// Called from the Form3 webhook handler.
func (s *Service) UpdateStatusFromWebhook(ctx context.Context, paymentID string, status Status) (Payment, error) {
	if !status.IsTerminal() {
		return Payment{}, ErrInvalidStatus
	}

	payment, err := s.repo.FindByID(ctx, paymentID)
	if err != nil {
		return Payment{}, fmt.Errorf("find payment: %w", err)
	}

	if payment.Status.IsTerminal() {
		// Already terminal; idempotent success
		s.log.InfoContext(ctx, fmt.Sprintf("payment [%s] already in terminal status: %q", paymentID, payment.Status))
		return payment, nil
	}

	if err := s.repo.UpdateStatus(ctx, paymentID, status); err != nil {
		return Payment{}, fmt.Errorf("update payment [%s] status: %w", paymentID, err)
	}

	payment.Status = status

	if err := s.publisher.PublishPaymentStatus(ctx, payment); err != nil {
		s.log.WarnContext(ctx, fmt.Sprintf("failed to publish payment.status event for payment [%s]: %v", paymentID, err))
		// Non-fatal: DB is updated
	}

	s.log.InfoContext(ctx, fmt.Sprintf("payment [%s] status updated from webhook to: %q", paymentID, status))
	return payment, nil
}
