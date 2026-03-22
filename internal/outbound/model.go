package outbound

import (
	"time"

	"github.com/google/uuid"
)

// Status represents the lifecycle state of an outbound payment.
type Status string

const (
	StatusCreated   Status = "created"
	StatusCompleted Status = "completed"
	StatusRejected  Status = "rejected"
)

// Payment is the core domain entity for an outbound payment.
type Payment struct {
	ID             string    `json:"id"`
	Amount         int64     `json:"amount"`   // in minor currency units (e.g. cents)
	Currency       string    `json:"currency"` // ISO 4217, e.g. "EUR"
	Status         Status    `json:"status"`
	IdempotencyKey string    `json:"idempotency_key"`
	DebtorName     string    `json:"debtor_name"`
	CreditorName   string    `json:"creditor_name"`
	CreatedAt      time.Time `json:"created_at"`
}

// NewPayment builds a new Payment from a CreatePayload and idempotency key.
func NewPayment(payload CreatePayload, idempotencyKey string) Payment {
	return Payment{
		ID:             uuid.NewString(),
		Amount:         payload.Amount,
		Currency:       payload.Currency,
		Status:         StatusCreated,
		IdempotencyKey: idempotencyKey,
		DebtorName:     payload.DebtorName,
		CreditorName:   payload.CreditorName,
		CreatedAt:      time.Now().UTC(),
	}
}

// IsTerminal returns true if the payment is in a final state.
func (s Status) IsTerminal() bool {
	return s == StatusCompleted || s == StatusRejected
}

// CreatePayload is the payload consumed from Kafka (outbound.payments).
type CreatePayload struct {
	Amount       int64  `json:"amount"`
	Currency     string `json:"currency"`
	DebtorName   string `json:"debtor_name"`
	CreditorName string `json:"creditor_name"`
}

// Validate performs basic input validation.
func (p CreatePayload) Validate() error {
	if p.Amount <= 0 {
		return ErrInvalidAmount
	}
	if len(p.Currency) != 3 {
		return ErrInvalidCurrency
	}
	if p.DebtorName == "" {
		return ErrInvalidDebtorName
	}
	if p.CreditorName == "" {
		return ErrInvalidCreditorName
	}
	return nil
}

// PaymentStatusEvent is the payload published when a payment reaches completed or rejected
// (e.g. to payment.status Kafka topic). Domain owns the event shape; transport layer serializes it.
type PaymentStatusEvent struct {
	PaymentID string    `json:"payment_id"`
	Status    string    `json:"status"` // "completed" or "rejected"
	Amount    int64     `json:"amount"`
	Currency  string    `json:"currency"`
	UpdatedAt time.Time `json:"updated_at"`
}
