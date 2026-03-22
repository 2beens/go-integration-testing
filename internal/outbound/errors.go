package outbound

import "errors"

// Sentinel errors for the outbound payment domain.
var (
	ErrInvalidAmount        = errors.New("amount must be greater than zero")
	ErrInvalidCurrency      = errors.New("currency must be a 3-letter ISO 4217 code")
	ErrInvalidDebtorName    = errors.New("debtor name is required")
	ErrInvalidCreditorName  = errors.New("creditor name is required")
	ErrDuplicateIdempotency = errors.New("payment already processed for this idempotency key")
	ErrNotFound             = errors.New("payment not found")
	ErrInvalidStatus        = errors.New("status must be completed or rejected")
)
