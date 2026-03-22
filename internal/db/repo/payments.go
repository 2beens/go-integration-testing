package repo

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"

	"github.com/2beens/simple-go-service/internal/outbound"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PaymentsRepo implements outbound.Repository for payments.
type PaymentsRepo struct {
	pool *pgxpool.Pool
}

// NewPaymentsRepo returns a new PaymentsRepo using the given pool.
func NewPaymentsRepo(pool *pgxpool.Pool) *PaymentsRepo {
	return &PaymentsRepo{pool: pool}
}

// Save inserts a new payment into the database.
func (r *PaymentsRepo) Save(ctx context.Context, payment outbound.Payment) error {
	_, err := r.pool.Exec(ctx,
		`INSERT INTO payments (id, amount, currency, status, idempotency_key, debtor, creditor, created_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		payment.ID, payment.Amount, payment.Currency, string(payment.Status),
		payment.IdempotencyKey, payment.DebtorName, payment.CreditorName, payment.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("insert payment: %w", err)
	}
	return nil
}

// UpdateStatus updates the status of an existing payment.
func (r *PaymentsRepo) UpdateStatus(ctx context.Context, id string, status outbound.Status) error {
	tag, err := r.pool.Exec(ctx,
		`UPDATE payments SET status = $1 WHERE id = $2`,
		string(status), id,
	)
	if err != nil {
		return fmt.Errorf("update payment status: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return outbound.ErrNotFound
	}
	return nil
}

// FindByID retrieves a single payment.
func (r *PaymentsRepo) FindByID(ctx context.Context, id string) (outbound.Payment, error) {
	row := r.pool.QueryRow(ctx,
		`SELECT id, amount, currency, status, idempotency_key, debtor, creditor, created_at
		 FROM payments WHERE id = $1`,
		id,
	)

	var payment outbound.Payment
	var status string
	if err := row.Scan(
		&payment.ID, &payment.Amount, &payment.Currency, &status,
		&payment.IdempotencyKey, &payment.DebtorName, &payment.CreditorName, &payment.CreatedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return outbound.Payment{}, outbound.ErrNotFound
		}
		return outbound.Payment{}, fmt.Errorf("scan payment row: %w", err)
	}

	payment.Status = outbound.Status(status)
	return payment, nil
}

// List returns all payments ordered by creation time descending.
func (r *PaymentsRepo) List(ctx context.Context) ([]outbound.Payment, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT id, amount, currency, status, idempotency_key, debtor, creditor, created_at
		 FROM payments ORDER BY created_at DESC`,
	)
	if err != nil {
		return nil, fmt.Errorf("query payments: %w", err)
	}
	defer rows.Close()

	var payments []outbound.Payment
	for rows.Next() {
		var payment outbound.Payment
		var status string
		if err := rows.Scan(
			&payment.ID, &payment.Amount, &payment.Currency, &status,
			&payment.IdempotencyKey, &payment.DebtorName, &payment.CreditorName, &payment.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan payment row: %w", err)
		}
		payment.Status = outbound.Status(status)
		payments = append(payments, payment)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate payment rows: %w", err)
	}

	return slices.Clone(payments), nil
}
