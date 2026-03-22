-- +goose Up
CREATE TABLE IF NOT EXISTS payments (
    id               TEXT        NOT NULL PRIMARY KEY,
    amount           BIGINT      NOT NULL,
    currency         CHAR(3)     NOT NULL,
    status           TEXT        NOT NULL DEFAULT 'created',
    idempotency_key  TEXT        NOT NULL UNIQUE,
    debtor           TEXT        NOT NULL DEFAULT '',
    creditor         TEXT        NOT NULL DEFAULT '',
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_payments_currency ON payments (currency);
CREATE INDEX IF NOT EXISTS idx_payments_created_at ON payments (created_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS idx_payments_idempotency_key ON payments (idempotency_key);

-- +goose Down
DROP TABLE IF EXISTS payments;
