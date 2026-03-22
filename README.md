<div align="center">

# Simple Outbound Payments Service

*Beyond unit tests: integration testing Go microservices with Docker*  
Golang Meetup Sofia 🇧🇬 March 2026 🇧🇬

![Go](https://img.shields.io/badge/Go-1.26+-00ADD8?logo=go&logoColor=white)

</div>

---

## What it does

The service processes **outbound payment requests**: it reads them from Kafka, enforces idempotency via Redis, creates payments via a simulated Form3 HTTP API, and updates their status when Form3 calls the webhook.

```
Kafka (outbound.payments)  →  Redis (idempotency check)
                           →  Form3 API (create payment)
                           →  PostgreSQL (save payment, status=created)
                           →  Redis (set idempotency key)

Form3 webhook (completed/rejected)  →  PostgreSQL (update status)
                                    →  Kafka (publish payment.status event)
```

### Endpoints

| Method | Path                           | Description                          |
|--------|--------------------------------|--------------------------------------|
| POST   | `/webhooks/payments/status`    | Form3 callback (completed/rejected) |
| GET    | `/payments`                    | List all payments                    |
| GET    | `/payments/{id}`               | Get a single payment by ID           |
| GET    | `/health`                      | Liveness probe                       |

### Example

```bash
# List payments
curl -s http://localhost:8080/payments | jq .

# Get a payment
curl -s http://localhost:8080/payments/{id} | jq .
```

**Creating payments** — The service consumes the `outbound.payments` Kafka topic. Each message must have:

- **Body:** `amount`, `currency`, `debtor_name`, `creditor_name` (e.g. `{"amount": 1999, "currency": "EUR", "debtor_name": "Debtor Inc", "creditor_name": "Creditor Ltd"}`)
- **Header:** `idempotency-key: <uuid>`

**Status updates** — Request body: `{"payment_id": "<id>", "status": "completed"|"rejected"}`.

## Stack

| Concern         | Library                                |
|-----------------|----------------------------------------|
| HTTP            | `net/http` (stdlib, Go 1.26)            |
| PostgreSQL      | `jackc/pgx/v5`                         |
| DB Migrations   | `pressly/goose/v3` (embedded SQL)      |
| Kafka           | `segmentio/kafka-go` (consumer + producer) |
| Redis           | `redis/go-redis/v9` (idempotency keys) |
| Logging         | `log/slog` (stdlib)                    |

## Running locally

```bash
# Start PostgreSQL, Redis, and Kafka
make docker-up

# Run the service (DB migrations run automatically on startup)
make run
```

### Environment variables

| Variable          | Default                                                              |
|-------------------|----------------------------------------------------------------------|
| `HTTP_ADDR`       | `:8080`                                                              |
| `POSTGRES_DSN`    | `postgres://postgres:postgres@localhost:5432/simple_go_service?sslmode=disable` |
| `REDIS_ADDR`      | `localhost:6379`                                                     |
| `KAFKA_BROKER`    | `localhost:9092`                                                     |
| `FORM3_BASE_URL`  | `http://localhost:9090`                                              |

## Testing

```bash
# Unit tests
make test

# Unit tests with race detector
make test-race

# Integration tests (requires Docker)
make test-integration
```

Integration tests are skipped unless `RUN_INTEGRATION_TESTS=1` is set.

## Debugging in VS Code / Cursor

Open **Run and Debug** (⇧⌘D / Shift+Ctrl+D) and choose one of the pre-configured launch targets in `.vscode/launch.json`:

- **Debug Unit Tests** — runs all unit tests with the debugger attached.
- **Debug Integration Tests** — runs integration tests with `RUN_INTEGRATION_TESTS=1`; containers start automatically via testcontainers-go.
- **Debug Server** — starts the HTTP server pointed at local Docker services.

Set breakpoints anywhere in application or test code. The debugger works the same way with testcontainers-go — Docker containers start as usual; execution just stops at your breakpoints.
