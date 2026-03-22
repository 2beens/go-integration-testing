.PHONY: build run test lint tidy docker-up docker-down

# Build the service binary.
build:
	go build -o bin/server ./cmd/server

# Run the service (requires docker-up first).
run:
	go run ./cmd/server

# Run unit tests only.
test:
	go test ./...

# Run unit tests with race detector.
test-race:
	go test -race ./...

# Run integration tests (requires Docker). Set RUN_INTEGRATION_TESTS=1 to enable.
test-integration:
	RUN_INTEGRATION_TESTS=1 go test -v -timeout=120s ./internal/integration/...

# Tidy Go modules.
tidy:
	go mod tidy

# Start dependencies via Docker Compose.
docker-up:
	docker compose up -d --wait

# Stop and remove Docker Compose services.
docker-down:
	docker compose down -v

# Run linter (requires golangci-lint).
lint:
	golangci-lint run ./...

# Clean build artifacts.
clean:
	rm -rf bin/
