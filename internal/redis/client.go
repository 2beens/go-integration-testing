package redis

import (
	"context"
	"fmt"
	"log/slog"

	goredis "github.com/redis/go-redis/v9"
)

const (
	idempotencyKeyPrefix = "idempotency:outbound:"
)

// Config holds Redis connection parameters.
type Config struct {
	Addr     string // e.g. "localhost:6379"
	Password string // leave empty when auth is not required
	DB       int    // Redis database index
}

// Client wraps a go-redis client for idempotency and other cache use.
type Client struct {
	rdb *goredis.Client
	log *slog.Logger
}

// New creates a new Redis Client and pings the server.
func New(ctx context.Context, cfg Config) (*Client, error) {
	rdb := goredis.NewClient(&goredis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
	})

	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("ping redis: %w", err)
	}

	return &Client{
		rdb: rdb,
		log: slog.Default().With("component", "redis.client"),
	}, nil
}

// IdempotencyKeyExists returns true if the idempotency key has already been used (payment already processed).
func (c *Client) IdempotencyKeyExists(ctx context.Context, key string) (bool, error) {
	k := idempotencyKeyPrefix + key
	_, err := c.rdb.Get(ctx, k).Result()
	if err != nil {
		if err == goredis.Nil {
			return false, nil
		}
		return false, fmt.Errorf("get %s: %w", k, err)
	}
	return true, nil
}

// SetIdempotencyKey records the idempotency key (permanent, no TTL) after a payment has been processed.
func (c *Client) SetIdempotencyKey(ctx context.Context, key string) error {
	k := idempotencyKeyPrefix + key
	if err := c.rdb.Set(ctx, k, "1", 0).Err(); err != nil {
		return fmt.Errorf("set %s: %w", k, err)
	}
	return nil
}

// KeyExists returns true if the key has already been used. Satisfies idempotency store interfaces that use this name.
func (c *Client) KeyExists(ctx context.Context, key string) (bool, error) {
	return c.IdempotencyKeyExists(ctx, key)
}

// SetKey records the key after processing. Satisfies idempotency store interfaces that use this name.
func (c *Client) SetKey(ctx context.Context, key string) error {
	return c.SetIdempotencyKey(ctx, key)
}

// Close closes the underlying Redis connection.
func (c *Client) Close() error {
	return c.rdb.Close()
}

// RDB exposes the underlying go-redis client (useful in tests).
func (c *Client) RDB() *goredis.Client {
	return c.rdb
}
