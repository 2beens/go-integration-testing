package db

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// Postgres wraps a pgx connection pool (migrations, pool access).
type Postgres struct {
	pool *pgxpool.Pool
	log  *slog.Logger
}

// New opens a connection pool and optionally runs migrations.
func New(ctx context.Context, dsn string) (*Postgres, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("create pgx pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}

	return &Postgres{
		pool: pool,
		log:  slog.Default().With("component", "db.postgres"),
	}, nil
}

// RunMigrations applies pending Goose migrations from the embedded FS.
func (p *Postgres) RunMigrations(ctx context.Context) error {
	// Goose needs a *sql.DB; obtain one from the pgx stdlib driver.
	db := stdlib.OpenDBFromPool(p.pool)

	goose.SetBaseFS(migrationsFS)
	if err := goose.SetDialect("postgres"); err != nil {
		return fmt.Errorf("set goose dialect: %w", err)
	}

	if err := goose.UpContext(ctx, db, "migrations"); err != nil {
		return fmt.Errorf("run migrations: %w", err)
	}

	p.log.InfoContext(ctx, "database migrations applied")
	return nil
}

// Close shuts down the connection pool.
func (p *Postgres) Close() {
	p.pool.Close()
}

// Pool exposes the underlying pool for use in tests.
func (p *Postgres) Pool() *pgxpool.Pool {
	return p.pool
}

// DB returns a *sql.DB built on top of the pool (useful for Goose in tests).
func (p *Postgres) DB() *sql.DB {
	return stdlib.OpenDBFromPool(p.pool)
}
