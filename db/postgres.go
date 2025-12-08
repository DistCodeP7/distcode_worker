package db

import (
	"context"
	"fmt"
	"os"

	"github.com/DistCodeP7/distcode_worker/endpoints/health"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Repository interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error)
	Close()
	health.HealthService
}

type PostgresRepository struct {
	pool *pgxpool.Pool
}

// NewPostgresRepository creates a new PostgresRepository
func NewPostgresRepository(ctx context.Context) (*PostgresRepository, error) {
	connString := os.Getenv("DB_URL")
	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		return nil, fmt.Errorf("unable to connect to database: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("database ping failed: %w", err)
	}

	return &PostgresRepository{pool: pool}, nil
}

// Close closes the database connection
func (r *PostgresRepository) Close() {
	r.pool.Close()
}

func (r *PostgresRepository) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	return r.pool.Exec(ctx, sql, args...)
}

func (r *PostgresRepository) BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error) {
	return r.pool.BeginTx(ctx, txOptions)
}

func (r *PostgresRepository) ping(ctx context.Context) error {
	return r.pool.Ping(ctx)
}

func (r *PostgresRepository) Ok() (bool, string) {
	if err := r.ping(context.Background()); err != nil {
		return false, fmt.Sprintf("Database ping failed: %v", err)
	}
	return true, "Database connection is healthy"
}

func (r *PostgresRepository) ServiceName() string {
	return "PostgresConnection"
}
