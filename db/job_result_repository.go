package db

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/DistCodeP7/distcode_worker/log"
	"github.com/DistCodeP7/distcode_worker/types"
	dt "github.com/distcodep7/dsnet/testing/disttest"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// JobRepository defines the methods to interact with the database
type JobRepository interface {
	SaveResult(ctx context.Context, jobID uuid.UUID, outcome types.Outcome, testResults []dt.TestResult, logs []types.LogEvent, startTime time.Time) error
	Close()
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

// SaveResult send a job result to the database
func (r *PostgresRepository) SaveResult(
	ctx context.Context,
	jobID uuid.UUID,
	outcome types.Outcome,
	testResults []dt.TestResult,
	logs []types.LogEvent,
	startTime time.Time,
) error {
	if testResults == nil {
		log.Logger.Warn("testResults is nil, initializing to empty slice")
		testResults = []dt.TestResult{}
	}

	outcomeJSON, err := json.Marshal(outcome)
	if err != nil {
		return fmt.Errorf("failed to marshal outcome: %w", err)
	}

	resultsJSON, err := json.Marshal(testResults)
	if err != nil {
		return fmt.Errorf("failed to marshal test results: %w", err)
	}

	logsJSON, err := json.Marshal(logs)
	if err != nil {
		return fmt.Errorf("failed to marshal logs: %w", err)
	}

	duration := time.Since(startTime).Milliseconds()

	query := `
		UPDATE job_results 
		SET outcome = $1, 
			test_results = $2,  
			duration = $3,
			logs = $4,
			finished_at = NOW() 
		WHERE job_uid = $5`

	_, err = r.pool.Exec(ctx, query,
		string(outcomeJSON),
		string(resultsJSON),
		duration,
		string(logsJSON),
		jobID,
	)

	if err != nil {
		return fmt.Errorf("failed to save job result: %w", err)
	}

	return nil
}
