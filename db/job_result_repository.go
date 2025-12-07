package db

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/DistCodeP7/distcode_worker/log"
	"github.com/DistCodeP7/distcode_worker/types"
	dt "github.com/distcodep7/dsnet/testing/disttest"
	"github.com/google/uuid"
)

// JobRepository defines the methods to interact with the database
type JobStore interface {
	SaveResult(ctx context.Context, jobID uuid.UUID, outcome types.Outcome, testResults []dt.TestResult, logs []types.LogEvent, startTime time.Time) error
}

type JobRepository struct {
	pool Repository
}

// NewJobRepository creates a new JobRepository using an existing PostgresRepository
func NewJobRepository(pool Repository) *JobRepository {
	return &JobRepository{pool: pool}
}

// SaveResult send a job result to the database
func (jr *JobRepository) SaveResult(
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

	_, err = jr.pool.Exec(ctx, query,
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
