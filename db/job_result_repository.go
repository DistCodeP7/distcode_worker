package db

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/DistCodeP7/distcode_worker/types"
	"github.com/jackc/pgx/v5"

	// Add the import for the LogEntry definition
	t "github.com/distcodep7/dsnet/testing"
	dt "github.com/distcodep7/dsnet/testing/disttest"
	"github.com/google/uuid"
)

// Update Interface signature to accept nodeMessageLogs
type JobStore interface {
	SaveResult(ctx context.Context, jobID uuid.UUID, outcome types.Outcome, testResults []dt.TestResult, logs []types.LogEvent, nodeMessageLogs []t.TraceEvent, startTime time.Time) error
}

type JobRepository struct {
	pool Repository
}

func NewJobRepository(pool Repository) *JobRepository {
	return &JobRepository{pool: pool}
}

func (jr *JobRepository) SaveResult(
	ctx context.Context,
	jobID uuid.UUID,
	outcome types.Outcome,
	testResults []dt.TestResult,
	logs []types.LogEvent,
	nodeMessageLogs []t.TraceEvent,
	startTime time.Time,
) error {
	if testResults == nil {
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
	tx, err := jr.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	updateQuery := `
		UPDATE job_results 
		SET outcome = $1, 
			test_results = $2,  
			duration = $3,
			logs = $4,
			finished_at = NOW() 
		WHERE job_uid = $5`

	_, err = tx.Exec(ctx, updateQuery,
		string(outcomeJSON),
		string(resultsJSON),
		duration,
		string(logsJSON),
		jobID,
	)

	if err != nil {
		return fmt.Errorf("failed to update job_results: %w", err)
	}

	if len(nodeMessageLogs) > 0 {
		insertQuery := `
			INSERT INTO job_process_messages 
			(job_uid, timestamp, "from", "to", event_type, message_type, vector_clock, payload, message_id, event_id)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`

		for _, msg := range nodeMessageLogs {
			vcJSON, err := json.Marshal(msg.VectorClock)
			if err != nil {
				return fmt.Errorf("failed to marshal vector clock: %w", err)
			}

			_, err = tx.Exec(ctx, insertQuery,
				jobID,
				msg.Timestamp,
				msg.From,
				msg.To,
				msg.EvtType,
				msg.MsgType,
				string(vcJSON),
				msg.Payload,
				msg.MessageID,
				msg.ID,
			)
			if err != nil {
				return fmt.Errorf("failed to insert node message log: %w", err)
			}
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}
