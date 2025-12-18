package db

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/DistCodeP7/distcode_worker/types"
	"github.com/DistCodeP7/distcode_worker/utils"
	"github.com/jackc/pgx/v5"

	t "github.com/distcodep7/dsnet/testing"
	dt "github.com/distcodep7/dsnet/testing/disttest"
)

type JobStore interface {
	SaveResult(
		ctx context.Context,
		outcome types.Outcome,
		testResults []dt.TestResult,
		logs []types.LogEvent,
		nodeMessageLogs []t.TraceEvent,
		startTime time.Time,
		job types.Job,
		timeSpent utils.TimeSpentPayload,
	) error
}

type JobRepository struct {
	pool Repository
}

var _ JobStore = (*JobRepository)(nil)

func NewJobRepository(pool Repository) *JobRepository {
	return &JobRepository{pool: pool}
}

func (jr *JobRepository) SaveResult(
	ctx context.Context,
	outcome types.Outcome,
	testResults []dt.TestResult,
	logs []types.LogEvent,
	nodeMessageLogs []t.TraceEvent,
	startTime time.Time,
	job types.Job,
	timeSpent utils.TimeSpentPayload,
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

	// Insert job result
	jobResultQuery := `
		INSERT INTO job_results
		(outcome, test_results, duration, logs, finished_at, queued_at, job_uid, user_id, problem_id,
		 time_compiling, time_running, time_reserving, time_pending, time_configuring_network)
		VALUES ($1,$2,$3,$4,NOW(),$5,$6,$7,$8,$9,$10,$11,$12,$13)
	`
	if _, err := tx.Exec(ctx, jobResultQuery,
		outcomeJSON,
		resultsJSON,
		duration,
		logsJSON,
		job.SubmittedAt,
		job.JobUID,
		job.UserID,
		job.ProblemID,
		timeSpent.Compiling,
		timeSpent.Running,
		timeSpent.Reserving,
		timeSpent.Pending,
		timeSpent.ConfiguringNetwork,
	); err != nil {
		return fmt.Errorf("insert job_results: %w", err)
	}

	if len(nodeMessageLogs) > 0 {
		values := make([]interface{}, 0, len(nodeMessageLogs)*10)
		placeholders := make([]string, 0, len(nodeMessageLogs))

		for i, msg := range nodeMessageLogs {
			vcJSON, err := json.Marshal(msg.VectorClock)
			if err != nil {
				return fmt.Errorf("marshal vector clock: %w", err)
			}

			idx := i*10 + 1
			placeholders = append(placeholders, fmt.Sprintf(
				"($%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d,$%d)",
				idx, idx+1, idx+2, idx+3, idx+4, idx+5, idx+6, idx+7, idx+8, idx+9,
			))

			values = append(values,
				job.JobUID,
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
		}

		insertQuery := `
			INSERT INTO job_process_messages
			(job_uid, timestamp, "from", "to", event_type, message_type, vector_clock, payload, message_id, event_id)
			VALUES ` + strings.Join(placeholders, ",")

		if _, err := tx.Exec(ctx, insertQuery, values...); err != nil {
			return fmt.Errorf("insert node messages: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
}
