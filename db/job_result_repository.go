package db

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/DistCodeP7/distcode_worker/jobsession"
	"github.com/DistCodeP7/distcode_worker/types"
	"github.com/DistCodeP7/distcode_worker/utils"
	"github.com/jackc/pgx/v5"

	dt "github.com/distcodep7/dsnet/testing/disttest"
)

type JobResult struct {
	Job       types.Job
	Logs      []types.LogEvent
	Artifacts jobsession.JobArtifacts
	Outcome   types.Outcome
	Timespent utils.TimeSpentPayload
	Duration  time.Duration
	Error     error
}

type JobStore interface {
	SaveResult(
		ctx context.Context,
		results JobResult,
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
	results JobResult,
) error {
	if results.Artifacts.TestResults == nil {
		results.Artifacts.TestResults = []dt.TestResult{}
	}

	outcomeJSON, err := json.Marshal(results.Outcome)
	if err != nil {
		return fmt.Errorf("failed to marshal outcome: %w", err)
	}

	resultsJSON, err := json.Marshal(results.Artifacts.TestResults)
	if err != nil {
		return fmt.Errorf("failed to marshal test results: %w", err)
	}

	logsJSON, err := json.Marshal(results.Logs)
	if err != nil {
		return fmt.Errorf("failed to marshal logs: %w", err)
	}

	duration := results.Duration.Milliseconds()
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
		results.Job.SubmittedAt,
		results.Job.JobUID,
		results.Job.UserID,
		results.Job.ProblemID,
		results.Timespent.Compiling,
		results.Timespent.Running,
		results.Timespent.Reserving,
		results.Timespent.Pending,
		results.Timespent.ConfiguringNetwork,
	); err != nil {
		return fmt.Errorf("insert job_results: %w", err)
	}

	if len(results.Artifacts.NodeMessageLogs) > 0 {
		values := make([]interface{}, 0, len(results.Artifacts.NodeMessageLogs)*10)
		placeholders := make([]string, 0, len(results.Artifacts.NodeMessageLogs))

		for i, msg := range results.Artifacts.NodeMessageLogs {
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
				results.Job.JobUID,
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
