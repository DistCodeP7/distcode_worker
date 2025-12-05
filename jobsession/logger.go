package jobsession

import (
	"bufio"
	"io"
	"time"

	"github.com/DistCodeP7/distcode_worker/types"
	dt "github.com/distcodep7/dsnet/testing/disttest"
	"github.com/google/uuid"
)

// Struct that manages job session logging and event emission
type JobSessionLogger struct {
	jobID     uuid.UUID
	userID    string
	out       chan<- types.StreamingJobEvent
	startTime time.Time
	phase     types.Phase
}

// Constructor for JobSessionLogger
func NewJobSession(job types.Job, out chan<- types.StreamingJobEvent) *JobSessionLogger {
	return &JobSessionLogger{
		jobID:     job.JobUID,
		userID:    job.UserID,
		out:       out,
		startTime: time.Now(),
		phase:     types.PhasePending,
	}
}

// Used to update the current phase and send a status event
func (s *JobSessionLogger) SetPhase(p types.Phase, msg string) {
	s.phase = p
	s.out <- types.StreamingJobEvent{
		JobUID: s.jobID,
		UserID: s.userID,
		Type:   types.TypeStatus,
		Status: &types.StatusEvent{
			Phase:   string(s.phase),
			Message: msg,
		},
	}
}

// Used to create a new log writer for streaming logs from a worker
func (s *JobSessionLogger) NewLogWriter(workerID string) io.Writer {
	pr, pw := io.Pipe()

	go func() {
		scanner := bufio.NewScanner(pr)
		for scanner.Scan() {
			text := scanner.Text()
			s.out <- types.StreamingJobEvent{
				JobUID: s.jobID,
				UserID: s.userID,
				Type:   types.TypeLog,
				Log: &types.LogEvent{
					WorkerID: workerID,
					Phase:    s.phase,
					Message:  text + "\n",
				},
			}
		}
	}()

	return pw
}

func (s *JobSessionLogger) duration() int64 {
	return time.Since(s.startTime).Milliseconds()
}

func (s *JobSessionLogger) FinishSuccess(tests []dt.TestResult) {
	s.phase = types.PhaseCompleted
	s.out <- types.StreamingJobEvent{
		JobUID: s.jobID,
		UserID: s.userID,
		Type:   types.TypeResult,
		Result: &types.ResultEvent{
			Outcome:     types.OutcomeSuccess,
			DurationMs:  s.duration(),
			TestResults: tests,
		},
	}
}

func (s *JobSessionLogger) FinishFail(results []dt.TestResult, outcome types.Outcome, err error, workerID string) {
	s.phase = types.PhaseCompleted
	errStr := ""
	if err != nil {
		errStr = err.Error()
	}

	s.out <- types.StreamingJobEvent{
		JobUID: s.jobID,
		UserID: s.userID,
		Type:   types.TypeResult,
		Result: &types.ResultEvent{
			Outcome:        types.Outcome(outcome),
			DurationMs:     s.duration(),
			Error:          errStr,
			TestResults:    results,
			FailedWorkerID: workerID,
		},
	}
}
