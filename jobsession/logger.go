package jobsession

import (
	"bufio"
	"io"
	"sync"
	"time"

	"github.com/DistCodeP7/distcode_worker/types"
	"github.com/distcodep7/dsnet/testing"
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
	outcome   types.Outcome
	mu        sync.Mutex
	logBuffer []types.LogEvent
}

// Constructor for JobSessionLogger
func NewJobSession(job types.Job, out chan<- types.StreamingJobEvent) *JobSessionLogger {
	return &JobSessionLogger{
		jobID:     job.JobUID,
		userID:    job.UserID,
		out:       out,
		startTime: time.Now(),
		phase:     types.PhasePending,
		outcome:   types.OutcomeSuccess, // Assumes success until failure
		logBuffer: make([]types.LogEvent, 0, 100),
	}
}

// Used to update the current phase and send a status event
func (s *JobSessionLogger) SetPhase(p types.Phase, msg string) {
	s.phase = p
	s.out <- types.StreamingJobEvent{
		UserID: s.userID,
		Type:   types.TypeStatus,
		Status: &types.StatusEvent{
			Phase:   string(s.phase),
			Message: msg,
		},
	}
}

var (
	MaxLogBuffer = 10000
)

// Used to create a new log writer for streaming logs from a worker
func (s *JobSessionLogger) NewLogWriter(workerID string) io.Writer {
	pr, pw := io.Pipe()

	go func() {
		scanner := bufio.NewScanner(pr)
		for scanner.Scan() {
			text := scanner.Text()

			logEvent := types.LogEvent{
				WorkerID: workerID,
				Phase:    s.phase,
				Message:  text + "\n",
			}

			s.out <- types.StreamingJobEvent{
				UserID: s.userID,
				Type:   types.TypeLog,
				Log:    &logEvent,
			}

			s.mu.Lock()
			if len(s.logBuffer) < MaxLogBuffer {
				s.logBuffer = append(s.logBuffer, logEvent)
			}
			s.mu.Unlock()
		}
	}()

	return pw
}

func (s *JobSessionLogger) GetBufferedLogs() []types.LogEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	logs := make([]types.LogEvent, len(s.logBuffer))
	copy(logs, s.logBuffer)
	return logs
}

func (s *JobSessionLogger) GetOutcome() types.Outcome {
	return s.outcome
}

func (s *JobSessionLogger) StartTime() time.Time {
	return s.startTime
}

func (s *JobSessionLogger) duration() int64 {
	return time.Since(s.startTime).Milliseconds()
}

type JobArtifacts struct {
	TestResults []dt.TestResult
	TestLogs    []testing.LogEntry
}

func (s *JobSessionLogger) FinishSuccess(artifacts JobArtifacts) {
	s.phase = types.PhaseCompleted
	s.out <- types.StreamingJobEvent{
		UserID: s.userID,
		Type:   types.TypeResult,
		Result: &types.ResultEvent{
			Outcome:     types.OutcomeSuccess,
			DurationMs:  s.duration(),
			TestResults: artifacts.TestResults,
			//TODO ADD LOGS
		},
	}
}

func (s *JobSessionLogger) FinishFail(artifacts JobArtifacts, outcome types.Outcome, err error, workerID string) {
	s.phase = types.PhaseCompleted
	s.outcome = outcome
	errStr := ""
	if err != nil {
		errStr = err.Error()
	}

	s.out <- types.StreamingJobEvent{
		UserID: s.userID,
		Type:   types.TypeResult,
		Result: &types.ResultEvent{
			Outcome:        s.outcome,
			DurationMs:     s.duration(),
			Error:          errStr,
			TestResults:    artifacts.TestResults,
			FailedWorkerID: workerID,
			//TODO ADD LOGS
		},
	}
}
