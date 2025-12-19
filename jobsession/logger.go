package jobsession

import (
	"bufio"
	"errors"
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

	// time spent in each phase
	timeSpent map[types.Phase]time.Duration
	clock     time.Time
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
		timeSpent: make(map[types.Phase]time.Duration),
		clock:     time.Now(),
	}
}

func (s *JobSessionLogger) trackPhaseTime(newPhase types.Phase) {
	now := time.Now()
	s.timeSpent[s.phase] += now.Sub(s.clock.Add(1 * time.Millisecond))
	s.clock = now
	s.phase = newPhase
}

// Used to update the current phase and send a status event
func (s *JobSessionLogger) SetPhase(p types.Phase, msg string) {
	s.mu.Lock()
	s.trackPhaseTime(p)
	s.mu.Unlock()

	s.out <- types.StreamingJobEvent{
		UserID: s.userID,
		Type:   types.TypeStatus,
		Status: &types.StatusEvent{
			Phase:   string(p),
			Message: msg,
		},
	}
}

const (
	MaxLogBuffer = 10000
	MaxLogBytes  = 1 * 1024 * 1024
)

var ErrLogLimitExceeded = errors.New("log limit exceeded")

func (s *JobSessionLogger) NewLogWriter(workerID string) io.Writer {
	pr, pw := io.Pipe()
	var totalBytes int64 = 0

	go func() {
		defer pr.Close()

		scanner := bufio.NewScanner(pr)
		for scanner.Scan() {
			text := scanner.Text()
			msgSize := len(text) + 1
			s.mu.Lock()
			if totalBytes+int64(msgSize) > MaxLogBytes || len(s.logBuffer) >= MaxLogBuffer {
				s.mu.Unlock()
				pr.CloseWithError(ErrLogLimitExceeded)
				return
			}

			logEvent := types.LogEvent{
				WorkerID: workerID,
				Phase:    s.phase,
				Message:  text + "\n",
			}

			s.logBuffer = append(s.logBuffer, logEvent)
			totalBytes += int64(msgSize)

			s.mu.Unlock()

			s.out <- types.StreamingJobEvent{
				UserID: s.userID,
				Type:   types.TypeLog,
				Log:    &logEvent,
			}
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
	TestResults     []dt.TestResult
	NodeMessageLogs []testing.TraceEvent
}

func (s *JobSessionLogger) FinishSuccess(artifacts JobArtifacts) map[types.Phase]time.Duration {
	s.trackPhaseTime(types.PhaseCompleted)

	s.out <- types.StreamingJobEvent{
		UserID: s.userID,
		Type:   types.TypeResult,
		Result: &types.ResultEvent{
			Outcome:         types.OutcomeSuccess,
			DurationMs:      s.duration(),
			TestResults:     artifacts.TestResults,
			NodeMessageLogs: artifacts.NodeMessageLogs,
		},
	}

	return s.timeSpent
}

func (s *JobSessionLogger) FinishFail(artifacts JobArtifacts, outcome types.Outcome, err error, workerID string) map[types.Phase]time.Duration {
	s.trackPhaseTime(types.PhaseCompleted)
	s.outcome = outcome
	errStr := ""
	if err != nil {
		errStr = err.Error()
	}

	s.out <- types.StreamingJobEvent{
		UserID: s.userID,
		Type:   types.TypeResult,
		Result: &types.ResultEvent{
			Outcome:         s.outcome,
			DurationMs:      s.duration(),
			Error:           errStr,
			TestResults:     artifacts.TestResults,
			FailedWorkerID:  workerID,
			NodeMessageLogs: artifacts.NodeMessageLogs,
		},
	}
	return s.timeSpent
}
