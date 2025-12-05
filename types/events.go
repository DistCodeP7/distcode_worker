package types

import (
	dt "github.com/distcodep7/dsnet/testing/disttest"
	"github.com/google/uuid"
)

type Phase string

const (
	PhasePending   Phase = "PENDING"
	PhaseCompiling Phase = "COMPILING"
	PhaseRunning   Phase = "RUNNING"
	PhaseCompleted Phase = "COMPLETED"
)

type JobEventType string

const (
	TypeLog    JobEventType = "log"
	TypeStatus JobEventType = "status"
	TypeResult JobEventType = "result"
)

type Outcome string

const (
	OutcomeSuccess          Outcome = "SUCCESS"
	OutcomeFailed           Outcome = "FAILED" // Runtime error
	OutcomeCompilationError Outcome = "COMPILATION_ERROR"
	OutcomeTimeout          Outcome = "TIMEOUT"
	OutcomeCancel           Outcome = "CANCELED"
)

type StreamingJobEvent struct {
	JobUID uuid.UUID    `json:"job_uid"`
	UserID string       `json:"user_id"`
	Type   JobEventType `json:"type"` // "log", "status", "result"

	// Payloads (Only one is non-nil)
	Log    *LogEvent    `json:"log,omitempty"`
	Status *StatusEvent `json:"status,omitempty"`
	Result *ResultEvent `json:"result,omitempty"`
}

// 1. LogEvent
type LogEvent struct {
	WorkerID string `json:"worker_id"`
	Phase    Phase  `json:"phase"` // Matches JobSessionLogger.Phase
	Message  string `json:"message"`
}

// 2. StatusEvent
type StatusEvent struct {
	Phase   string `json:"phase"`
	Message string `json:"message"`
}

// 3. ResultEvent
type ResultEvent struct {
	Outcome        Outcome         `json:"outcome"` // SUCCESS, FAILED, etc.
	DurationMs     int64           `json:"duration_ms"`
	TestResults    []dt.TestResult `json:"test_results,omitempty"`
	FailedWorkerID string          `json:"failed_worker_id,omitempty"`
	Error          string          `json:"error,omitempty"`
}
