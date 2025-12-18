package types

import (
	"github.com/distcodep7/dsnet/testing"
	dt "github.com/distcodep7/dsnet/testing/disttest"
)

type Phase string

const (
	PhaseDebugging          Phase = "DEBUGGING"
	PhaseReserving          Phase = "RESERVING"
	PhaseConfiguringNetwork Phase = "CONFIGURING_NETWORK"
	PhasePending            Phase = "PENDING"
	PhaseCompiling          Phase = "COMPILING"
	PhaseRunning            Phase = "RUNNING"
	PhaseCompleted          Phase = "COMPLETED"
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
	OutcomeFailed           Outcome = "FAILED"
	OutcomeCompilationError Outcome = "COMPILATION_ERROR"
	OutcomeTimeout          Outcome = "TIMEOUT"
	OutcomeCanceled         Outcome = "CANCELED"
)

type StreamingJobEvent struct {
	UserID string       `json:"user_id"`
	Type   JobEventType `json:"type"` // "log", "status", "result"
	Log    *LogEvent    `json:"log,omitempty"`
	Status *StatusEvent `json:"status,omitempty"`
	Result *ResultEvent `json:"result,omitempty"`
}

type LogEvent struct {
	WorkerID string `json:"worker_id"`
	Phase    Phase  `json:"phase"`
	Message  string `json:"message"`
}

type StatusEvent struct {
	Phase   string `json:"phase"`
	Message string `json:"message"`
}

type ResultEvent struct {
	Outcome         Outcome              `json:"outcome"`
	DurationMs      int64                `json:"duration_ms"`
	TestResults     []dt.TestResult      `json:"test_results,omitempty"`
	NodeMessageLogs []testing.TraceEvent `json:"node_message_logs,omitempty"`
	FailedWorkerID  string               `json:"failed_worker_id,omitempty"`
	Error           string               `json:"error,omitempty"`
}
