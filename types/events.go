package types

import (
	dt "github.com/distcodep7/dsnet/testing/disttest"
	"github.com/google/uuid"
)

// JobStatus is a typed status for jobs, enforcing allowed values
type JobStatus string

const (
	StatusJobSuccess          JobStatus = "JOB_SUCCESS"
	StatusJobFailed           JobStatus = "JOB_FAILED"
	StatusJobTimeout          JobStatus = "JOB_TIMEOUT"
	StatusJobCompilationError JobStatus = "JOB_COMPILATION_ERROR"
	StatusJobCanceled         JobStatus = "JOB_CANCELED"
)

// StreamingEvent is the interface all event variants implement.
type StreamingEvent interface {
	EventKind() string
}

// Kind constants for JSON readability and frontend discriminant
const (
	KindCompiled = "compiled"
	KindLog      = "log"
	KindStatus   = "status"
)

// CompiledEvent indicates compilation outcome for the job (all containers)
type CompiledEvent struct {
	Success        bool   `json:"success"`
	FailedWorkerID string `json:"failed_worker_id,omitempty"`
	Error          string `json:"error,omitempty"`
}

func (e CompiledEvent) EventKind() string { return KindCompiled }

// LogEvent is a buffered log chunk belonging to the job; includes worker id.
type LogEvent struct {
	WorkerID string `json:"worker_id,omitempty"`
	Message  string `json:"message"`
}

func (e LogEvent) EventKind() string { return KindLog }

// StatusEvent is the final job summary
type StatusEvent struct {
	Status         JobStatus       `json:"status"`
	Message        string          `json:"message,omitempty"`
	DurationMillis int64           `json:"duration_millis"`
	TestResults    []dt.TestResult `json:"test_results,omitempty"`
	FailedWorkerID string          `json:"failed_worker_id,omitempty"`
}

func (e StatusEvent) EventKind() string { return KindStatus }

// StreamingJobEvent is the job-level wrapper that is sent over the channel.
type StreamingJobEvent struct {
	JobUID uuid.UUID        `json:"job_uid"`
	Events []StreamingEvent `json:"events"`
	UserId string           `json:"user_id"`
}
