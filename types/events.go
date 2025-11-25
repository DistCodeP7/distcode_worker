package types

import (
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

type StreamingEvent struct {
	Kind     string    `json:"kind"` // "log" | "status" | ...
	WorkerId string    `json:"worker_id"`
	Message  string    `json:"message,omitempty"`
	Status   JobStatus `json:"status,omitempty"`
}

type StreamingJobEvent struct {
	JobUID        uuid.UUID        `json:"job_uid"`
	ProblemId     int              `json:"problem_id"`
	Events        []StreamingEvent `json:"events"`
	UserId        string           `json:"user_id"`
	SequenceIndex int              `json:"sequence_index"`
}

type CancelJobRequest struct {
	JobUID uuid.UUID
}
