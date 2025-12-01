package types

import (
	"encoding/json"

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
	Status         JobStatus `json:"status"`
	Message        string    `json:"message,omitempty"`
	DurationMillis int64     `json:"duration_millis"`
	FailedWorkerID string    `json:"failed_worker_id,omitempty"`
}

func (e StatusEvent) EventKind() string { return KindStatus }

// StreamingJobEvent is the job-level wrapper that is sent over the channel.
type StreamingJobEvent struct {
	JobUID uuid.UUID        `json:"job_uid"`
	Events []StreamingEvent `json:"events"`
	UserId string           `json:"user_id"`
}

// Custom JSON marshaling for StreamingJobEvent so each event carries a discriminant
// and concrete event fields are inline for easy consumption by JS/TS frontend.
func (s StreamingJobEvent) MarshalJSON() ([]byte, error) {
	out := struct {
		JobUID uuid.UUID         `json:"job_uid"`
		Events []json.RawMessage `json:"events"`
		UserId string            `json:"user_id"`
	}{
		JobUID: s.JobUID,
		UserId: s.UserId,
	}

	events := make([]json.RawMessage, 0, len(s.Events))
	for _, ev := range s.Events {
		var raw []byte
		var err error
		switch v := ev.(type) {
		case CompiledEvent:
			payload, err := json.Marshal(v)
			if err != nil {
				return nil, err
			}
			raw, err = json.Marshal(struct {
				Kind string `json:"kind"`
				CompiledEvent
			}{
				Kind:          KindCompiled,
				CompiledEvent: v,
			})
			if err != nil {
				return nil, err
			}
			_ = payload // no-op but kept for symmetry
		case LogEvent:
			raw, err = json.Marshal(struct {
				Kind string `json:"kind"`
				LogEvent
			}{
				Kind:     KindLog,
				LogEvent: v,
			})
			if err != nil {
				return nil, err
			}
		case StatusEvent:
			raw, err = json.Marshal(struct {
				Kind string `json:"kind"`
				StatusEvent
			}{
				Kind:        KindStatus,
				StatusEvent: v,
			})
			if err != nil {
				return nil, err
			}
		default:
			// try generic marshalling — fallback for future event types
			payload, err := json.Marshal(v)
			if err != nil {
				return nil, err
			}
			// attempt to obtain a kind via StreamingEvent.EventKind
			kind := v.EventKind()
			raw, err = json.Marshal(struct {
				Kind string          `json:"kind"`
				Data json.RawMessage `json:"data"`
			}{
				Kind: kind,
				Data: payload,
			})
			if err != nil {
				return nil, err
			}
		}
		if err != nil {
			return nil, err
		}
		events = append(events, json.RawMessage(raw))
	}

	out.Events = events
	return json.Marshal(out)
}
