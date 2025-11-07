package types

import (
	"time"

	"github.com/google/uuid"
)

type CancelJobRequest struct {
	JobUID uuid.UUID
}

type JobRequest struct {
	JobUID       uuid.UUID
	ProblemId    int
	Code         []string
	UserId       int
	TimeoutLimit int // in seconds
}

type WorkerMetricPayload struct {
	StartTime time.Time     `json:"start_time"`
	EndTime   time.Time     `json:"end_time"`
	DeltaTime time.Duration `json:"delta_time"`
}

type JobMetricPayload struct {
	SchedulingDelay time.Duration `json:"scheduling_delay,omitempty"`
	ExecutionTime   time.Duration `json:"execution_time,omitempty"`
	TotalTime       time.Duration `json:"total_time,omitempty"`
}

type StreamingEvent struct {
	Kind        string // "stdout" | "stderr" | "error" | "cancel" | "metric"
	WorkerId    *string `json:"worker_id"`
	Message     *string `json:"message,omitempty"`
	WorkerMetric *WorkerMetricPayload `json:"worker_metric,omitempty"`
	JobMetric   *JobMetricPayload  `json:"job_metric,omitempty"`
}

type StreamingJobEvent struct {
	JobUID        uuid.UUID `json:"job_uid"`
	ProblemId     int
	Events        []StreamingEvent
	UserId        int
	SequenceIndex int
}
