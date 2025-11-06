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

type MetricPayload struct {
	StartTime time.Time     `json:"start_time"`
	EndTime   time.Time     `json:"end_time"`
	DeltaTime time.Duration `json:"delta_time"`
}

type StreamingEvent struct {
	Kind     string // "stdout" | "stderr" | "error" | "cancel" | "metric"
	WorkerId string `json:"worker_id"`
	Message  string
	Metric   *MetricPayload
}

type StreamingJobEvent struct {
	JobUID        uuid.UUID `json:"job_uid"`
	ProblemId     int
	Events        []StreamingEvent
	UserId        int
	SequenceIndex int
}
