package types

import "github.com/google/uuid"

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

type StreamingEvent struct {
	Kind     string // "stdout" | "stderr" | "error" | "cancel"
	Message  string
	WorkerId string
}

type StreamingJobResult struct {
	JobUID        uuid.UUID `json:"job_uid"`
	ProblemId     int
	Events        []StreamingEvent
	UserId        int
	SequenceIndex int
}
