package types

type JobRequest struct {
	ProblemId int
	Code      []string
	UserId    int
}

type StreamingEvent struct {
	Kind     string // "stdout" | "stderr" | "error"
	Message  string
	WorkerId string
}

type StreamingJobResult struct {
	JobId         int
	Events        []StreamingEvent
	UserId        int
	SequenceIndex int
}
