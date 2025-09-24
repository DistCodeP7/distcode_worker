package types

import (
	"context"
	"sync"

	"github.com/docker/docker/client"
)

type JobRequest struct {
	ProblemId int
	Code      string
	UserId    int
}

type StreamingEvent struct {
	Kind    string // "stdout" | "stderr" | "error"
	Message string
}

type StreamingJobResult struct {
	JobId         int
	Events        []StreamingEvent
	UserId        int
	SequenceIndex int
}

type WorkerConfig struct {
	Ctx       context.Context
	DockerCli *client.Client
	Wg        *sync.WaitGroup
	Jobs      <-chan JobRequest
	Results   chan<- StreamingJobResult
}
