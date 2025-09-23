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

type StreamingJobResult struct {
	JobId  int
	Result StreamingResult
	UserId int
	Done   bool
}

type StreamingResult struct {
	Stdout []string
	Stderr []string
	Error  string
}

type WorkerConfig struct {
	Ctx       context.Context
	DockerCli *client.Client
	Wg        *sync.WaitGroup
	Jobs      <-chan JobRequest
	Results   chan<- StreamingJobResult
}
