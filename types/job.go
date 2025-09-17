package types

import (
	"context"
	"sync"

	"github.com/docker/docker/client"
)

type JobRequest struct {
	ID   int
	Code string
}

type JobResult struct {
	JobID  int
	Result Result
}

type Result struct {
	Stdout string
	Stderr string
	Err    error
}

type WorkerConfig struct {
	Ctx       context.Context
	DockerCli *client.Client
	Wg        *sync.WaitGroup
	Jobs      <-chan JobRequest
	Results   chan<- JobResult
}
