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

type JobResult struct {
	JobId  int
	Result Result
	UserId int
}

type Result struct {
	Stdout string
	Stderr string
	Err    string
}

type WorkerConfig struct {
	Ctx       context.Context
	DockerCli *client.Client
	Wg        *sync.WaitGroup
	Jobs      <-chan JobRequest
	Results   chan<- JobResult
}
