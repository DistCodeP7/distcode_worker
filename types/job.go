package types

import "github.com/DistCodeP7/distcode_worker/worker"

type Job struct {
	ID   int
	Code string
}

type JobResult struct {
	JobID  int
	Result worker.Result
}
