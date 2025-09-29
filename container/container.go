package container

import (
	"github.com/DistCodeP7/distcode_worker/worker"
)

type Container struct {
	JobDispatcher *worker.JobDispatcher
}

func NewContainer(jd *worker.JobDispatcher) *Container {
	return &Container{
		JobDispatcher: jd,
	}
}