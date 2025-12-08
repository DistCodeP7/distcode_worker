package worker

import (
	"context"
	"fmt"
	"sync"

	"github.com/DistCodeP7/distcode_worker/dockercli"
	t "github.com/DistCodeP7/distcode_worker/types"
)

type WorkerProducer interface {
	NewWorkers(ctx context.Context, spec []t.NodeSpec) ([]WorkerInterface, error)
}

type DockerWorkerProducer struct {
	dockerCli       dockercli.Client
	workerImageName string
}

var _ WorkerProducer = (*DockerWorkerProducer)(nil)

func NewDockerWorkerProducer(dockerCli dockercli.Client, workerImageName string) *DockerWorkerProducer {
	return &DockerWorkerProducer{
		dockerCli:       dockerCli,
		workerImageName: workerImageName,
	}
}

// NewWorkers creates new Docker-based workers based on the provided specifications.
// The index order of specs is preserved in the returned slice of WorkerInterface.
func (dwp *DockerWorkerProducer) NewWorkers(ctx context.Context, specs []t.NodeSpec) ([]WorkerInterface, error) {
	workers := make([]WorkerInterface, len(specs))

	var wg sync.WaitGroup
	var errMu sync.Mutex
	var errors []error

	for i, spec := range specs {
		// Capture loop variable
		idx := i
		s := spec

		wg.Go(func() {
			w, err := NewWorker(ctx, dwp.dockerCli, dwp.workerImageName, s)
			if err != nil {
				errMu.Lock()
				errors = append(errors, err)
				errMu.Unlock()
			} else {
				workers[idx] = w
			}
		})
	}
	wg.Wait()

	if len(errors) > 0 {
		for _, w := range workers {
			if w != nil {
				_ = w.Stop(ctx)
			}
		}
		return nil, fmt.Errorf("errors occurred while creating workers: %v", errors)
	}

	return workers, nil
}
