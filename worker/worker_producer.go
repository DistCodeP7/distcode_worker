package worker

import (
	"context"
	"fmt"
	"sync"

	t "github.com/DistCodeP7/distcode_worker/types"
	"github.com/docker/docker/client"
)

type WorkerProducer interface {
	NewWorkers(ctx context.Context, spec []t.NodeSpec) ([]WorkerInterface, error)
}

type DockerWorkerProducer struct {
	dockerCli       *client.Client
	workerImageName string
}

var _ WorkerProducer = (*DockerWorkerProducer)(nil)

func NewDockerWorkerProducer(dockerCli *client.Client, workerImageName string) *DockerWorkerProducer {
	return &DockerWorkerProducer{
		dockerCli:       dockerCli,
		workerImageName: workerImageName,
	}
}

func (dwp *DockerWorkerProducer) NewWorkers(ctx context.Context, specs []t.NodeSpec) ([]WorkerInterface, error) {
	workers := make([]WorkerInterface, 0)
	var mu sync.Mutex
	var wg sync.WaitGroup
	errors := make([]error, 0)

	for _, spec := range specs {
		wg.Go(func() {
			if w, err := NewWorker(ctx, dwp.dockerCli, dwp.workerImageName, spec); err != nil {
				mu.Lock()
				errors = append(errors, err)
				mu.Unlock()
			} else {
				mu.Lock()
				workers = append(workers, w)
				mu.Unlock()
			}
		})
	}
	wg.Wait()

	if len(errors) > 0 {
		for _, w := range workers {
			w.Stop(ctx)
		}

		return nil, fmt.Errorf("errors occurred while creating workers: %v", errors)
	}

	return workers, nil
}
