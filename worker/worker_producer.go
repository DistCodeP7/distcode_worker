package worker

import (
	"context"
	"fmt"
	"sync"

	"github.com/DistCodeP7/distcode_worker/dockercli"
	t "github.com/DistCodeP7/distcode_worker/types"
	"github.com/DistCodeP7/distcode_worker/utils"
	"github.com/docker/docker/api/types/container"
)

type WorkerProducer interface {
	NewWorkers(ctx context.Context, spec []t.NodeSpec) ([]Worker, error)
}

type DockerWorkerProducer struct {
	dockerCli       dockercli.Client
	workerImageName string
	ressourceConfig container.Resources
}

var _ WorkerProducer = (*DockerWorkerProducer)(nil)

type WorkerProducerOption func(*DockerWorkerProducer)

func WithResources(res container.Resources) WorkerProducerOption {
	return func(dwp *DockerWorkerProducer) {
		dwp.ressourceConfig = res
	}
}

func NewDockerWorkerProducer(dockerCli dockercli.Client, workerImageName string, opts ...WorkerProducerOption) *DockerWorkerProducer {
	dwp := &DockerWorkerProducer{
		dockerCli:       dockerCli,
		workerImageName: workerImageName,
		ressourceConfig: container.Resources{
			CPUShares:      512,
			NanoCPUs:       1_000_000_000,
			Memory:         512 * 1024 * 1024,
			MemorySwap:     1024 * 1024 * 1024,
			PidsLimit:      utils.PtrInt64(1024),
			OomKillDisable: utils.PtrBool(false),
			Ulimits: []*container.Ulimit{
				{Name: "cpu", Soft: 30, Hard: 30},
				{Name: "nofile", Soft: 1024, Hard: 1024},
			},
		},
	}

	for _, opt := range opts {
		opt(dwp)
	}

	return dwp
}

// NewWorkers creates new Docker-based workers based on the provided specifications.
// The index order of specs is preserved in the returned slice of WorkerInterface.
func (dwp *DockerWorkerProducer) NewWorkers(ctx context.Context, specs []t.NodeSpec) ([]Worker, error) {
	workers := make([]Worker, len(specs))

	var wg sync.WaitGroup
	var errMu sync.Mutex
	var errors []error

	for i, spec := range specs {
		// Capture loop variable
		idx := i
		s := spec

		wg.Go(func() {
			w, err := NewWorker(ctx, dwp.dockerCli, dwp.workerImageName, s, dwp.ressourceConfig)
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
