package worker

import (
	"context"
	"fmt"
	"sync"

	t "github.com/DistCodeP7/distcode_worker/types"
	"github.com/google/uuid"
)

type WorkerManager struct {
	maxWorkers     int
	jobs           map[string][]WorkerInterface
	mu             sync.RWMutex
	workerProducer WorkerProducer
}

func NewWorkerManager(maxWorkers int, workerFactory WorkerProducer) (*WorkerManager, error) {
	manager := &WorkerManager{
		maxWorkers:     maxWorkers,
		jobs:           make(map[string][]WorkerInterface),
		workerProducer: workerFactory,
	}

	return manager, nil
}

func (wm *WorkerManager) ReserveWorkers(jobID uuid.UUID, specs []t.NodeSpec) ([]WorkerInterface, error) {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	current := 0

	for _, workers := range wm.jobs {
		current += len(workers)
	}

	if current+len(specs) > wm.maxWorkers {
		return nil, fmt.Errorf("worker limit exceeded: current=%d, requested=%d, max=%d",
			current, len(specs), wm.maxWorkers)
	}

	workers, err := wm.workerProducer.NewWorkers(context.TODO(), specs)
	if err != nil {
		return nil, fmt.Errorf("failed to create workers for job %d: %w", jobID, err)
	}

	wm.jobs[jobID.String()] = workers

	return workers, nil
}

func (wm *WorkerManager) removeWorkers(workers []WorkerInterface) error {
	// Create waitgroup to stop workers concurrently
	var wg sync.WaitGroup
	errors := make(chan error, len(workers))
	defer close(errors)

	for _, w := range workers {
		wg.Go(func() {
			if err := w.Stop(context.TODO()); err != nil {
				errors <- err
			}
		})
	}

	wg.Wait()

	if len(errors) > 0 {
		return fmt.Errorf("failed to stop some workers: %v", errors)
	}

	return nil
}

func (wm *WorkerManager) Shutdown() error {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	var allWorkers []WorkerInterface
	for key, workers := range wm.jobs {
		allWorkers = append(allWorkers, workers...)
		delete(wm.jobs, key)
	}

	if err := wm.removeWorkers(allWorkers); err != nil {
		return fmt.Errorf("failed to remove workers during shutdown: %w", err)
	}

	// Help GC
	wm.jobs = nil
	return nil
}

func (wm *WorkerManager) ReleaseJob(jobID uuid.UUID) error {
	wm.mu.Lock()
	reservedWorkers, ok := wm.jobs[jobID.String()]
	if !ok {
		return fmt.Errorf("jobId %d not found", jobID)
	}

	delete(wm.jobs, jobID.String())
	wm.mu.Unlock()

	if err := wm.removeWorkers(reservedWorkers); err != nil {
		return fmt.Errorf("failed to release workers for job %d: %v", jobID, err)
	}

	return nil
}
