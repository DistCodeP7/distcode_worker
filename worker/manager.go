package worker

import (
	"context"
	"fmt"
	"sync"

	"github.com/DistCodeP7/distcode_worker/types"
	"github.com/google/uuid"
)

type WorkerManager struct {
	maxWorkers     int
	jobs           map[string][]Worker
	mu             sync.RWMutex
	workerProducer WorkerProducer
}

func NewWorkerManager(maxWorkers int, workerFactory WorkerProducer) (*WorkerManager, error) {
	manager := &WorkerManager{
		maxWorkers:     maxWorkers,
		jobs:           make(map[string][]Worker),
		workerProducer: workerFactory,
	}

	return manager, nil
}

func (wm *WorkerManager) ReserveWorkers(
	jobID uuid.UUID,
	testSpec types.NodeSpec,
	submissionSpecs []types.NodeSpec,
) (WorkUnit, []WorkUnit, error) {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	current := 0

	for _, workers := range wm.jobs {
		current += len(workers)
	}

	totalRequested := 1 + len(submissionSpecs)
	if current+totalRequested > wm.maxWorkers {
		return WorkUnit{}, nil, fmt.Errorf("worker limit exceeded: current=%d, requested=%d, max=%d",
			current, totalRequested, wm.maxWorkers)
	}

	nodeOrder := append([]types.NodeSpec{testSpec}, submissionSpecs...)
	workers, err := wm.workerProducer.NewWorkers(context.Background(), nodeOrder)
	if err != nil {
		return WorkUnit{}, nil, fmt.Errorf("failed to create workers for job %d: %w", jobID, err)
	}

	wm.jobs[jobID.String()] = workers

	testUnit := WorkUnit{
		Spec:   testSpec,
		Worker: workers[0],
	}

	// Indices 1..N are the Submission Containers
	submissionUnits := make([]WorkUnit, len(submissionSpecs))
	for i, spec := range submissionSpecs {
		submissionUnits[i] = WorkUnit{
			Spec:   spec,
			Worker: workers[i+1], // Offset by 1
		}
	}

	return testUnit, submissionUnits, nil
}

func (wm *WorkerManager) removeWorkers(workers []Worker) error {
	// Create waitgroup to stop workers concurrently
	var wg sync.WaitGroup
	errors := make(chan error, len(workers))

	for _, w := range workers {
		wg.Go(func() {
			if err := w.Stop(context.TODO()); err != nil {
				errors <- err
			}
		})
	}

	wg.Wait()
	close(errors)

	if len(errors) > 0 {
		return fmt.Errorf("failed to stop some workers: %v", errors)
	}

	return nil
}

func (wm *WorkerManager) Shutdown() error {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	var allWorkers []Worker
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
