package worker

import (
	"context"
	"fmt"
	"sync"
)

type WorkerManager struct {
	workers     map[string]WorkerInterface
	idleWorkers []WorkerInterface
	jobPool     map[int][]WorkerInterface
	mu          sync.RWMutex
}

func NewWorkerManager(initialWorkers []WorkerInterface) (*WorkerManager, error) {
	workersMap := make(map[string]WorkerInterface, len(initialWorkers))
	idleWorkersCopy := make([]WorkerInterface, len(initialWorkers))

	for i, worker := range initialWorkers {
		if worker.ID() == "" {
			return nil, fmt.Errorf("worker at index %d has an empty ID", i)
		}
		workersMap[worker.ID()] = worker
		idleWorkersCopy[i] = worker
	}

	manager := &WorkerManager{
		workers:     workersMap,
		idleWorkers: idleWorkersCopy,
		jobPool:     make(map[int][]WorkerInterface),
	}

	return manager, nil
}

func (w *WorkerManager) ReserveWorkers(jobId, jobSize int) ([]WorkerInterface, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if len(w.idleWorkers) < jobSize {
		return nil, fmt.Errorf("not enough idle workers currently")
	}

	reserved := w.idleWorkers[:jobSize]
	w.idleWorkers = w.idleWorkers[jobSize:]
	reservedCopy := make([]WorkerInterface, len(reserved))
	copy(reservedCopy, reserved)

	w.jobPool[jobId] = reserved

	return reservedCopy, nil
}

func (w *WorkerManager) Shutdown() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	var wg sync.WaitGroup
	var firstErr error
	var errMu sync.Mutex

	for _, worker := range w.workers {
		wg.Add(1)
		go func(wk WorkerInterface) {
			defer wg.Done()
			if err := wk.Stop(context.Background()); err != nil {
				errMu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				errMu.Unlock()
			}
		}(worker)
	}

	wg.Wait()
	w.workers = nil
	w.idleWorkers = nil
	w.jobPool = nil
	return firstErr
}

func (w *WorkerManager) ReleaseJob(jobId int) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	reservedWorkers, ok := w.jobPool[jobId]
	if !ok {
		return fmt.Errorf("jobId %d not found", jobId)
	}

	delete(w.jobPool, jobId)
	w.idleWorkers = append(w.idleWorkers, reservedWorkers...)
	return nil
}
