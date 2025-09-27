package worker

import (
	"context"
	"fmt"
	"sync"

	"github.com/docker/docker/client"
)

type WorkerManager struct {
	workers     map[string]*Worker // all workers by ID
	idleWorkers []*Worker          // pool of idle workers
	jobPool     map[int][]*Worker  // workers assigned per job
	mu          sync.RWMutex       // lock for accessing workers, idleWorkers and jobPools
	client      *client.Client
}

type WorkerManagerConfig struct {
	Ctx         context.Context
	DockerCli   *client.Client
	WorkerCount int
}

// Creates a new worker manager and instantiates workers
func NewWorkerManager(config *WorkerManagerConfig) (*WorkerManager, error) {
	workerList := make([]*Worker, config.WorkerCount)
	workersMap := make(map[string]*Worker)

	for i := 0; i < config.WorkerCount; i++ {
		worker, err := NewWorker(config.Ctx, config.DockerCli)
		if err != nil {
			return nil, err
		}
		workerList[i] = worker
		workersMap[worker.containerID] = worker
	}

	manager := &WorkerManager{
		workers:     workersMap,
		idleWorkers: workerList,
		jobPool:     make(map[int][]*Worker),
		client:      config.DockerCli,
	}

	return manager, nil
}

func (w *WorkerManager) Shutdown() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	var wg sync.WaitGroup
	var firstErr error
	var errMu sync.Mutex

	for _, worker := range w.workers {
		wg.Add(1)
		go func(wk *Worker) {
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

func (w *WorkerManager) ReserveWorkers(jobId, jobSize int) ([]*Worker, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if len(w.idleWorkers) < jobSize {
		return nil, fmt.Errorf("not enough idle workers currently")
	}

	reserved := w.idleWorkers[:jobSize]
	w.idleWorkers = w.idleWorkers[jobSize:]
	reservedCopy := make([]*Worker, len(reserved)) // Creates copy so caller can't modify
	copy(reservedCopy, reserved)

	w.jobPool[jobId] = reserved

	return reservedCopy, nil
}

// Releases all the workers related to a job and inserts them into idleWorkers
// Errors if the passed jobId isnt in jobPool
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
