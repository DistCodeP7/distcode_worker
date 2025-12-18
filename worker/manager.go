package worker

import (
	"context"
	"fmt"
	"sync"

	"github.com/DistCodeP7/distcode_worker/log"
	"github.com/DistCodeP7/distcode_worker/types"
	"github.com/google/uuid"
)

type WorkerManager struct {
	maxWorkers     int
	runningWorkers int
	jobs           map[string][]Worker
	mu             sync.Mutex
	workerProducer WorkerProducer
	capacitySignal chan struct{}
}

func NewWorkerManager(maxWorkers int, workerFactory WorkerProducer) (*WorkerManager, error) {
	return &WorkerManager{
		maxWorkers:     maxWorkers,
		jobs:           make(map[string][]Worker),
		workerProducer: workerFactory,
		capacitySignal: make(chan struct{}, 1),
	}, nil
}

type ErrorReserveTooManyWorkers struct {
	Requested int
	Capacity  int
}

func (e *ErrorReserveTooManyWorkers) Error() string {
	return fmt.Sprintf("requested %d workers but max capacity is %d", e.Requested, e.Capacity)
}

// ReserveSlotsOrWait reserves the given number of worker slots, blocking until they are available.
func (wm *WorkerManager) ReserveSlotsOrWait(ctx context.Context, count int) error {
	if count > wm.maxWorkers {
		return &ErrorReserveTooManyWorkers{Requested: count, Capacity: wm.maxWorkers}
	}

	for {
		wm.mu.Lock()
		if wm.runningWorkers+count <= wm.maxWorkers {
			wm.runningWorkers += count
			wm.mu.Unlock()
			return nil
		}
		wm.mu.Unlock()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-wm.capacitySignal:
			continue
		}
	}
}

// CreateWorkers creates workers for the given job and specs.
// Assumes slots have already been reserved via ReserveSlotsOrWait.
func (wm *WorkerManager) CreateWorkers(
	ctx context.Context,
	jobID uuid.UUID,
	testSpec types.NodeSpec,
	submissionSpecs []types.NodeSpec,
) (WorkUnit, []WorkUnit, error) {
	if len(submissionSpecs)+1 > wm.maxWorkers {
		panic("CreateWorkers called with more workers than max capacity")
	}

	nodeOrder := append([]types.NodeSpec{testSpec}, submissionSpecs...)
	workers, err := wm.workerProducer.NewWorkers(ctx, nodeOrder)
	if err != nil {
		return WorkUnit{}, nil, err
	}

	wm.mu.Lock()
	wm.jobs[jobID.String()] = workers
	wm.mu.Unlock()

	testUnit := WorkUnit{Spec: testSpec, Worker: workers[0]}
	submissionUnits := make([]WorkUnit, len(submissionSpecs))
	for i, spec := range submissionSpecs {
		submissionUnits[i] = WorkUnit{Spec: spec, Worker: workers[i+1]}
	}

	return testUnit, submissionUnits, nil
}

// DiscardReservation releases the reserved slots without creating workers.
func (wm *WorkerManager) DiscardReservation(count int) {
	wm.mu.Lock()
	wm.runningWorkers -= count
	wm.mu.Unlock()

	select {
	case wm.capacitySignal <- struct{}{}:
	default:
	}
}

// ReleaseJob stops and removes all workers associated with the given job ID.
func (wm *WorkerManager) ReleaseJob(jobID uuid.UUID) error {
	wm.mu.Lock()
	workers, ok := wm.jobs[jobID.String()]
	if !ok {
		wm.mu.Unlock()
		return fmt.Errorf("jobId %s not found", jobID)
	}
	delete(wm.jobs, jobID.String())
	wm.mu.Unlock()

	err := wm.removeWorkers(workers)

	if err != nil {
		log.Logger.Errorf("Error releasing resources for job %s: %v", jobID.String(), err)
	} else {
		log.Logger.Debugf("Released resources for job %s", jobID.String())
	}

	wm.mu.Lock()
	wm.runningWorkers -= len(workers)
	wm.mu.Unlock()

	select {
	case wm.capacitySignal <- struct{}{}:
	default:
	}

	return err
}

// removeWorkers stops and removes the given workers.
func (wm *WorkerManager) removeWorkers(workers []Worker) error {
	var wg sync.WaitGroup
	errors := make(chan error, len(workers))

	for _, w := range workers {
		wg.Go(func() {
			if err := w.Stop(context.Background()); err != nil {
				errors <- err
			}
		})
	}
	wg.Wait()
	close(errors)
	if len(errors) > 0 {
		return fmt.Errorf("errors stopping workers")
	}
	return nil
}

// Shutdown gracefully stops all workers managed by the WorkerManager.
func (wm *WorkerManager) Shutdown() error {
	wm.mu.Lock()
	defer wm.mu.Unlock()
	var allWorkers []Worker
	for k, workers := range wm.jobs {
		allWorkers = append(allWorkers, workers...)
		delete(wm.jobs, k)
	}
	return wm.removeWorkers(allWorkers)
}
