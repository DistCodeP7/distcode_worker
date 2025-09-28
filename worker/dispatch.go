package worker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/DistCodeP7/distcode_worker/types"
	"github.com/jonboulle/clockwork"
)

type JobDispatcherConfig struct {
	JobChannel     <-chan types.JobRequest         // receive-only
	ResultsChannel chan<- types.StreamingJobResult // send-only
	WorkerManager  WorkerManagerInterface          // assumes non-nil
	NetworkManager NetworkManagerInterface         // assumes non-nil
	Clock          clockwork.Clock                 // Optional, for testing purposes
	FlushInterval  time.Duration                   // Optional, for testing purposes
}

type JobDispatcher struct {
	jobChannel     <-chan types.JobRequest
	resultsChannel chan<- types.StreamingJobResult
	workerManager  WorkerManagerInterface
	networkManager NetworkManagerInterface
	Clock          clockwork.Clock
	FlushInterval  time.Duration
}

type WorkerManagerInterface interface {
	ReserveWorkers(jobId, jobSize int) ([]WorkerInterface, error)
	ReleaseJob(jobId int) error
	Shutdown() error
}

type NetworkManagerInterface interface {
	CreateAndConnect(ctx context.Context, workers []WorkerInterface) (cleanup func(), err error)
}

func NewJobDispatcher(config JobDispatcherConfig) *JobDispatcher {
	return &JobDispatcher{
		jobChannel:     config.JobChannel,
		resultsChannel: config.ResultsChannel,
		workerManager:  config.WorkerManager,
		networkManager: config.NetworkManager,
		Clock:          config.Clock,
	}
}

// Run starts the job dispatcher loop. It listens for incoming job requests
// and processes each job in a separate goroutine.
func (d *JobDispatcher) Run(ctx context.Context) {
	if d.FlushInterval == 0 {
		d.FlushInterval = 200 * time.Millisecond
	}
	for {
		select {
		case <-ctx.Done():
			return
		case job := <-d.jobChannel:
			go d.processJob(ctx, job)
		}
	}
}

// Utility function to send an error result for a job.
func (d *JobDispatcher) sendJobError(job types.JobRequest, err error) {
	d.resultsChannel <- types.StreamingJobResult{
		JobId:         job.ProblemId,
		UserId:        job.UserId,
		SequenceIndex: -1, // signals end of stream
		Events: []types.StreamingEvent{{
			Kind:    "error",
			Message: err.Error(),
		}},
	}
}

// processJob processes a single job request. It reserves the required number of workers,
// sets up a Docker network, streams logs from each worker, and sends aggregated events to the results channel.
func (d *JobDispatcher) processJob(ctx context.Context, job types.JobRequest) {
	log.Printf("Starting job %v", job.ProblemId)

	requiredWorkers := len(job.Code)
	workers, err := d.requestWorkerReservation(ctx, job.ProblemId, requiredWorkers)
	if err != nil {
		d.sendJobError(job, err)
		return
	}

	jobCtx, cancel := context.WithTimeout(ctx, time.Duration(job.TimeoutLimit)*time.Second)
	defer cancel()

	cleanupNetwork, err := d.networkManager.CreateAndConnect(jobCtx, workers)
	if err != nil {
		log.Printf("Failed to setup network for job %d: %v", job.ProblemId, err)
		_ = d.workerManager.ReleaseJob(job.ProblemId)
		d.sendJobError(job, err)
		return
	}
	defer cleanupNetwork()

	ea := EventAggregator{
		wg:             sync.WaitGroup{},
		muEvents:       sync.Mutex{},
		resultsChannel: d.resultsChannel,
		eventBuf:       make([]types.StreamingEvent, 0),
		clock:          d.Clock,
	}

	cleanupTimedFlush := ea.startPeriodicFlush(jobCtx, job, d.FlushInterval)
	defer cleanupTimedFlush()

	for i, worker := range workers {
		ea.startWorkerLogStreaming(jobCtx, worker, job.Code[i], cancel)
	}

	ea.wg.Wait()

	if jobCtx.Err() != nil {
		if errors.Is(jobCtx.Err(), context.DeadlineExceeded) {
			d.sendJobError(job, fmt.Errorf("job timed out after %ds", job.TimeoutLimit))
		} else {
			d.sendJobError(job, jobCtx.Err())
		}
	} else {
		ea.flushRemainingEvents(job)
	}

	if err = d.workerManager.ReleaseJob(job.ProblemId); err != nil {
		d.sendJobError(job, err)
	}

	log.Printf("Finished job %v", job.ProblemId)
}

// requestWorkerReservation tries to reserve the required number of workers for the job.
// If not enough workers are available, it waits/blocks until they are or the context is cancelled.
func (d *JobDispatcher) requestWorkerReservation(ctx context.Context, jobID, count int) ([]WorkerInterface, error) {
	for {
		workers, err := d.workerManager.ReserveWorkers(jobID, count)
		if err == nil {
			return workers, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-d.Clock.After(200 * time.Millisecond):
			// Retry after a short delay
		}
	}
}

// EventAggregator collects events from one or more workers and sends them to the results channel.
type EventAggregator struct {
	wg             sync.WaitGroup
	muEvents       sync.Mutex
	resultsChannel chan<- types.StreamingJobResult
	eventBuf       []types.StreamingEvent
	clock          clockwork.Clock
}

// startWorkerLogStreaming streams logs from a worker and appends them to the event buffer.
func (e *EventAggregator) startWorkerLogStreaming(ctx context.Context, worker WorkerInterface, code string, cancelJob context.CancelFunc) {
	stdoutCh := make(chan string, 10)
	stderrCh := make(chan string, 10)

	// Stream stdout
	go func(id string) {
		for line := range stdoutCh {
			e.muEvents.Lock()
			e.eventBuf = append(e.eventBuf, types.StreamingEvent{Kind: "stdout", Message: line, WorkerId: id})
			e.muEvents.Unlock()
		}
	}(worker.ID())

	// Stream stderr
	go func(id string) {
		for line := range stderrCh {
			e.muEvents.Lock()
			e.eventBuf = append(e.eventBuf, types.StreamingEvent{Kind: "stderr", Message: line, WorkerId: id})
			e.muEvents.Unlock()
		}
	}(worker.ID())

	e.wg.Add(1)
	// Run the code in the worker
	go func(w WorkerInterface, code string, id string) {
		defer e.wg.Done()
		if err := w.ExecuteCode(ctx, code, stdoutCh, stderrCh); err != nil {
			e.muEvents.Lock()
			e.eventBuf = append(e.eventBuf, types.StreamingEvent{Kind: "error", Message: err.Error(), WorkerId: id})
			e.muEvents.Unlock()
			cancelJob()
		}

		close(stdoutCh)
		close(stderrCh)
	}(worker, code, worker.ID())
}

// startPeriodicFlush periodically sends events to the results channel.
func (e *EventAggregator) startPeriodicFlush(ctx context.Context, job types.JobRequest, tickerInterval time.Duration) func() {
	ticker := e.clock.NewTicker(tickerInterval)
	sequence := 0

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.Chan():
				e.muEvents.Lock()
				if len(e.eventBuf) > 0 {
					eventsCopy := append([]types.StreamingEvent(nil), e.eventBuf...)
					e.eventBuf = nil
					e.resultsChannel <- types.StreamingJobResult{
						JobId:         job.ProblemId,
						UserId:        job.UserId,
						SequenceIndex: sequence,
						Events:        eventsCopy,
					}
					sequence++
				}
				e.muEvents.Unlock()
			}
		}
	}()

	return func() {
		ticker.Stop()
	}
}

// flushRemainingEvents sends any remaining events to the jobs channel.
// It also also sends the special SequenceIndex of -1 to indicate the end of the job's events.
func (e *EventAggregator) flushRemainingEvents(job types.JobRequest) {
	e.muEvents.Lock()
	events := e.eventBuf

	// Ensure events is not nil
	if events == nil {
		events = make([]types.StreamingEvent, 0)
	}
	e.resultsChannel <- types.StreamingJobResult{
		JobId:         job.ProblemId,
		UserId:        job.UserId,
		SequenceIndex: -1,
		Events:        events,
	}
	e.muEvents.Unlock()
}
