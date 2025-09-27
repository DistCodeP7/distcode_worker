package worker

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/DistCodeP7/distcode_worker/types"
)

type JobDispatcherConfig struct {
	JobChannel     <-chan types.JobRequest         // receive-only
	ResultsChannel chan<- types.StreamingJobResult // send-only
	WorkerManager  *WorkerManager
	NetworkManager NetworkManager
}

type JobDispatcher struct {
	jobChannel     <-chan types.JobRequest
	resultsChannel chan<- types.StreamingJobResult
	workerManager  *WorkerManager
	networkManager NetworkManager
}

func NewJobDispatcher(config JobDispatcherConfig) *JobDispatcher {
	return &JobDispatcher{
		jobChannel:     config.JobChannel,
		resultsChannel: config.ResultsChannel,
		workerManager:  config.WorkerManager,
		networkManager: config.NetworkManager,
	}
}

// Run starts the job dispatcher loop. It listens for incoming job requests
// and processes each job in a separate goroutine.
func (d *JobDispatcher) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case job := <-d.jobChannel:
			go d.processJob(ctx, job)
		}
	}
}

// processJob processes a single job request. It reserves the required number of workers,
// sets up a Docker network, streams logs from each worker, and sends aggregated events to the results channel.
func (d *JobDispatcher) processJob(ctx context.Context, job types.JobRequest) {
	log.Printf("Starting job %v", job.ProblemId)

	requiredWorkers := len(job.Code)
	workers, err := d.requestWorkerReservation(ctx, job.ProblemId, requiredWorkers)
	if err != nil {
		return
	}

	cleanupNetwork, err := d.networkManager.CreateAndConnect(ctx, workers)
	if err != nil {
		log.Printf("Failed to setup network for job %d: %v", job.ProblemId, err)
		_ = d.workerManager.ReleaseJob(job.ProblemId)
		return
	}
	defer cleanupNetwork()

	ea := EventAggregator{
		wg:             sync.WaitGroup{},
		muEvents:       sync.Mutex{},
		resultsChannel: d.resultsChannel,
		eventBuf:       make([]types.StreamingEvent, 0),
	}

	cleanupTimedFlush := ea.startPeriodicFlush(ctx, job, 200*time.Millisecond)
	defer cleanupTimedFlush()

	for i, worker := range workers {
		ea.startWorkerLogStreaming(ctx, worker, job.Code[i])
	}

	ea.wg.Wait()
	ea.flushRemainingEvents(job)

	if err = d.workerManager.ReleaseJob(job.ProblemId); err != nil {
		log.Fatalf("Job has been released twice, should never happen")
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
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// EventAggregator collects events from one or more workers and sends them to the results channel.
type EventAggregator struct {
	wg             sync.WaitGroup
	muEvents       sync.Mutex
	resultsChannel chan<- types.StreamingJobResult
	eventBuf       []types.StreamingEvent
}

// startWorkerLogStreaming streams logs from a worker and appends them to the event buffer.
func (e *EventAggregator) startWorkerLogStreaming(ctx context.Context, worker WorkerInterface, code string) {
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

		execCtx, cancelExec := context.WithTimeout(ctx, 120*time.Second)
		defer cancelExec()

		if err := w.ExecuteCode(execCtx, code, stdoutCh, stderrCh); err != nil {
			e.muEvents.Lock()
			e.eventBuf = append(e.eventBuf, types.StreamingEvent{Kind: "error", Message: err.Error()})
			e.muEvents.Unlock()
		}

		close(stdoutCh)
		close(stderrCh)
	}(worker, code, worker.ID())
}

// startPeriodicFlush periodically sends events to the results channel.
func (e *EventAggregator) startPeriodicFlush(ctx context.Context, job types.JobRequest, tickerInterval time.Duration) func() {
	ticker := time.NewTicker(tickerInterval)
	sequence := 0

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
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
