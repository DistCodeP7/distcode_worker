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
	CancelJobChan  <-chan types.CancelJobRequest  // receive-only
	JobChannel     <-chan types.JobRequest        // receive-only
	ResultsChannel chan<- types.StreamingJobEvent // send-only
	MetricsChannel chan<- types.StreamingJobEvent // send-only
	WorkerManager  WorkerManagerInterface         // assumes non-nil
	NetworkManager NetworkManagerInterface        // assumes non-nil
	Clock          clockwork.Clock                // Optional, for testing purposes
	FlushInterval  time.Duration                  // Optional, for testing purposes
}

type JobDispatcher struct {
	cancelJobChan  <-chan types.CancelJobRequest
	jobChannel     <-chan types.JobRequest
	resultsChannel chan<- types.StreamingJobEvent
	metricsChannel chan<- types.StreamingJobEvent
	workerManager  WorkerManagerInterface
	networkManager NetworkManagerInterface
	Clock          clockwork.Clock
	FlushInterval  time.Duration
	mu             sync.Mutex
	activeJobs     map[string]context.CancelFunc
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
	if config.Clock == nil {
		config.Clock = clockwork.NewRealClock()
	}
	if config.FlushInterval == 0 {
		config.FlushInterval = 200 * time.Millisecond
	}
	return &JobDispatcher{
		cancelJobChan:  config.CancelJobChan,
		jobChannel:     config.JobChannel,
		resultsChannel: config.ResultsChannel,
		metricsChannel: config.MetricsChannel,
		workerManager:  config.WorkerManager,
		networkManager: config.NetworkManager,
		Clock:          config.Clock,
		FlushInterval:  config.FlushInterval,
		activeJobs:     make(map[string]context.CancelFunc),
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
		case cancelReq := <-d.cancelJobChan:
			d.cancelJob(cancelReq)
		}
	}
}

func (d *JobDispatcher) cancelJob(req types.CancelJobRequest) {
	d.mu.Lock()
	cancel, ok := d.activeJobs[req.JobUID.String()]
	d.mu.Unlock()

	if !ok {
		log.Printf("Cancel request for unknown or completed job: %s", req.JobUID.String())
		return
	}

	cancel()
}

// Utility function to send an error result for a job.
func (d *JobDispatcher) sendJobError(job types.JobRequest, err error) {
	d.resultsChannel <- types.StreamingJobEvent{
		JobUID:        job.JobUID,
		ProblemId:     job.ProblemId,
		UserId:        job.UserId,
		SequenceIndex: -1, // signals end of stream
		Events: []types.StreamingEvent{{
			Kind:     "error",
			WorkerId: "",
			Message:  err.Error(),
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

	// Per-job context with timeout
	jobCtx, cancel := context.WithTimeout(ctx, time.Duration(job.TimeoutLimit)*time.Second)
	d.mu.Lock()
	d.activeJobs[job.JobUID.String()] = cancel
	d.mu.Unlock()
	defer func() {
		cancel()
		d.mu.Lock()
		delete(d.activeJobs, job.JobUID.String())
		d.mu.Unlock()
	}()

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
		metricsChannel: d.metricsChannel,
		eventBuf:       make([]types.StreamingEvent, 0),
		clock:          d.Clock,
		sequence:       0,
	}

	cleanupTimedFlush, flushDone := ea.startPeriodicFlush(jobCtx, job, d.FlushInterval)
	for i, worker := range workers {
		ea.startWorkerLogStreaming(jobCtx, worker, job.Code[i], cancel, job)
	}

	ea.wg.Wait()
	if jobCtx.Err() != nil {
		if errors.Is(jobCtx.Err(), context.Canceled) {
			log.Printf("Job %d cancelled", job.ProblemId)
		} else if errors.Is(jobCtx.Err(), context.DeadlineExceeded) {
			d.sendJobError(job, fmt.Errorf("job timed out after %ds", job.TimeoutLimit))
		} else {
			d.sendJobError(job, jobCtx.Err())
		}
	}

	if err = d.workerManager.ReleaseJob(job.ProblemId); err != nil {
		d.sendJobError(job, err)
	}

	cleanupTimedFlush()
	<-flushDone

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
	resultsChannel chan<- types.StreamingJobEvent
	metricsChannel chan<- types.StreamingJobEvent // Used for direct metric sends
	eventBuf       []types.StreamingEvent
	clock          clockwork.Clock
	sequence       int // Sequence number for results events
}

// startWorkerLogStreaming streams logs from a worker and appends them to the event buffer.
// It also records execution duration and sends a metric event to the metrics channel if configured.
func (e *EventAggregator) startWorkerLogStreaming(ctx context.Context, worker WorkerInterface, code string, cancelJob context.CancelFunc, job types.JobRequest) {
	stdoutCh := make(chan string, 10)
	stderrCh := make(chan string, 10)

	// Stream stdout
	go func(id string) {
		for line := range stdoutCh {
			e.muEvents.Lock()
			e.eventBuf = append(e.eventBuf, types.StreamingEvent{
				Kind:     "stdout",
				WorkerId: id,
				Message:  line,
			})
			e.muEvents.Unlock()
		}
	}(worker.ID())

	// Stream stderr
	go func(id string) {
		for line := range stderrCh {
			e.muEvents.Lock()
			e.eventBuf = append(e.eventBuf, types.StreamingEvent{
				Kind:     "stderr",
				WorkerId: id,
				Message:  line,
			})
			e.muEvents.Unlock()
		}
	}(worker.ID())

	e.wg.Add(1)
	// Run the code in the worker and record metrics
	go func(w WorkerInterface, code string, id string) {
		defer e.wg.Done()
		startT := time.Now()
		if err := w.ExecuteCode(ctx, code, stdoutCh, stderrCh); err != nil {
			if errors.Is(ctx.Err(), context.Canceled) {
				log.Printf("Worker %s execution cancelled", id)
			} else {
				log.Printf("Worker %s execution error: %v", id, err)
				e.muEvents.Lock()
				e.eventBuf = append(e.eventBuf, types.StreamingEvent{
					Kind:     "error",
					WorkerId: id,
					Message:  err.Error(),
				})
				e.muEvents.Unlock()
				cancelJob()
			}
		}
		endT := time.Now()

		// Send metric directly through metrics channel if configured
		if e.metricsChannel != nil {
			metricEvent := types.StreamingJobEvent{
				JobUID:    job.JobUID,
				ProblemId: job.ProblemId,
				UserId:    job.UserId,
				Events: []types.StreamingEvent{{
					Kind:     "metric",
					WorkerId: id,
					Metric: &types.MetricPayload{
						StartTime: startT,
						EndTime:   endT,
						DeltaTime: endT.Sub(startT),
					},
				}},
			}
			// Non-blocking send - if channel is full, we drop the metric rather than
			// risk impacting worker execution or results delivery
			select {
			case e.metricsChannel <- metricEvent:
			default:
				log.Printf("Dropped metric for worker %s (channel full)", id)
			}
		}

		close(stdoutCh)
		close(stderrCh)
	}(worker, code, worker.ID())
}

func (e *EventAggregator) startPeriodicFlush(ctx context.Context, job types.JobRequest, tickerInterval time.Duration) (cleanup func(), done <-chan struct{}) {
	ticker := e.clock.NewTicker(tickerInterval)
	sequence := 0
	doneCh := make(chan struct{})

	var once sync.Once

	go func() {
		defer close(doneCh)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.Chan():
				e.muEvents.Lock()
				if len(e.eventBuf) > 0 {
					eventsCopy := append([]types.StreamingEvent(nil), e.eventBuf...)
					e.eventBuf = nil
					e.resultsChannel <- types.StreamingJobEvent{
						JobUID:        job.JobUID,
						ProblemId:     job.ProblemId,
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

	cleanupFunc := func() {
		once.Do(func() {
			ticker.Stop()
			e.flushRemainingEvents(job)
		})
	}

	return cleanupFunc, doneCh
}

// flushRemainingEvents sends any remaining events to both the results and metrics channels.
// It also sends the special SequenceIndex of -1 to indicate the end of the job's events.
func (e *EventAggregator) flushRemainingEvents(job types.JobRequest) {
	e.muEvents.Lock()
	events := e.eventBuf

	// Ensure events is not nil
	if events == nil {
		events = make([]types.StreamingEvent, 0)
	}

	var metricEvents, resultEvents []types.StreamingEvent
	for _, evt := range events {
		switch evt.Kind {
		case "result":
			resultEvents = append(resultEvents, evt)
		case "metric":
			metricEvents = append(metricEvents, evt)
		}
	}

	// Send final non-metric events
	e.resultsChannel <- types.StreamingJobEvent{
		JobUID:        job.JobUID,
		ProblemId:     job.ProblemId,
		UserId:        job.UserId,
		SequenceIndex: -1,
		Events:        resultEvents,
	}

	// Send final metric events
	if e.metricsChannel != nil {
		e.metricsChannel <- types.StreamingJobEvent{
			JobUID:        job.JobUID,
			ProblemId:     job.ProblemId,
			UserId:        job.UserId,
			SequenceIndex: -1,
			Events:        metricEvents,
		}
	}

	e.muEvents.Unlock()
}
