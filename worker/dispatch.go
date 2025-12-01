package worker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/DistCodeP7/distcode_worker/log"
	"github.com/DistCodeP7/distcode_worker/metrics"
	"github.com/DistCodeP7/distcode_worker/types"
	"github.com/google/uuid"
	"github.com/jonboulle/clockwork"
)

// WorkerManagerInterface manages worker reservation and lifecycle
type WorkerManagerInterface interface {
	ReserveWorkers(jobID uuid.UUID, specs []types.NodeSpec) ([]WorkerInterface, error)
	ReleaseJob(jobID uuid.UUID) error
	Shutdown() error
}

// NetworkManagerInterface creates and connects workers to a network
type NetworkManagerInterface interface {
	CreateAndConnect(ctx context.Context, workers []WorkerInterface) (cleanup func(), networkName string, err error)
}

// JobCancellation holds per-job cancel func and a flag for user cancellation
type JobCancellation struct {
	Cancel         context.CancelFunc
	CanceledByUser atomic.Bool
}

// JobDispatcher orchestrates job execution across workers
type JobDispatcher struct {
	cancelJobChan    <-chan types.CancelJobRequest
	jobChannel       <-chan types.JobRequest
	resultsChannel   chan<- types.StreamingJobEvent
	workerManager    WorkerManagerInterface
	networkManager   NetworkManagerInterface
	metricsCollector metrics.JobMetricsCollector
	clock            clockwork.Clock

	mu         sync.Mutex
	activeJobs map[string]*JobCancellation
}

func NewJobDispatcher(
	cancelJobChan <-chan types.CancelJobRequest,
	jobChannel <-chan types.JobRequest,
	resultsChannel chan<- types.StreamingJobEvent,
	workerManager WorkerManagerInterface,
	networkManager NetworkManagerInterface,
	metricsCollector metrics.JobMetricsCollector,
	opts ...func(*JobDispatcher),
) *JobDispatcher {
	d := &JobDispatcher{
		cancelJobChan:    cancelJobChan,
		jobChannel:       jobChannel,
		resultsChannel:   resultsChannel,
		workerManager:    workerManager,
		networkManager:   networkManager,
		metricsCollector: metricsCollector,
		clock:            clockwork.NewRealClock(),
		activeJobs:       make(map[string]*JobCancellation),
	}

	for _, opt := range opts {
		opt(d)
	}
	return d
}

// Run starts the dispatcher loop
func (d *JobDispatcher) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case job := <-d.jobChannel:
			go d.processJob(ctx, job)
		case cancelReq := <-d.cancelJobChan:
			go d.cancelJob(cancelReq)
		}
	}
}

// Cancel a running job: mark as user-cancelled then call cancel func
func (d *JobDispatcher) cancelJob(req types.CancelJobRequest) {
	d.mu.Lock()
	jc, ok := d.activeJobs[req.JobUID.String()]
	d.mu.Unlock()

	if !ok {
		log.Logger.Warnf("Cancel request for unknown or completed job: %s", req.JobUID.String())
		return
	}

	jc.CanceledByUser.Store(true)
	if jc.Cancel != nil {
		jc.Cancel()
	}
}

// helper to send events in one StreamingJobEvent
func (d *JobDispatcher) sendJobEvents(job types.JobRequest, events ...types.StreamingEvent) {
	d.resultsChannel <- types.StreamingJobEvent{
		JobUID: job.JobUID,
		UserId: job.UserId,
		Events: events,
	}
}

// processJob is the top-level orchestration: compile all, send compiled event, then execute and stream logs
func (d *JobDispatcher) processJob(ctx context.Context, job types.JobRequest) {
	log.Logger.Infof("Starting job %s", job.JobUID.String())
	d.metricsCollector.IncJobTotal()

	workers, err := d.requestWorkers(ctx, job)
	if err != nil {
		// immediate failure reserve workers
		d.sendFinalStatus(job, types.StatusJobFailed, fmt.Sprintf("failed to reserve workers: %v", err), "")
		return
	}

	jobCtx, jc := d.createJobContext(ctx, job)
	defer d.cleanupJob(job.JobUID)

	startTime := d.clock.Now()

	// set up network once
	cleanup, _, netErr := d.networkManager.CreateAndConnect(jobCtx, workers)
	if netErr != nil {
		d.sendFinalStatus(job, types.StatusJobFailed, fmt.Sprintf("failed to set up network: %v", netErr), "")
		return
	}
	defer func() {
		if cleanup != nil {
			cleanup()
		}
	}()

	// 1) Compile step: ensure each container compiles before running anything
	compiledSuccess, failedWorker, compileErr := d.compileAll(jobCtx, workers, job.Nodes)
	// Send compiled event regardless of success/failure
	d.sendJobEvents(job, types.CompiledEvent{
		Success:        compiledSuccess,
		FailedWorkerID: failedWorker,
		Error: func() string {
			if compileErr != nil {
				return compileErr.Error()
			}
			return ""
		}(),
	})

	if !compiledSuccess {
		// short-circuit on compilation failure and send final status
		d.metricsCollector.IncJobFailure()
		duration := d.clock.Since(startTime).Milliseconds()
		d.sendJobEvents(job, types.StatusEvent{
			Status:         types.StatusJobCompilationError,
			Message:        "compilation failed",
			DurationMillis: duration,
			FailedWorkerID: failedWorker,
		})
		return
	}

	// 2) Execution step: run all containers concurrently, buffer logs per worker and send job-level log events (buffered)
	execErr := d.executeAll(jobCtx, job, workers, job.Nodes, jc)

	// 3) Final status
	if jc != nil && jc.CanceledByUser.Load() {
		d.metricsCollector.IncJobCanceled()
		duration := d.clock.Since(startTime).Milliseconds()
		d.sendJobEvents(job, types.StatusEvent{
			Status:         types.StatusJobCanceled,
			Message:        "job canceled by user",
			DurationMillis: duration,
		})
		return
	}

	if errors.Is(jobCtx.Err(), context.DeadlineExceeded) {
		d.metricsCollector.IncJobTimeout()
		duration := d.clock.Since(startTime).Milliseconds()
		d.sendJobEvents(job, types.StatusEvent{
			Status:         types.StatusJobTimeout,
			Message:        fmt.Sprintf("job timed out after %ds", job.Timeout),
			DurationMillis: duration,
		})
		return
	}

	if execErr != nil {
		d.metricsCollector.IncJobFailure()
		duration := d.clock.Since(startTime).Milliseconds()
		// if execErr is a BuildError we would have already short-circuited; treat other errors as job failure
		d.sendJobEvents(job, types.StatusEvent{
			Status:         types.StatusJobFailed,
			Message:        execErr.Error(),
			DurationMillis: duration,
		})
		return
	}

	d.metricsCollector.IncJobSuccess()
	duration := d.clock.Since(startTime).Milliseconds()
	d.sendJobEvents(job, types.StatusEvent{
		Status:         types.StatusJobSuccess,
		Message:        "completed successfully",
		DurationMillis: duration,
	})
}

// compileAll runs build command on each worker sequentially (or concurrently if you prefer).
// It buffers build output per worker and returns on the first build failure.
func (d *JobDispatcher) compileAll(ctx context.Context, workers []WorkerInterface, specs []types.NodeSpec) (bool, string, error) {
	var wg sync.WaitGroup
	type buildResult struct {
		workerID string
		out      string
		err      error
	}
	results := make(chan buildResult, len(workers))

	// Run builds concurrently but short-circuit on first failure detection.
	for i := range workers {
		wg.Add(1)
		go func(worker WorkerInterface, spec types.NodeSpec) {
			defer wg.Done()
			var buf bytes.Buffer
			workerID := worker.ID()
			if spec.BuildCommand == "" {
				// no build required -> success
				results <- buildResult{workerID: workerID, out: "", err: nil}
				return
			}
			log.Logger.Infof("Building on worker %s: %s", workerID[:12], spec.BuildCommand)
			err := worker.ExecuteCommand(ctx, ExecuteCommandOptions{
				Cmd:          spec.BuildCommand,
				OutputWriter: &buf,
			})
			if err != nil {
				results <- buildResult{workerID: workerID, out: buf.String(), err: err}
				return
			}
			results <- buildResult{workerID: workerID, out: buf.String(), err: nil}
		}(workers[i], specs[i])
	}

	// wait for all goroutines to finish or until context cancelled
	go func() {
		wg.Wait()
		close(results)
	}()

	// collect results; fail fast: pick first non-nil error as failure
	var firstErr error
	var failedWorker string
	var aggregatedBuildLogs bytes.Buffer
	for r := range results {
		aggregatedBuildLogs.WriteString(fmt.Sprintf("--- Worker %s build output ---\n%s\n", r.workerID[:12], r.out))
		if r.err != nil && firstErr == nil {
			firstErr = r.err
			failedWorker = r.workerID
			// do not return immediately; still drain channel to keep goroutines clean
		}
	}

	if firstErr != nil {
		return false, failedWorker, firstErr
	}
	return true, "", nil
}

// executeAll runs all workers' entry commands concurrently, buffers logs per worker,
// and sends job-level LogEvent containing buffered logs per worker when each worker finishes.
func (d *JobDispatcher) executeAll(ctx context.Context, job types.JobRequest, workers []WorkerInterface, specs []types.NodeSpec, jc *JobCancellation) error {
	var wg sync.WaitGroup
	errCh := make(chan error, len(workers))

	for i := range workers {
		wg.Add(1)
		go func(worker WorkerInterface, spec types.NodeSpec) {
			defer wg.Done()
			var buf bytes.Buffer
			workerID := worker.ID()

			log.Logger.Infof("Executing on worker %s: %s", workerID[:12], spec.EntryCommand)
			buf.WriteString(fmt.Sprintf("[Execute] Running: %s\n", spec.EntryCommand))

			err := worker.ExecuteCommand(ctx, ExecuteCommandOptions{
				Cmd:          spec.EntryCommand,
				OutputWriter: &buf,
			})
			if err != nil {
				buf.WriteString(fmt.Sprintf("[Execute ERROR] %v\n", err))
				// send buffered logs for this worker before propagating the error
				d.sendJobEvents(job, types.LogEvent{
					WorkerID: workerID,
					Message:  buf.String(),
				})
				errCh <- fmt.Errorf("execution failed on worker %s: %w", workerID[:12], err)
				// ensure we cancel the job context so other workers stop
				if jc != nil && jc.Cancel != nil {
					jc.Cancel()
				}
				return
			}

			buf.WriteString("[Execute Success]\n")
			// send buffered logs for this worker as a single job-level log event
			d.sendJobEvents(job, types.LogEvent{
				WorkerID: workerID,
				Message:  buf.String(),
			})
		}(workers[i], specs[i])
	}

	// wait and collect errors
	wg.Wait()
	close(errCh)

	// if any error occurred return the first one
	for e := range errCh {
		if e != nil {
			return e
		}
	}
	return nil
}

// Reserve workers with retry
func (d *JobDispatcher) requestWorkers(ctx context.Context, job types.JobRequest) ([]WorkerInterface, error) {
	for {
		workers, err := d.workerManager.ReserveWorkers(job.JobUID, job.Nodes)
		if err == nil {
			return workers, nil
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-d.clock.After(200 * time.Millisecond):
			// Retry after delay
		}
	}
}

// Create a per-job context with timeout and register JobControl
func (d *JobDispatcher) createJobContext(ctx context.Context, job types.JobRequest) (context.Context, *JobCancellation) {
	jobCtx, cancel := context.WithTimeout(ctx, time.Duration(job.Timeout)*time.Second)
	jc := &JobCancellation{Cancel: cancel}

	d.mu.Lock()
	d.activeJobs[job.JobUID.String()] = jc
	d.mu.Unlock()

	return jobCtx, jc
}

func (d *JobDispatcher) cleanupJob(jobUID uuid.UUID) {
	d.mu.Lock()
	jc, ok := d.activeJobs[jobUID.String()]
	if ok {
		if jc.Cancel != nil {
			jc.Cancel()
		}
		delete(d.activeJobs, jobUID.String())
	}
	d.mu.Unlock()
	if err := d.workerManager.ReleaseJob(jobUID); err != nil {
		log.Logger.Errorf("Failed to release workers for job %s: %v", jobUID, err)
	}
}

// sendFinalStatus is a convenience for immediate final job events outside the normal flow
func (d *JobDispatcher) sendFinalStatus(job types.JobRequest, status types.JobStatus, msg string, failedWorker string) {
	d.sendJobEvents(job, types.StatusEvent{
		Status:         status,
		Message:        msg,
		DurationMillis: 0,
		FailedWorkerID: failedWorker,
	})
}
