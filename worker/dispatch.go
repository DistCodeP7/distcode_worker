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
	"github.com/sirupsen/logrus"
)

const (
	EventLog    = "log"
	EventStatus = "status"
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
			d.cancelJob(cancelReq)
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

// sendJobResult sends final job result as two deterministic events: log + status
func (d *JobDispatcher) sendJobResult(job types.JobRequest, aggregatedLogs string, status types.JobStatus, statusMsg string) {
	//TDOO THIS IS SHIT BUT WE NEED TO FIGURE OUT WHAT WE WANT TO DO
	events := []types.StreamingEvent{
		{
			Kind:     EventLog,
			WorkerId: "",
			Message:  aggregatedLogs,
		},
		{
			Kind:     EventStatus,
			WorkerId: "",
			Status:   status,
			Message:  statusMsg,
		},
	}

	d.resultsChannel <- types.StreamingJobEvent{
		JobUID:        job.JobUID,
		ProblemId:     job.ProblemId,
		UserId:        job.UserId,
		SequenceIndex: -1,
		Events:        events,
	}
}

func (d *JobDispatcher) processJob(ctx context.Context, job types.JobRequest) {
	log.Logger.Infof("Starting job %s", job.JobUID.String())
	d.metricsCollector.IncJobTotal()

	workers, err := d.requestWorkers(ctx, job)
	if err != nil {
		d.sendJobResult(job, "", types.StatusJobFailed, err.Error())
		return
	}

	jobCtx, jc := d.createJobContext(ctx, job)
	defer d.cleanupJob(job.JobUID.String())

	startTime := d.clock.Now()

	logs, jobErr := d.runWorkers(jobCtx, job, workers)
	d.handleJobCompletion(jobCtx, job, jc, startTime, logs, jobErr)

	if err := d.workerManager.ReleaseJob(job.JobUID); err != nil {
		log.Logger.Errorf("Error releasing job %s workers: %v", job.JobUID.String(), err)
	}

	log.Logger.Infof("Finished job %s", job.JobUID.String())
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

// Cleanup job entry from activeJobs and cancel context if still present
func (d *JobDispatcher) cleanupJob(jobUID string) {
	d.mu.Lock()
	jc, ok := d.activeJobs[jobUID]
	if ok {
		// ensure cancel called and remove entry
		if jc.Cancel != nil {
			jc.Cancel()
		}
		delete(d.activeJobs, jobUID)
	}
	d.mu.Unlock()
}

// Run all workers for a job and collect logs
func (d *JobDispatcher) runWorkers(ctx context.Context, job types.JobRequest, workers []WorkerInterface) (string, error) {
	var wg sync.WaitGroup
	var logs bytes.Buffer
	var jobErr error
	var mu sync.Mutex

	cleanup, _, netErr := d.networkManager.CreateAndConnect(ctx, workers)
	if netErr != nil {
		return "", fmt.Errorf("failed to set up network: %w", netErr)
	}
	defer func() {
		if cleanup != nil {
			cleanup()
		}
	}()

	for i, worker := range workers {
		wg.Add(1)
		go func(worker WorkerInterface, spec types.NodeSpec) {
			defer wg.Done()
			wLogs, err := d.runWorkerJob(ctx, worker, spec)

			mu.Lock()
			logs.WriteString(fmt.Sprintf("--- Worker %s ---\n%s", worker.ID()[:12], wLogs))
			if err != nil {
				jobErr = err
				d.mu.Lock()
				jc, ok := d.activeJobs[job.JobUID.String()]
				d.mu.Unlock()
				if ok && jc.Cancel != nil {
					jc.Cancel()
				}
			}
			mu.Unlock()
		}(worker, job.Nodes[i])
	}

	wg.Wait()
	return logs.String(), jobErr
}

type BuildError struct {
	WorkerID string
	Err      error
}

func (e *BuildError) Error() string {
	return fmt.Sprintf("build error on worker %s: %v", e.WorkerID[:12], e.Err)
}

// Run build + execute for a single worker
func (d *JobDispatcher) runWorkerJob(ctx context.Context, worker WorkerInterface, spec types.NodeSpec) (string, error) {
	var logBuffer bytes.Buffer
	workerID := worker.ID()

	if spec.BuildCommand != "" {
		log.Logger.Infof("Building code on worker %s: %s", workerID[:12], spec.BuildCommand)
		logBuffer.WriteString(fmt.Sprintf("[Build] Running: %s", spec.BuildCommand))
		if err := worker.ExecuteCommand(ctx, ExecuteCommandOptions{
			Cmd:          spec.BuildCommand,
			OutputWriter: &logBuffer,
		}); err != nil {
			logBuffer.WriteString(fmt.Sprintf("[Build ERROR] %v\n", err))
			return logBuffer.String(), &BuildError{WorkerID: workerID, Err: err}
		}
		logBuffer.WriteString("[Build Success]\n")
	}

	log.Logger.Infof("Executing code on worker %s: %s", workerID[:12], spec.EntryCommand)
	logBuffer.WriteString(fmt.Sprintf("[Execute] Running: %s	", spec.EntryCommand))
	if err := worker.ExecuteCommand(ctx, ExecuteCommandOptions{
		Cmd:          spec.EntryCommand,
		OutputWriter: &logBuffer,
	}); err != nil {
		logBuffer.WriteString(fmt.Sprintf("[Execute ERROR] %v\n", err))
		return logBuffer.String(), fmt.Errorf("execution failed on worker %s: %w", workerID[:12], err)
	}
	logBuffer.WriteString("[Execute Success]\n")

	return logBuffer.String(), nil
}

// Handle job completion based on context or worker error
func (d *JobDispatcher) handleJobCompletion(ctx context.Context, job types.JobRequest, jc *JobCancellation, startTime time.Time, aggregatedLogs string, workerError error) {
	duration := d.clock.Since(startTime).Seconds()
	d.metricsCollector.ObserveJobDuration(duration)
	switch {
	case jc != nil && jc.CanceledByUser.Load():
		d.metricsCollector.IncJobCanceled()
		log.Logger.Infof("Job %d canceled by user request", job.ProblemId)
		d.sendJobResult(job, aggregatedLogs, types.StatusJobCanceled, "job canceled by user")

	case errors.Is(ctx.Err(), context.DeadlineExceeded):
		d.metricsCollector.IncJobTimeout()
		log.Logger.Warnf("Job %d timed out after %ds", job.ProblemId, job.Timeout)
		d.sendJobResult(job, aggregatedLogs, types.StatusJobTimeout, fmt.Sprintf("job timed out after %ds", job.Timeout))

	case workerError != nil:
		switch e := workerError.(type) {
		case *BuildError:
			d.metricsCollector.IncJobFailure()
			log.Logger.WithFields(
				logrus.Fields{
					"job_id":    job.JobUID.String(),
					"worker_id": e.WorkerID[:12],
					"error":     e.Err,
				}).Errorf("Failed build")
			d.sendJobResult(job, aggregatedLogs, types.StatusJobCompilationError, fmt.Sprintf("build error on worker: %s", e.WorkerID[:12]))
		default:
			d.metricsCollector.IncJobFailure()
			log.Logger.Errorf("Job %d failed due to worker error: %v", job.ProblemId, workerError)
			d.sendJobResult(job, aggregatedLogs, types.StatusJobFailed, workerError.Error())
		}

	default:
		d.sendJobResult(job, aggregatedLogs, types.StatusJobSuccess, "completed successfully")
		d.metricsCollector.IncJobSuccess()
	}
}
