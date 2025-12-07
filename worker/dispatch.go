package worker

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/DistCodeP7/distcode_worker/db"
	"github.com/DistCodeP7/distcode_worker/endpoints/metrics"
	"github.com/DistCodeP7/distcode_worker/jobsession"
	"github.com/DistCodeP7/distcode_worker/log"
	"github.com/DistCodeP7/distcode_worker/types"
	t "github.com/distcodep7/dsnet/testing"
	dt "github.com/distcodep7/dsnet/testing/disttest"
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
	CanceledByUser bool
}

// JobDispatcher orchestrates job execution across workers
type JobDispatcher struct {
	cancelJobChan    <-chan types.CancelJobRequest
	jobChannel       <-chan types.Job
	resultsChannel   chan<- types.StreamingJobEvent
	jobStore         db.JobStore
	workerManager    WorkerManagerInterface
	networkManager   NetworkManagerInterface
	metricsCollector metrics.JobMetricsCollector
	clock            clockwork.Clock

	mu           sync.Mutex
	activeJobs   map[string]*JobCancellation
	activeJobsWg sync.WaitGroup
}

func NewJobDispatcher(
	cancelJobChan <-chan types.CancelJobRequest,
	jobChannel <-chan types.Job,
	resultsChannel chan<- types.StreamingJobEvent,
	workerManager WorkerManagerInterface,
	networkManager NetworkManagerInterface,
	jobStore db.JobStore,
	metricsCollector metrics.JobMetricsCollector,
	opts ...func(*JobDispatcher),
) *JobDispatcher {
	d := &JobDispatcher{
		cancelJobChan:    cancelJobChan,
		jobChannel:       jobChannel,
		resultsChannel:   resultsChannel,
		workerManager:    workerManager,
		networkManager:   networkManager,
		jobStore:         jobStore,
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
			log.Logger.Info("Dispatcher stopping, waiting for active jobs to finish...")
			d.activeJobsWg.Wait()
			log.Logger.Info("All jobs finished. Dispatcher exiting.")
			return
		case job := <-d.jobChannel:
			d.activeJobsWg.Go(func() {
				d.metricsCollector.IncCurrentJobs()
				d.processJob(ctx, job)
				d.metricsCollector.DecCurrentJobs()
			})
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

	d.mu.Lock()
	if jc != nil {
		jc.CanceledByUser = true
		if jc.Cancel != nil {
			jc.Cancel()
		}
	}
	d.mu.Unlock()
}

// processJob is the top-level orchestration
func (d *JobDispatcher) processJob(ctx context.Context, job types.Job) {
	log.Logger.Infof("Starting job %s", job.JobUID.String())
	d.metricsCollector.IncJobTotal()

	session := jobsession.NewJobSession(job, d.resultsChannel)
	session.SetPhase(types.PhasePending, "Reserving workers...")

	var testResults []dt.TestResult
	defer func() {
		saveCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err := d.jobStore.SaveResult(
			saveCtx,
			job.JobUID,
			session.GetOutcome(),
			testResults,
			session.GetBufferedLogs(),
			session.StartTime(),
		)

		if err != nil {
			log.Logger.Errorf("Failed to save job results to DB: %v", err)
		}
	}()

	defer d.cleanupJob(job.JobUID)
	workers, err := d.requestWorkers(ctx, job)
	if err != nil {
		d.metricsCollector.IncJobFailure()
		session.FinishFail(jobsession.JobArtifacts{}, types.OutcomeFailed, fmt.Errorf("failed to reserve workers: %w", err), "")
		return
	}

	jobCtx, jc := d.createJobContext(ctx, job)

	cleanup, _, netErr := d.networkManager.CreateAndConnect(jobCtx, workers)
	if netErr != nil {
		d.metricsCollector.IncJobFailure()
		session.FinishFail(jobsession.JobArtifacts{}, types.OutcomeFailed, fmt.Errorf("failed to set up network: %w", netErr), "")
		return
	}
	defer func() {
		if cleanup != nil {
			cleanup()
		}
	}()

	workerSpec, err := d.pairWorkerSpecs(workers, job.Nodes)
	if err != nil {
		d.metricsCollector.IncJobFailure()
		session.FinishFail(jobsession.JobArtifacts{}, types.OutcomeFailed, fmt.Errorf("failed to pair workers and specs: %w", err), "")
		return
	}

	session.SetPhase(types.PhaseCompiling, "Compiling code...")
	compiledSuccess, failedWorker, compileErr := d.compileAll(jobCtx, session, workerSpec)

	if !compiledSuccess {
		d.metricsCollector.IncJobFailure()
		session.FinishFail(jobsession.JobArtifacts{}, types.OutcomeCompilationError, compileErr, failedWorker)
		return
	}

	session.SetPhase(types.PhaseRunning, "Compilation successful. Executing...")
	execErr := d.executeAll(jobCtx, session, workerSpec, jc)

	testContainer := d.getTestWorker(workers)
	testContainer.ExecuteCommand(ctx, ExecuteCommandOptions{
		Cmd:          "ls -R",
		OutputWriter: session.NewLogWriter(testContainer.Alias()),
	})

	testResults = d.collectTestResults(ctx, workers)
	testLogs := d.collectTestLogs(ctx, workers)

	jobArtifacts := jobsession.JobArtifacts{
		TestResults: testResults,
		TestLogs:    testLogs,
	}

	d.mu.Lock()
	canceledByUser := jc.CanceledByUser
	d.mu.Unlock()

	if canceledByUser {
		d.metricsCollector.IncJobCanceled()
		session.FinishFail(jobArtifacts, types.OutcomeCancel, errors.New("job canceled by user"), "")
	} else if errors.Is(jobCtx.Err(), context.DeadlineExceeded) {
		d.metricsCollector.IncJobTimeout()
		session.FinishFail(jobArtifacts, types.OutcomeTimeout, fmt.Errorf("job timed out after %ds", job.Timeout), "")
	} else if execErr != nil {
		d.metricsCollector.IncJobFailure()
		session.FinishFail(jobArtifacts, types.OutcomeFailed, execErr, "")
	} else {
		d.metricsCollector.IncJobSuccess()
		session.FinishSuccess(jobArtifacts)
	}
}

type WorkUnit struct {
	Spec   types.NodeSpec
	Worker WorkerInterface
}

func (d *JobDispatcher) pairWorkerSpecs(worker []WorkerInterface, specs []types.NodeSpec) ([]WorkUnit, error) {
	workerByAlias := make(map[string]WorkerInterface, len(worker))

	for _, w := range worker {
		workerByAlias[w.Alias()] = w
	}

	var workUnits []WorkUnit
	for _, spec := range specs {
		w, ok := workerByAlias[spec.Alias]
		if !ok {
			return nil, fmt.Errorf("no worker found for alias %s", spec.Alias)
		}
		workUnits = append(workUnits, WorkUnit{
			Spec:   spec,
			Worker: w,
		})
	}
	return workUnits, nil
}

// compileAll runs build command on each worker sequentially/concurrently and streams logs
func (d *JobDispatcher) compileAll(
	ctx context.Context,
	session *jobsession.JobSessionLogger,
	workUnits []WorkUnit,
) (bool, string, error) {

	var wg sync.WaitGroup
	type buildResult struct {
		workerID string
		err      error
	}
	results := make(chan buildResult, len(workUnits))

	for _, workUnit := range workUnits {
		if workUnit.Spec.BuildCommand == "" {
			continue
		}

		wg.Add(1)
		go func(w WorkerInterface, s types.NodeSpec) {
			defer wg.Done()

			logWriter := session.NewLogWriter(w.Alias())
			fmt.Fprintf(logWriter, "Running build: %s\n", s.BuildCommand)

			err := w.ExecuteCommand(ctx, ExecuteCommandOptions{
				Cmd:          s.BuildCommand,
				OutputWriter: logWriter,
			})

			if err != nil {
				results <- buildResult{workerID: w.ID(), err: err}
			}
		}(workUnit.Worker, workUnit.Spec)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	// If any build fails, the whole job fails
	for r := range results {
		if r.err != nil {
			return false, r.workerID, r.err
		}
	}

	return true, "", nil
}

// executeAll runs all workers' entry commands concurrently and streams logs
func (d *JobDispatcher) executeAll(
	ctx context.Context,
	session *jobsession.JobSessionLogger,
	workUnits []WorkUnit,
	jc *JobCancellation,
) error {

	var wg sync.WaitGroup
	errCh := make(chan error, len(workUnits))

	for _, workUnit := range workUnits {

		wg.Add(1)
		go func(w WorkerInterface, s types.NodeSpec) {
			defer wg.Done()

			logWriter := session.NewLogWriter(w.Alias())
			fmt.Fprintf(logWriter, "Executing: %s\n", s.EntryCommand)

			err := w.ExecuteCommand(ctx, ExecuteCommandOptions{
				Cmd:          s.EntryCommand,
				OutputWriter: logWriter,
			})
			if err != nil {
				fmt.Fprintf(logWriter, "Execution Error: %v\n", err)
				errCh <- fmt.Errorf("execution failed on worker %s: %w", w.ID()[:12], err)

				// Ensure we cancel the job context so other workers stop immediately
				if jc != nil && jc.Cancel != nil {
					jc.Cancel()
				}
			}
		}(workUnit.Worker, workUnit.Spec)
	}

	wg.Wait()
	close(errCh)

	for e := range errCh {
		if e != nil {
			return e
		}
	}
	return nil
}

func (d *JobDispatcher) getTestWorker(workers []WorkerInterface) WorkerInterface {
	for _, w := range workers {
		if w.Alias() == "test-container" {
			return w
		}
	}
	return nil
}

// collectTestResults helper to read json from the test container
func (d *JobDispatcher) collectTestResults(ctx context.Context, workers []WorkerInterface) []dt.TestResult {
	// 1. Find the worker (extracted to helper to avoid repetition)
	testWorker := d.getTestWorker(workers)
	if testWorker == nil {
		return nil
	}

	// 2. Read raw bytes
	data, err := testWorker.ReadFile(ctx, "app/tmp/test_results.json")
	if err != nil {
		log.Logger.Warnf("Failed to read test results file: %v", err)
		return nil
	}

	// 3. Parse Standard JSON (Array of objects)
	var results []dt.TestResult
	if err := json.Unmarshal(data, &results); err != nil {
		log.Logger.Errorf("Failed to unmarshal test results: %v", err)
		return nil
	}

	return results
}

func (d *JobDispatcher) collectTestLogs(ctx context.Context, workers []WorkerInterface) []t.LogEntry {
	testWorker := d.getTestWorker(workers)
	if testWorker == nil {
		return nil
	}

	data, err := testWorker.ReadFile(ctx, "app/tmp/trace_log.jsonl")
	if err != nil {
		log.Logger.Warnf("Failed to read logs: %v", err)
		return nil
	}

	var logs []t.LogEntry
	scanner := bufio.NewScanner(bytes.NewReader(data))

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}

		var entry t.LogEntry
		if err := json.Unmarshal(line, &entry); err != nil {
			log.Logger.Warnf("Skipping malformed log line: %v", err)
			continue
		}
		logs = append(logs, entry)
	}

	return logs
}

// Reserve workers with retry
func (d *JobDispatcher) requestWorkers(ctx context.Context, job types.Job) ([]WorkerInterface, error) {
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
func (d *JobDispatcher) createJobContext(ctx context.Context, job types.Job) (context.Context, *JobCancellation) {
	jobCtx, cancel := context.WithTimeout(ctx, time.Duration(job.Timeout)*time.Second)
	jc := &JobCancellation{Cancel: cancel, CanceledByUser: false}

	d.mu.Lock()
	d.activeJobs[job.JobUID.String()] = jc
	d.mu.Unlock()

	return jobCtx, jc
}

// Cleanup job: remove from active jobs and release workers
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
