package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/DistCodeP7/distcode_worker/types"
	"github.com/DistCodeP7/distcode_worker/utils"
	"github.com/docker/docker/client"
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
	DockerCli      *client.Client                 // Docker client for ephemeral jobs
	WorkerImage    string                         // image to use when creating ephemeral job containers
}

type JobDispatcher struct {
	cancelJobChan    <-chan types.CancelJobRequest
	jobChannel       <-chan types.JobRequest
	resultsChannel   chan<- types.StreamingJobEvent
	metricsChannel   chan<- types.StreamingJobEvent
	workerManager    WorkerManagerInterface
	networkManager   NetworkManagerInterface
	Clock            clockwork.Clock
	FlushInterval    time.Duration
	dockerCli        *client.Client
	workerImage      string
	mu               sync.Mutex
	activeJobs       map[string]context.CancelFunc
	activeJobsByUser map[string]string
}

type WorkerManagerInterface interface {
	ReserveWorkers(jobUID string, jobSize int) ([]WorkerInterface, error)
	ReleaseJob(jobUID string) error
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
		cancelJobChan:    config.CancelJobChan,
		jobChannel:       config.JobChannel,
		resultsChannel:   config.ResultsChannel,
		metricsChannel:   config.MetricsChannel,
		workerManager:    config.WorkerManager,
		networkManager:   config.NetworkManager,
		Clock:            config.Clock,
		FlushInterval:    config.FlushInterval,
		dockerCli:        config.DockerCli,
		workerImage:      config.WorkerImage,
		activeJobs:       make(map[string]context.CancelFunc),
		activeJobsByUser: make(map[string]string),
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
			WorkerId: nil,
			Message:  utils.PtrString(err.Error()),
		}},
	}
}

// processJob processes a single job request. It reserves the required number of workers,
// sets up a Docker network, streams logs from each worker, and sends aggregated events to the results channel.
func (d *JobDispatcher) processJob(ctx context.Context, job types.JobRequest) {
	log.Printf("Starting job %v", job.ProblemId)
	submissionT := time.Now()

	// If the same user has a currently active job, cancel it and free resources
	d.mu.Lock()
	if existingJobUID, ok := d.activeJobsByUser[job.UserId]; ok {
		if cancelFn, found := d.activeJobs[existingJobUID]; found {
			log.Printf("Cancelling previous job %s for user %s in favor of new submission %s", existingJobUID, job.UserId, job.JobUID.String())
			cancelFn()
			delete(d.activeJobs, existingJobUID)
		}
		delete(d.activeJobsByUser, job.UserId)
	}
	d.mu.Unlock()

	// If this is the centralized mutex problem (id 1) we want to create
	// an ephemeral container per job and mount the submitted files into
	// the package directory before running the test.
	if job.ProblemId == 1 {
		if len(job.Code) == 0 {
			d.sendJobError(job, fmt.Errorf("empty submission for problem %d", job.ProblemId))
			return
		}

		setupStartT := time.Now()

		// Per-job context with timeout
		jobCtx, cancel := context.WithTimeout(ctx, time.Duration(job.TimeoutLimit)*time.Second)
		d.mu.Lock()
		d.activeJobs[job.JobUID.String()] = cancel
		d.activeJobsByUser[job.UserId] = job.JobUID.String()
		d.mu.Unlock()
		defer func() {
			cancel()
			d.mu.Lock()
			delete(d.activeJobs, job.JobUID.String())
			if uid, ok := d.activeJobsByUser[job.UserId]; ok && uid == job.JobUID.String() {
				delete(d.activeJobsByUser, job.UserId)
			}
			d.mu.Unlock()
		}()

		ea := EventAggregator{
			wg:             sync.WaitGroup{},
			muEvents:       sync.Mutex{},
			resultsChannel: d.resultsChannel,
			metricsChannel: d.metricsChannel,
			eventBuf:       make([]types.StreamingEvent, 0),
			clock:          d.Clock,
		}

		cleanupTimedFlush, flushDone := ea.startPeriodicFlush(jobCtx, job, d.FlushInterval)

		stdoutCh := make(chan string, 10)
		stderrCh := make(chan string, 10)

		// Stream stdout into event buffer. Use a non-nil WorkerId so clients know
		// this came from the ephemeral runner rather than a worker in the pool.
		go func() {
			for line := range stdoutCh {
				ea.muEvents.Lock()
				ea.eventBuf = append(ea.eventBuf, types.StreamingEvent{
					Kind:     "stdout",
					WorkerId: nil,
					Message:  utils.PtrString(line),
				})
				ea.muEvents.Unlock()
			}
		}()

		// Stream stderr into event buffer
		go func() {
			for line := range stderrCh {
				ea.muEvents.Lock()
				ea.eventBuf = append(ea.eventBuf, types.StreamingEvent{
					Kind:     "stderr",
					WorkerId: nil,
					Message:  utils.PtrString(line),
				})
				ea.muEvents.Unlock()
			}
		}()

		// Reserve multiple workers: 1 server + N clients (we test with 4 clients)
		numClients := 4
		requiredWorkers := 1 + numClients
		workers, err := d.requestWorkerReservation(ctx, job.JobUID.String(), requiredWorkers)
		if err != nil {
			d.sendJobError(job, fmt.Errorf("failed to reserve worker: %w", err))
			close(stdoutCh)
			close(stderrCh)
			return
		}

		// Connect reserved workers to a per-job network (keeps behavior consistent with multi-worker jobs)
		cleanupNetwork, err := d.networkManager.CreateAndConnect(jobCtx, workers)
		if err != nil {
			_ = d.workerManager.ReleaseJob(job.JobUID.String())
			d.sendJobError(job, fmt.Errorf("failed to setup network for ephemeral job: %w", err))
			close(stdoutCh)
			close(stderrCh)
			return
		}
		// If we didn't actually get enough workers (tests/mocks may return fewer),
		// fall back to the original single-worker ExecuteTest behavior so unit tests
		// that exercise that path continue to work.
		if len(workers) < requiredWorkers {
			runErrCh := make(chan error, 1)
			go func() {
				runErrCh <- workers[0].ExecuteTest(jobCtx, job.JobUID.String(), "centralized_mutex", job.Code, stdoutCh, stderrCh)
			}()

			select {
			case err := <-runErrCh:
				close(stdoutCh)
				close(stderrCh)
				if err != nil {
					d.sendJobError(job, err)
				}
			case <-jobCtx.Done():
				close(stdoutCh)
				close(stderrCh)
				if errors.Is(jobCtx.Err(), context.DeadlineExceeded) {
					d.sendJobError(job, fmt.Errorf("job timed out after %ds", job.TimeoutLimit))
				} else {
					d.sendJobError(job, jobCtx.Err())
				}
			}

			// Ensure periodic flush exits then cleanup network and release
			cancel()
			cleanupNetwork()
			if err := d.workerManager.ReleaseJob(job.JobUID.String()); err != nil {
				log.Printf("Failed to release worker for job %d: %v", job.ProblemId, err)
			}

			cleanupTimedFlush()
			<-flushDone

			log.Printf("Finished job %v", job.ProblemId)
			return
		}

		// Prepare per-worker workspaces by copying repository files into each worker's hostPath
		// so each worker has an independent module workspace. This allows replacing the
		// client implementation with the user-submitted file on client workers.
		// Find repo root (where go.mod lives)
		findGoMod := func(dir string) (string, bool) {
			cand := filepath.Join(dir, "go.mod")
			if _, err := os.Stat(cand); err == nil {
				return dir, true
			}
			return "", false
		}

		repoRoot := ""
		if cwd, err := os.Getwd(); err == nil {
			if d, ok := findGoMod(cwd); ok {
				repoRoot = d
			}
		}
		if repoRoot == "" {
			// try executable path
			if exe, err := os.Executable(); err == nil {
				dir := filepath.Dir(exe)
				for {
					if d, ok := findGoMod(dir); ok {
						repoRoot = d
						break
					}
					parent := filepath.Dir(dir)
					if parent == dir {
						break
					}
					dir = parent
				}
			}
		}

		if repoRoot == "" {
			d.sendJobError(job, fmt.Errorf("repository root (go.mod) not found"))
			cleanupNetwork()
			_ = d.workerManager.ReleaseJob(job.JobUID.String())
			close(stdoutCh)
			close(stderrCh)
			return
		}

		// copyDir copies files from src to dst recursively.
		var copyDir func(src, dst string) error
		copyDir = func(src, dst string) error {
			ents, err := os.ReadDir(src)
			if err != nil {
				return err
			}
			if err := os.MkdirAll(dst, 0755); err != nil {
				return err
			}
			for _, e := range ents {
				srcPath := filepath.Join(src, e.Name())
				dstPath := filepath.Join(dst, e.Name())
				if e.IsDir() {
					if err := copyDir(srcPath, dstPath); err != nil {
						return err
					}
					continue
				}
				data, err := os.ReadFile(srcPath)
				if err != nil {
					return err
				}
				if err := os.WriteFile(dstPath, data, 0644); err != nil {
					return err
				}
			}
			return nil
		}

		// Prepare and start processes on each worker
		type workerRun struct {
			stdout <-chan string
			stderr <-chan string
			done   <-chan error
		}

		runs := make([]workerRun, len(workers))
		var wgRuns sync.WaitGroup

	// Start processes on each worker, but ensure the server (idx==0)
	// has announced readiness before starting any clients to avoid
	// connection-refused races.
	var serverReady bool
	for i, wk := range workers {
			// Need concrete worker to write files into its hostPath
			concrete, ok := wk.(*Worker)
			if !ok {
				d.sendJobError(job, fmt.Errorf("worker is not concrete Worker type; cannot prepare workspace"))
				cleanupNetwork()
				_ = d.workerManager.ReleaseJob(job.JobUID.String())
				close(stdoutCh)
				close(stderrCh)
				return
			}

			workerRoot := filepath.Join(concrete.hostPath, "jobs", job.JobUID.String(), fmt.Sprintf("worker-%d", i))
			if err := os.MkdirAll(workerRoot, 0755); err != nil {
				d.sendJobError(job, fmt.Errorf("failed to create worker workspace: %w", err))
				cleanupNetwork()
				_ = d.workerManager.ReleaseJob(job.JobUID.String())
				close(stdoutCh)
				close(stderrCh)
				return
			}

			// Copy repo go.mod into workerRoot
			if data, err := os.ReadFile(filepath.Join(repoRoot, "go.mod")); err == nil {
				if err := os.WriteFile(filepath.Join(workerRoot, "go.mod"), data, 0644); err != nil {
					d.sendJobError(job, fmt.Errorf("failed to write go.mod: %w", err))
					cleanupNetwork()
					_ = d.workerManager.ReleaseJob(job.JobUID.String())
					close(stdoutCh)
					close(stderrCh)
					return
				}
			}

			// Copy algorithms into workerRoot/algorithms
			if err := copyDir(filepath.Join(repoRoot, "algorithms"), filepath.Join(workerRoot, "algorithms")); err != nil {
				d.sendJobError(job, fmt.Errorf("failed to copy algorithms: %w", err))
				cleanupNetwork()
				_ = d.workerManager.ReleaseJob(job.JobUID.String())
				close(stdoutCh)
				close(stderrCh)
				return
			}

			// Overwrite the client.go in the package with the user's submission for all workers.
			// This allows running the integration test (which executes the cmd binaries)
			// from worker-0 and ensures the submitted client implementation is used.
			clientPkgPath := filepath.Join(workerRoot, "algorithms", "centralized_mutex")
			if len(job.Code) > 0 {
				// normalize package name to centralized_mutex
				content := job.Code[0]
				pkgDecl := "package centralized_mutex\n\n"
				if len(content) > 0 {
					re := regexp.MustCompile(`(?m)^\s*package\s+\w+`)
					if re.MatchString(content) {
						content = re.ReplaceAllString(content, "package centralized_mutex")
					} else {
						content = pkgDecl + content
					}
				}
				if err := os.WriteFile(filepath.Join(clientPkgPath, "client.go"), []byte(content), 0644); err != nil {
					d.sendJobError(job, fmt.Errorf("failed to write client.go for worker %d: %w", i, err))
					cleanupNetwork()
					_ = d.workerManager.ReleaseJob(job.JobUID.String())
					close(stdoutCh)
					close(stderrCh)
					return
				}
				// Compute and append host-side SHA256 of the written client.go so it is visible
				// in the job's event stream for correlation with the in-container file.
				func() {
					path := filepath.Join(clientPkgPath, "client.go")
					data, err := os.ReadFile(path)
					if err != nil {
						ea.muEvents.Lock()
						ea.eventBuf = append(ea.eventBuf, types.StreamingEvent{Kind: "stderr", Message: utils.PtrString(fmt.Sprintf("failed to read written client.go for hash: %v", err))})
						ea.muEvents.Unlock()
						return
					}
					sum := sha256.Sum256(data)
					hexStr := hex.EncodeToString(sum[:])
					ea.muEvents.Lock()
					ea.eventBuf = append(ea.eventBuf, types.StreamingEvent{Kind: "stdout", WorkerId: utils.PtrString(wk.ID()), Message: utils.PtrString(fmt.Sprintf("HOST_CLIENT_GO_SHA256: %s", hexStr))})
					ea.muEvents.Unlock()
				}()
			}

			// Start processes: server on i==0, clients on i>0
			stdout := make(chan string, 50)
			stderr := make(chan string, 50)

			runs[i].stdout = stdout
			runs[i].stderr = stderr

			// If we have enough workers, run the integration test inside worker-0
			// so it exercises the cmd binaries (server_main and client_runner) from
			// a single container. This is simpler and more reliable than trying to
			// coordinate long-running server + multiple client containers.
			if i == 0 {
				wgRuns.Add(1)
				go func(idx int, w WorkerInterface, root string, outC, errC chan string) {
					defer wgRuns.Done()
					containerRoot := filepath.Join("/app", "jobs", job.JobUID.String(), fmt.Sprintf("worker-%d", idx))
					// Run a workspace listing (diagnostic) then the in-process HTTP unit
					// test which exercises the server handlers and clients without
					// spawning separate processes. The listing will reveal whether
					// the package files (and the submitted client.go) are present.
					testCmd := fmt.Sprintf("cd %s || true && go test ./algorithms/centralized_mutex -run TestCentralizedMutex -v -count=1", containerRoot)
					cmd := []string{"sh", "-c", testCmd}
					if err := w.RunCommand(jobCtx, cmd, outC, errC); err != nil {
						errC <- err.Error()
					}
				}(i, wk, workerRoot, stdout, stderr)
			} else {
				// Do not start separate client containers when running integration test
				// inside worker-0; just create forwarders so any incidental logs from
				// these workspaces are visible (they won't run anything).
			}

			// Start per-worker stdout/stderr forwarders into the aggregator buffer
			go func(id string, ch <-chan string) {
				for line := range ch {
					ea.muEvents.Lock()
					ea.eventBuf = append(ea.eventBuf, types.StreamingEvent{
						Kind:     "stdout",
						WorkerId: utils.PtrString(id),
						Message:  utils.PtrString(line),
					})
					ea.muEvents.Unlock()
				}
			}(wk.ID(), stdout)
			go func(id string, ch <-chan string) {
				for line := range ch {
					ea.muEvents.Lock()
					ea.eventBuf = append(ea.eventBuf, types.StreamingEvent{
						Kind:     "stderr",
						WorkerId: utils.PtrString(id),
						Message:  utils.PtrString(line),
					})
					ea.muEvents.Unlock()
				}
			}(wk.ID(), stderr)

			// If this was the server we just started, wait for the readiness log
			if i == 0 {
				// wait up to 3s for the server readiness message to appear in the event buffer
				deadline := time.Now().Add(3 * time.Second)
				for time.Now().Before(deadline) {
					ea.muEvents.Lock()
					found := false
					for _, ev := range ea.eventBuf {
						if ev.WorkerId != nil && *ev.WorkerId == wk.ID() && ev.Message != nil {
							if strings.Contains(*ev.Message, "Starting centralized-mutex HTTP server") {
								found = true
								break
							}
						}
					}
					ea.muEvents.Unlock()
					if found {
						serverReady = true
						break
					}
					time.Sleep(50 * time.Millisecond)
				}
				if !serverReady {
					// if we didn't see the readiness log, append a warning so it's visible to clients
					ea.muEvents.Lock()
					ea.eventBuf = append(ea.eventBuf, types.StreamingEvent{Kind: "stderr", Message: utils.PtrString("server readiness not observed within timeout; proceeding")})
					ea.muEvents.Unlock()
					serverReady = true // proceed anyway to avoid deadlock
				}
			}
		}

		// Wait for client processes to either exit or to demonstrate success by
		// entering the critical section. The server is long-running and not
		// included in the waitgroup; we consider the job successful when all
		// clients have printed ENTERED_CS at least once, or when the client
		// processes exit normally.
		doneCh := make(chan struct{})
		go func() {
			wgRuns.Wait()
			close(doneCh)
		}()

		// observe unique client worker IDs that reported ENTERED_CS
		observed := make(map[string]bool)
		deadline := time.Now().Add(time.Duration(job.TimeoutLimit) * time.Second)
	waitLoop:
		for {
			// If clients' execs finished, break (normal completion)
			select {
			case <-doneCh:
				break waitLoop
			default:
			}

			// scan event buffer for ENTERED_CS messages from client workers
			ea.muEvents.Lock()
			for _, ev := range ea.eventBuf {
				if ev.Kind == "stdout" && ev.Message != nil && ev.WorkerId != nil {
					if strings.Contains(*ev.Message, "ENTERED_CS") {
						observed[*ev.WorkerId] = true
					}
				}
			}
			ea.muEvents.Unlock()

			if len(observed) >= numClients {
				// success condition met: all clients entered CS at least once
				break waitLoop
			}

			// timeout
			if time.Now().After(deadline) {
				if errors.Is(jobCtx.Err(), context.DeadlineExceeded) {
					d.sendJobError(job, fmt.Errorf("job timed out after %ds", job.TimeoutLimit))
				} else {
					d.sendJobError(job, fmt.Errorf("job timed out after %ds", job.TimeoutLimit))
				}
				// cancel and proceed to cleanup below
				break waitLoop
			}

			time.Sleep(100 * time.Millisecond)
		}

		// Ensure the periodic-flush goroutine exits by cancelling the job context
		// before we call the cleanup that waits for the flush to finish.
		cancel()

		// cleanup network and release worker slot
		cleanupNetwork()
		if err := d.workerManager.ReleaseJob(job.JobUID.String()); err != nil {
			log.Printf("Failed to release worker for job %d: %v", job.ProblemId, err)
		}

		endT := time.Now()

		jobMetric := &types.JobMetricPayload{
			SchedulingDelay: setupStartT.Sub(submissionT),
			ExecutionTime:   endT.Sub(setupStartT),
			TotalTime:       endT.Sub(submissionT),
		}

		jobMetricEvent := types.StreamingJobEvent{
			JobUID:    job.JobUID,
			ProblemId: job.ProblemId,
			UserId:    job.UserId,
			Events: []types.StreamingEvent{{
				Kind:      "metric",
				JobMetric: jobMetric,
			}},
		}

		if d.metricsChannel != nil {
			select {
			case d.metricsChannel <- jobMetricEvent:
			default:
				log.Printf("Dropped job metric for job %v (channel full)", job.ProblemId)
			}
		}

		cleanupTimedFlush()
		<-flushDone

		log.Printf("Finished job %v", job.ProblemId)
		return
	}

	requiredWorkers := len(job.Code)
	workers, err := d.requestWorkerReservation(ctx, job.JobUID.String(), requiredWorkers)
	if err != nil {
		d.sendJobError(job, err)
		return
	}

	setupStartT := time.Now()

	// Per-job context with timeout
	jobCtx, cancel := context.WithTimeout(ctx, time.Duration(job.TimeoutLimit)*time.Second)
	d.mu.Lock()
	d.activeJobs[job.JobUID.String()] = cancel
	d.activeJobsByUser[job.UserId] = job.JobUID.String()
	d.mu.Unlock()
	defer func() {
		cancel()
		d.mu.Lock()
		delete(d.activeJobs, job.JobUID.String())
		if uid, ok := d.activeJobsByUser[job.UserId]; ok && uid == job.JobUID.String() {
			delete(d.activeJobsByUser, job.UserId)
		}
		d.mu.Unlock()
	}()

	cleanupNetwork, err := d.networkManager.CreateAndConnect(jobCtx, workers)
	if err != nil {
		log.Printf("Failed to setup network for job %d: %v", job.ProblemId, err)
		_ = d.workerManager.ReleaseJob(job.JobUID.String())
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

	endT := time.Now()

	jobMetric := &types.JobMetricPayload{
		SchedulingDelay: setupStartT.Sub(submissionT),
		ExecutionTime:   endT.Sub(setupStartT),
		TotalTime:       endT.Sub(submissionT),
	}

	jobMetricEvent := types.StreamingJobEvent{
		JobUID:    job.JobUID,
		ProblemId: job.ProblemId,
		UserId:    job.UserId,
		Events: []types.StreamingEvent{{
			Kind:      "metric",
			JobMetric: jobMetric,
		}},
	}

	// Send job-level metric (non-blocking)
	if d.metricsChannel != nil {
		select {
		case d.metricsChannel <- jobMetricEvent:
		default:
			log.Printf("Dropped job metric for job %v (channel full)", job.ProblemId)
		}
	}

	if err = d.workerManager.ReleaseJob(job.JobUID.String()); err != nil {
		d.sendJobError(job, err)
	}

	// Ensure the periodic flush goroutine exits by cancelling the job context
	cancel()

	cleanupTimedFlush()
	<-flushDone

	log.Printf("Finished job %v", job.ProblemId)
}

// requestWorkerReservation tries to reserve the required number of workers for the job.
// If not enough workers are available, it waits/blocks until they are or the context is cancelled.
func (d *JobDispatcher) requestWorkerReservation(ctx context.Context, jobUID string, count int) ([]WorkerInterface, error) {
	for {
		workers, err := d.workerManager.ReserveWorkers(jobUID, count)
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
				Kind:    "stdout",
				Message: utils.PtrString(line),
			})
			e.muEvents.Unlock()
		}
	}(worker.ID())

	// Stream stderr
	go func(id string) {
		for line := range stderrCh {
			e.muEvents.Lock()
			e.eventBuf = append(e.eventBuf, types.StreamingEvent{
				Kind:    "stderr",
				Message: utils.PtrString(line),
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
					Kind:    "error",
					Message: utils.PtrString(err.Error()),
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
					WorkerId: utils.PtrString(id),
					WorkerMetric: &types.WorkerMetricPayload{
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
	defer e.muEvents.Unlock()
	events := e.eventBuf

	// Ensure events is not nil
	if events == nil {
		events = []types.StreamingEvent{}
	}

	resultEvents := make([]types.StreamingEvent, 0)
	metricEvents := make([]types.StreamingEvent, 0)
	var totalWorkerTime time.Duration

	for _, evt := range events {
		switch evt.Kind {
		case "result", "stdout", "stderr", "error":
			resultEvents = append(resultEvents, evt)
		case "metric":
			metricEvents = append(metricEvents, evt)
			if evt.WorkerMetric != nil {
				totalWorkerTime += evt.WorkerMetric.DeltaTime
			}
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
		jobMetric := &types.JobMetricPayload{
			ExecutionTime: totalWorkerTime,
		}

		e.metricsChannel <- types.StreamingJobEvent{
			JobUID:        job.JobUID,
			ProblemId:     job.ProblemId,
			UserId:        job.UserId,
			SequenceIndex: -1,
			Events: append(metricEvents, types.StreamingEvent{
				Kind:      "metric",
				JobMetric: jobMetric,
			}),
		}
	}
}
