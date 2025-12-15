package worker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/DistCodeP7/distcode_worker/db"
	"github.com/DistCodeP7/distcode_worker/endpoints/metrics"
	"github.com/DistCodeP7/distcode_worker/jobsession"
	"github.com/DistCodeP7/distcode_worker/log"
	"github.com/DistCodeP7/distcode_worker/types"
	"github.com/google/uuid"
	"github.com/jonboulle/clockwork"
)

// WorkerManagerInterface manages worker reservation and lifecycle
type WorkerManagerInterface interface {
	ReserveWorkers(jobID uuid.UUID, testSpec types.NodeSpec, submissionSpecs []types.NodeSpec) (WorkUnit, []WorkUnit, error)
	ReleaseJob(jobID uuid.UUID) error
}

// NetworkManagerInterface creates and connects workers to a network
type NetworkManagerInterface interface {
	CreateAndConnect(ctx context.Context, workers []Worker) (cleanup func(), networkName string, err error)
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
	activeJobs   map[string]*JobRun
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
		activeJobs:       make(map[string]*JobRun),
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

// processJob orchestrates the lifecycle strictly and linearly
func (d *JobDispatcher) processJob(ctx context.Context, job types.Job) {
	log.Logger.Infof("Starting job %s", job.JobUID.String())
	d.metricsCollector.IncJobTotal()

	session := jobsession.NewJobSession(job, d.resultsChannel)
	session.SetPhase(types.PhasePending, "Initializing...")

	session.SetPhase(types.PhasePending, "Reserving workers...")
	testUnit, submissionUnits, err := d.requestWorkers(ctx, job)
	if err != nil {
		d.finalizeJob(job, session, jobsession.JobArtifacts{}, types.OutcomeFailed, fmt.Errorf("worker reservation failed: %w", err))
		return
	}
	defer d.workerManager.ReleaseJob(job.JobUID)
	session.SetPhase(types.PhasePending, "Configuring network...")

	submissionWorkers := make([]Worker, len(submissionUnits))
	for i, unit := range submissionUnits {
		submissionWorkers[i] = unit.Worker
	}

	cleanupNet, _, err := d.networkManager.CreateAndConnect(ctx, append([]Worker{testUnit.Worker}, submissionWorkers...))
	if err != nil {
		d.finalizeJob(job, session, jobsession.JobArtifacts{}, types.OutcomeFailed, fmt.Errorf("network setup failed: %w", err))
		return
	}
	defer cleanupNet()

	jobRun := NewJobRun(ctx, job, testUnit, submissionUnits, session)
	d.registerActiveJob(jobRun)
	defer d.unregisterActiveJob(job.JobUID)

	artifacts, outcome, err := jobRun.Execute()
	d.finalizeJob(job, session, artifacts, outcome, err)
}

// finalizeJob updates session state, metrics, and persists results
func (d *JobDispatcher) finalizeJob(
	job types.Job,
	session *jobsession.JobSessionLogger,
	artifacts jobsession.JobArtifacts,
	outcome types.Outcome,
	err error,
) {
	if outcome == types.OutcomeSuccess {
		session.FinishSuccess(artifacts)
	} else {
		session.FinishFail(artifacts, outcome, err, "")
	}

	d.metricsCollector.IncJobOutcome(outcome)
	saveCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	saveErr := d.jobStore.SaveResult(
		saveCtx,
		job.JobUID,
		outcome,
		artifacts.TestResults,
		session.GetBufferedLogs(),
		artifacts.NodeMessageLogs,
		session.StartTime(),
		job.SubmittedAt,
	)

	if saveErr != nil {
		log.Logger.Errorf("Failed to save job results to DB: %v", saveErr)
	}
}

func (d *JobDispatcher) requestWorkers(ctx context.Context, job types.Job) (WorkUnit, []WorkUnit, error) {
	for {
		testWorker, submissionWorkers, err := d.workerManager.ReserveWorkers(job.JobUID, job.TestNode, job.SubmissionNodes)
		if err == nil {
			return testWorker, submissionWorkers, nil
		}

		select {
		case <-ctx.Done():
			return WorkUnit{}, nil, ctx.Err()
		case <-d.clock.After(200 * time.Millisecond):
			// Retry loop
		}
	}
}

// registerActiveJob adds the job to active tracking
func (d *JobDispatcher) registerActiveJob(run *JobRun) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.activeJobs[run.ID.String()] = run
}

// unregisterActiveJob removes the job from active tracking
func (d *JobDispatcher) unregisterActiveJob(id uuid.UUID) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.activeJobs, id.String())
}

// CancelJob delegates cancellation to the specific JobRun
func (d *JobDispatcher) cancelJob(req types.CancelJobRequest) {
	d.mu.Lock()
	jobRun, ok := d.activeJobs[req.JobUID.String()]
	d.mu.Unlock()

	if !ok {
		log.Logger.Warnf("Cancel request for unknown or completed job: %s", req.JobUID.String())
		return
	}
	jobRun.Cancel(true)
}
