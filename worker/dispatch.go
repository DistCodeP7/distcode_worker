package worker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/DistCodeP7/distcode_worker/cancel"
	"github.com/DistCodeP7/distcode_worker/db"
	"github.com/DistCodeP7/distcode_worker/endpoints/metrics"
	"github.com/DistCodeP7/distcode_worker/jobsession"
	"github.com/DistCodeP7/distcode_worker/log"
	"github.com/DistCodeP7/distcode_worker/types"
	"github.com/DistCodeP7/distcode_worker/utils"
	"github.com/google/uuid"
	"github.com/jonboulle/clockwork"
)

// WorkerManagerInterface manages worker reservation and lifecycle
type WorkerManagerInterface interface {
	ReserveSlotsOrWait(ctx context.Context, count int) error
	CreateWorkers(ctx context.Context, jobID uuid.UUID, testSpec types.NodeSpec, submissionSpecs []types.NodeSpec) (WorkUnit, []WorkUnit, error)
	DiscardReservation(count int)
	ReleaseJob(jobID uuid.UUID) error
}

type NetworkManagerInterface interface {
	CreateAndConnect(ctx context.Context, workers []Worker) (cleanup func(), networkName string, err error)
}

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

	cancellationTracker *cancel.CancellationTracker
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
		cancelJobChan:       cancelJobChan,
		jobChannel:          jobChannel,
		resultsChannel:      resultsChannel,
		workerManager:       workerManager,
		networkManager:      networkManager,
		jobStore:            jobStore,
		metricsCollector:    metricsCollector,
		clock:               clockwork.NewRealClock(),
		activeJobs:          make(map[string]*JobRun),
		cancellationTracker: cancel.NewCancellationTracker(5*time.Minute, 1*time.Minute),
	}

	for _, opt := range opts {
		opt(d)
	}
	return d
}

func (d *JobDispatcher) Run(ctx context.Context) {
	go d.cancellationTracker.StartGC(ctx)
	for {
		select {
		case <-ctx.Done():
			log.Logger.Info("Dispatcher stopping...")
			d.activeJobsWg.Wait()
			return

		case cancelReq := <-d.cancelJobChan:
			d.handleCancelRequest(cancelReq)

		case job := <-d.jobChannel:
			neededSlots := 1 + len(job.SubmissionNodes)
			if err := d.waitForSlots(ctx, job, neededSlots); err != nil {
				continue
			}
			d.activeJobsWg.Add(1)
			go d.processJob(ctx, job, neededSlots)
		}
	}
}

// waitForSlots blocks until the WorkerManager reserves the count.
// It actively listens to d.cancelJobChan to allow cancelling the *waiting* job.
func (d *JobDispatcher) waitForSlots(ctx context.Context, job types.Job, count int) error {
	if d.cancellationTracker.IsCancelled(job.JobUID) {
		d.failCancelledJob(job, "cancelled before reservation")
		return context.Canceled
	}

	waitCtx, cancelWait := context.WithCancel(ctx)
	defer cancelWait()

	errCh := make(chan error, 1)

	go func() {
		errCh <- d.workerManager.ReserveSlotsOrWait(waitCtx, count)
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case cancelReq := <-d.cancelJobChan:
			d.handleCancelRequest(cancelReq)
			if cancelReq.JobUID == job.JobUID {
				log.Logger.Infof("Job %s cancelled while waiting for slots", job.JobUID)
				cancelWait()
			}

		case err := <-errCh:
			if err != nil {
				if err == context.Canceled {
					d.failCancelledJob(job, "cancelled during reservation")
				} else if _, ok := err.(*ErrorReserveTooManyWorkers); ok {
					log.Logger.Warnf("Job %s requests too many workers: %v", job.JobUID.String(), err)
					d.finalizeJob(job, jobsession.NewJobSession(job, d.resultsChannel), jobsession.JobArtifacts{}, types.OutcomeFailed, fmt.Errorf("requested too many workers: %w", err))
				} else {
					log.Logger.Warnf("Failed to reserve slots for job %s: %v, THIS SHOULDNT HAPPEN", job.JobUID.String(), err)
					d.finalizeJob(job, jobsession.NewJobSession(job, d.resultsChannel), jobsession.JobArtifacts{}, types.OutcomeFailed, fmt.Errorf("failed to reserve slots: %w", err))
				}
				return err
			}
			return nil
		}
	}
}

func (d *JobDispatcher) handleCancelRequest(req types.CancelJobRequest) {
	d.mu.Lock()
	jobRun, isActive := d.activeJobs[req.JobUID.String()]
	d.mu.Unlock()

	if isActive {
		log.Logger.Debugf("Cancelling active job: %s", req.JobUID.String())
		jobRun.Cancel(true)
	} else {
		d.cancellationTracker.Track(req.JobUID)
		log.Logger.Debugf("Staging cancellation for pending job: %s", req.JobUID.String())
	}
}

func (d *JobDispatcher) failCancelledJob(job types.Job, reason string) {
	log.Logger.Warnf("Job %s %s", job.JobUID.String(), reason)
	session := jobsession.NewJobSession(job, d.resultsChannel)
	d.finalizeJob(job, session, jobsession.JobArtifacts{}, types.OutcomeCanceled, fmt.Errorf("%s", reason))
}

// processJob handles creation, execution, and cleanup.
// It assumes 'reservedSlots' have already been secured.
func (d *JobDispatcher) processJob(ctx context.Context, job types.Job, reservedSlots int) {
	defer d.activeJobsWg.Done()
	endJob := d.metricsCollector.StartJob()
	defer endJob()

	log.Logger.Infof("Starting job %s", job.JobUID.String())

	session := jobsession.NewJobSession(job, d.resultsChannel)
	session.SetPhase(types.PhasePending, "Initializing...")

	if d.cancellationTracker.IsCancelled(job.JobUID) {
		d.workerManager.DiscardReservation(reservedSlots)
		d.failCancelledJob(job, "cancelled before worker creation")
		return
	}

	session.SetPhase(types.PhaseReserving, "Reserving workers...")
	testUnit, submissionUnits, err := d.workerManager.CreateWorkers(ctx, job.JobUID, job.TestNode, job.SubmissionNodes)
	if err != nil {
		d.workerManager.DiscardReservation(reservedSlots)
		d.finalizeJob(job, session, jobsession.JobArtifacts{}, types.OutcomeFailed, fmt.Errorf("worker creation failed: %w", err))
		return
	}
	defer d.workerManager.ReleaseJob(job.JobUID)
	session.SetPhase(types.PhaseConfiguringNetwork, "Configuring network...")

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
	defer d.unregisterActiveJob(jobRun)

	if d.cancellationTracker.IsCancelled(job.JobUID) {
		log.Logger.Warnf("Job %s cancelled during setup, aborting execution", job.JobUID.String())
		jobRun.Cancel(true)
	}

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

	var timeSpent map[types.Phase]time.Duration
	if outcome == types.OutcomeSuccess {
		timeSpent = session.FinishSuccess(artifacts)
	} else {
		timeSpent = session.FinishFail(artifacts, outcome, err, "")
	}

	d.metricsCollector.TrackTimeSpent(timeSpent)
	d.metricsCollector.IncJobOutcome(outcome)
	saveCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	saveErr := d.jobStore.SaveResult(
		saveCtx,
		db.JobResult{
			Job:       job,
			Logs:      session.GetBufferedLogs(),
			Artifacts: artifacts,
			Outcome:   outcome,
			Timespent: utils.MapToPayload(timeSpent),
			Duration:  time.Since(session.StartTime()),
			Error:     err,
		},
	)

	if saveErr != nil {
		log.Logger.Errorf("Failed to save job results to DB: %v", saveErr)
	}
}

func (d *JobDispatcher) registerActiveJob(run *JobRun) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.activeJobs[run.GetID().String()] = run
}

func (d *JobDispatcher) unregisterActiveJob(run *JobRun) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.activeJobs, run.GetID().String())
}
