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

	"github.com/DistCodeP7/distcode_worker/jobsession"
	"github.com/DistCodeP7/distcode_worker/log"
	"github.com/DistCodeP7/distcode_worker/types"
	t "github.com/distcodep7/dsnet/testing"
	dt "github.com/distcodep7/dsnet/testing/disttest"
	"github.com/google/uuid"
)

// JobRun encapsulates the execution state of a single job.
// It is created by the Dispatcher and is responsible for the specific
// Compile -> Run -> Collect pipeline.
type JobRun struct {
	ID              uuid.UUID
	Job             types.Job
	Session         *jobsession.JobSessionLogger
	SubmissionUnits []WorkUnit
	TestUnit        WorkUnit
	AllUnits        []WorkUnit
	ctx             context.Context
	cancel          context.CancelFunc
	mu              sync.Mutex
	canceledByUser  bool
	finished        bool
	debugEnabled    bool
}

type WorkUnit struct {
	Spec   types.NodeSpec
	Worker Worker
}

func NewJobRun(
	parentCtx context.Context,
	job types.Job,
	tUnit WorkUnit,
	sUnits []WorkUnit,
	session *jobsession.JobSessionLogger,
) *JobRun {
	jobCtx, cancel := context.WithTimeout(parentCtx, time.Duration(job.Timeout)*time.Second)

	allUnits := make([]WorkUnit, 0, 1+len(sUnits))
	allUnits = append(allUnits, tUnit)
	allUnits = append(allUnits, sUnits...)

	return &JobRun{
		ID:              job.JobUID,
		Job:             job,
		SubmissionUnits: sUnits,
		TestUnit:        tUnit,
		AllUnits:        allUnits,
		Session:         session,
		ctx:             jobCtx,
		cancel:          cancel,
		debugEnabled:    true,
	}
}

// Cancel terminates the job.
// If byUser is true, it flags the state so we can distinguish it from a timeout.
func (r *JobRun) Cancel(byUser bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.finished {
		return
	}

	if byUser {
		r.canceledByUser = true
	}
	r.cancel()
}

// Execute runs the full job pipeline.
// It returns artifacts, the final outcome, and any error that occurred.
func (r *JobRun) Execute() (jobsession.JobArtifacts, types.Outcome, error) {
	defer func() {
		r.mu.Lock()
		r.finished = true
		r.mu.Unlock()
		r.cancel()
	}()

	if r.debugEnabled {
		r.Session.SetPhase(types.PhaseDebugging, "Debugging enabled. Dumping go.mod and file tree...")
		_ = r.ExecuteCommandOnAllUnits("cat go.mod && ls -R")
	}

	r.Session.SetPhase(types.PhaseCompiling, "Compiling code...")
	compileSuccess, failedWorker, compileErr := r.compileAll()

	// Handle cancel/timeout BEFORE treating it as a plain compilation error
	if !compileSuccess {
		r.mu.Lock()
		userCancelled := r.canceledByUser
		r.mu.Unlock()

		// User explicitly cancelled
		if userCancelled && errors.Is(r.ctx.Err(), context.Canceled) {
			return jobsession.JobArtifacts{}, types.OutcomeCancel,
				errors.New("job canceled by user")
		}

		// Context timed out during compile
		if errors.Is(r.ctx.Err(), context.DeadlineExceeded) {
			return jobsession.JobArtifacts{}, types.OutcomeTimeout,
				fmt.Errorf("job timed out after %ds (during compilation)", r.Job.Timeout)
		}

		// Genuine compilation error
		return jobsession.JobArtifacts{}, types.OutcomeCompilationError,
			fmt.Errorf("compilation failed on %s: %w", failedWorker, compileErr)
	}

	r.Session.SetPhase(types.PhaseRunning, "Compilation successful. Executing...")
	execErr := r.executeAll()

	r.Session.SetPhase(types.PhaseRunning, "Collecting artifacts...")
	artifacts := r.collectArtifacts()

	r.mu.Lock()
	userCancelled := r.canceledByUser
	r.mu.Unlock()

	// Still necessary to check for user cancellation here as a user can
	// cancel even after compilation succeeds.
	if userCancelled {
		return artifacts, types.OutcomeCancel, errors.New("job canceled by user")
	}

	if errors.Is(r.ctx.Err(), context.DeadlineExceeded) {
		return artifacts, types.OutcomeTimeout,
			fmt.Errorf("job timed out after %ds", r.Job.Timeout)
	}

	if execErr != nil {
		return artifacts, types.OutcomeFailed, execErr
	}

	return artifacts, types.OutcomeSuccess, nil
}

func (r *JobRun) ExecuteCommandOnAllUnits(cmd string) error {
	var wg sync.WaitGroup
	errCh := make(chan error, len(r.AllUnits))

	for _, unit := range r.AllUnits {
		wg.Go(func() {
			logWriter := r.Session.NewLogWriter(unit.Worker.Alias())
			fmt.Fprintf(logWriter, "Executing command: %s\n", cmd)

			err := unit.Worker.ExecuteCommand(r.ctx, ExecuteCommandOptions{
				Cmd:          cmd,
				OutputWriter: logWriter,
			})
			if err != nil {
				errCh <- fmt.Errorf("command execution failed on worker %s: %w", unit.Worker.ID()[:12], err)

				// Fail Fast: If one node crashes, cancel the others to save resources
				r.cancel()
			}
		})
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *JobRun) compileAll() (bool, string, error) {
	var wg sync.WaitGroup
	type buildResult struct {
		workerID string
		err      error
	}
	results := make(chan buildResult, len(r.AllUnits))

	for _, unit := range r.AllUnits {
		if unit.Spec.BuildCommand == "" {
			continue
		}

		wg.Go(func() {
			logWriter := r.Session.NewLogWriter(unit.Worker.Alias())
			fmt.Fprintf(logWriter, "Running build: %s\n", unit.Spec.BuildCommand)

			err := unit.Worker.ExecuteCommand(r.ctx, ExecuteCommandOptions{
				Cmd:          unit.Spec.BuildCommand,
				OutputWriter: logWriter,
			})
			if err != nil {
				results <- buildResult{workerID: unit.Worker.ID(), err: err}
			}
		})
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	// If any single build fails, the entire job fails.
	for res := range results {
		if res.err != nil {
			return false, res.workerID, res.err
		}
	}
	return true, "", nil
}

func (r *JobRun) executeAll() error {
	var wg sync.WaitGroup
	errCh := make(chan error, len(r.AllUnits))

	for _, unit := range r.AllUnits {
		wg.Go(func() {
			logWriter := r.Session.NewLogWriter(unit.Worker.Alias())
			fmt.Fprintf(logWriter, "Executing: %s\n", unit.Spec.EntryCommand)

			err := unit.Worker.ExecuteCommand(r.ctx, ExecuteCommandOptions{
				Cmd:          unit.Spec.EntryCommand,
				OutputWriter: logWriter,
			})
			if err != nil {
				fmt.Fprintf(logWriter, "Execution Error: %v\n", err)
				errCh <- fmt.Errorf("execution failed on worker %s: %w", unit.Worker.ID()[:12], err)

				// Fail Fast: If one node crashes, cancel the others to save resources
				r.cancel()
			}
		})
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *JobRun) collectArtifacts() jobsession.JobArtifacts {
	var results []dt.TestResult
	var logs []t.TraceEvent
	var logsMu sync.Mutex
	var wg sync.WaitGroup

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	wg.Go(func() {
		tw := r.TestUnit.Worker
		data, err := tw.ReadFile(ctx, "app/tmp/test_results.json")
		if err == nil {
			if jsonErr := json.Unmarshal(data, &results); jsonErr != nil {
				log.Logger.Errorf("Failed to unmarshal test results JSON: %v", jsonErr)
			}
		}
	})

	for _, unit := range r.AllUnits {
		wg.Go(func() {
			logData, err := unit.Worker.ReadFile(ctx, "app/tmp/trace_log.jsonl")
			if err != nil {
				return
			}

			var workerLogs []t.TraceEvent
			scanner := bufio.NewScanner(bytes.NewReader(logData))
			for scanner.Scan() {
				line := scanner.Bytes()
				if len(bytes.TrimSpace(line)) == 0 {
					continue
				}
				var entry t.TraceEvent
				if json.Unmarshal(line, &entry) == nil {
					workerLogs = append(workerLogs, entry)
				}
			}

			logsMu.Lock()
			logs = append(logs, workerLogs...)
			logsMu.Unlock()
		})
	}

	wg.Wait()

	return jobsession.JobArtifacts{
		TestResults:     results,
		NodeMessageLogs: logs,
	}
}
