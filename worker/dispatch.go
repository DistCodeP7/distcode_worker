package worker

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/DistCodeP7/distcode_worker/types"
	"github.com/docker/docker/api/types/network"
	"github.com/google/uuid"
)

type JobDispatcherConfig struct {
	JobChannel     <-chan types.JobRequest         // receive-only
	ResultsChannel chan<- types.StreamingJobResult // send-only
	WorkerManager  *WorkerManager
}

type JobDispatcher struct {
	jobChannel     <-chan types.JobRequest
	resultsChannel chan<- types.StreamingJobResult
	workerManager  *WorkerManager
}

func NewJobDispatcher(config JobDispatcherConfig) *JobDispatcher {
	return &JobDispatcher{
		jobChannel:     config.JobChannel,
		resultsChannel: config.ResultsChannel,
		workerManager:  config.WorkerManager,
	}
}

func (d *JobDispatcher) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case job := <-d.jobChannel:
			go d.handleJob(ctx, job)
		}
	}
}

func (d *JobDispatcher) handleJob(ctx context.Context, job types.JobRequest) {
	log.Printf("Starting job %v", job.ProblemId)

	requiredWorkers := len(job.Code)

	// --- Retry reserving workers until available or ctx is done ---
	var workers []*Worker
	var err error
	for {
		workers, err = d.workerManager.ReserveWorkers(job.ProblemId, requiredWorkers)
		if err == nil {
			break
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(200 * time.Millisecond):
			// retry
		}
	}

	// --- Create per-job network ---
	networkName := "job-" + uuid.NewString()
	_, err = d.workerManager.client.NetworkCreate(ctx, networkName, network.CreateOptions{
		Driver: "bridge",
	})
	if err != nil {
		log.Printf("Failed to create network %s: %v", networkName, err)
	}
	defer func() {
		if rmErr := d.workerManager.client.NetworkRemove(ctx, networkName); rmErr != nil {
			log.Printf("Failed to remove network %s: %v", networkName, rmErr)
		}
	}()

	// --- Connect workers with aliases ---
	for i, worker := range workers {
		alias := fmt.Sprintf("worker-%d", i)
		worker.ConnectToNetwork(ctx, networkName, alias)
	}

	var wg sync.WaitGroup
	for i, worker := range workers {
		stdoutCh := make(chan string, 10)
		stderrCh := make(chan string, 10)

		// log readers
		go func(id string) {
			for line := range stdoutCh {
				log.Printf("Worker %s stdout: %s", id, line)
			}
		}(worker.containerID)
		go func(id string) {
			for line := range stderrCh {
				log.Printf("Worker %s stderr: %s", id, line)
			}
		}(worker.containerID)

		wg.Add(1)
		go func(w *Worker, code string, id string) {
			defer wg.Done()

			execCtx, cancelExec := context.WithTimeout(ctx, 120*time.Second)
			defer cancelExec()

			log.Printf("Executing code for worker %s", id)
			if err := w.ExecuteCode(execCtx, code, stdoutCh, stderrCh); err != nil {
				log.Printf("Worker %s failed: %v", id, err)
			}
			close(stdoutCh)
			close(stderrCh)
		}(worker, job.Code[i], worker.containerID)
	}

	wg.Wait()

	// disconnect workers from network
	for _, worker := range workers {
		worker.DisconnectFromNetwork(ctx, networkName)
	}

	if err = d.workerManager.ReleaseJob(job.ProblemId); err != nil {
		log.Fatalf("Job has been released twice, should never happen")
	}

	// send minimal result (streaming aggregation can be re-added later)
	d.resultsChannel <- types.StreamingJobResult{
		JobId:         job.ProblemId,
		Events:        []types.StreamingEvent{},
		UserId:        job.UserId,
		SequenceIndex: 0,
	}
	log.Printf("Finished job %v", job.ProblemId)
}
