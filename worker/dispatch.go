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

	// --- Reserve workers ---
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
		}
	}

	networkName := "job-" + uuid.NewString()
	_, err = d.workerManager.client.NetworkCreate(ctx, networkName, network.CreateOptions{Driver: "bridge"})
	if err != nil {
		log.Printf("Failed to create network %s: %v", networkName, err)
	}
	defer d.workerManager.client.NetworkRemove(ctx, networkName)

	for i, worker := range workers {
		alias := fmt.Sprintf("worker-%d", i)
		worker.ConnectToNetwork(ctx, networkName, alias)
	}

	var wg sync.WaitGroup
	eventBuf := make([]types.StreamingEvent, 0)
	var muEvents sync.Mutex
	sequence := 0

	// Aggregator ticker
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				muEvents.Lock()
				if len(eventBuf) > 0 {
					eventsCopy := append([]types.StreamingEvent(nil), eventBuf...)
					eventBuf = nil
					d.resultsChannel <- types.StreamingJobResult{
						JobId:         job.ProblemId,
						UserId:        job.UserId,
						SequenceIndex: sequence,
						Events:        eventsCopy,
					}
					sequence++
				}
				muEvents.Unlock()
			}
		}
	}()

	for i, worker := range workers {
		stdoutCh := make(chan string, 10)
		stderrCh := make(chan string, 10)

		// Stream stdout
		go func(id string) {
			for line := range stdoutCh {
				muEvents.Lock()
				eventBuf = append(eventBuf, types.StreamingEvent{Kind: "stdout", Message: line})
				muEvents.Unlock()
			}
		}(worker.containerID)

		// Stream stderr
		go func(id string) {
			for line := range stderrCh {
				muEvents.Lock()
				eventBuf = append(eventBuf, types.StreamingEvent{Kind: "stderr", Message: line})
				muEvents.Unlock()
			}
		}(worker.containerID)

		wg.Add(1)
		go func(w *Worker, code string, id string) {
			defer wg.Done()

			execCtx, cancelExec := context.WithTimeout(ctx, 120*time.Second)
			defer cancelExec()

			if err := w.ExecuteCode(execCtx, code, stdoutCh, stderrCh); err != nil {
				muEvents.Lock()
				eventBuf = append(eventBuf, types.StreamingEvent{Kind: "error", Message: err.Error()})
				muEvents.Unlock()
			}

			close(stdoutCh)
			close(stderrCh)
		}(worker, job.Code[i], worker.containerID)
	}

	wg.Wait()

	// Send any remaining events
	muEvents.Lock()
	if len(eventBuf) > 0 {
		d.resultsChannel <- types.StreamingJobResult{
			JobId:         job.ProblemId,
			UserId:        job.UserId,
			SequenceIndex: -1,
			Events:        eventBuf,
		}
	}
	muEvents.Unlock()

	for _, worker := range workers {
		worker.DisconnectFromNetwork(ctx, networkName)
	}

	if err = d.workerManager.ReleaseJob(job.ProblemId); err != nil {
		log.Fatalf("Job has been released twice, should never happen")
	}

	log.Printf("Finished job %v", job.ProblemId)
}
