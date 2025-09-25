package worker

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/DistCodeP7/distcode_worker/types"
)

func StartWorker(config *types.WorkerConfig, workerID int) {
	defer config.Wg.Done()
	log.Printf("Worker %d starting...", workerID)

	w, err := New(config.Ctx, config.DockerCli)
	if err != nil {
		log.Printf("Error starting worker %d: %v", workerID, err)
		return
	}

	defer func() {
		if err := w.Stop(context.Background()); err != nil {
			log.Printf("Error stopping worker %d: %v", workerID, err)
		}
	}()

	for {
		select {
		case job, ok := <-config.Jobs:
			if !ok {
				log.Printf("Worker %d shutting down as jobs channel is closed.", workerID)
				return
			}
			log.Printf("Worker %d picked up Job %d", workerID, job.ProblemId)

			execCtx, cancelExec := context.WithTimeout(config.Ctx, 120*time.Second)

			stdoutCh := make(chan string)
			stderrCh := make(chan string)

			var (
				eventBuf []types.StreamingEvent
				muEvents sync.Mutex
			)

			// Capture stdout as events
			go func() {
				for line := range stdoutCh {
					muEvents.Lock()
					eventBuf = append(eventBuf, types.StreamingEvent{
						Kind:    "stdout",
						Message: line,
					})
					muEvents.Unlock()
				}
			}()

			go func() {
				for line := range stderrCh {
					muEvents.Lock()
					eventBuf = append(eventBuf, types.StreamingEvent{
						Kind:    "stderr",
						Message: line,
					})
					muEvents.Unlock()
				}
			}()

			go func() {
				ticker := time.NewTicker(50 * time.Millisecond)
				defer ticker.Stop()

				sequence := 0
				for {
					select {
					case <-execCtx.Done():
						return
					case <-ticker.C:
						muEvents.Lock()
						if len(eventBuf) > 0 {
							eventsCopy := append([]types.StreamingEvent(nil), eventBuf...)
							eventBuf = nil
							config.Results <- types.StreamingJobResult{
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

			err := w.ExecuteCode(execCtx, job.Code, stdoutCh, stderrCh)
			cancelExec()

			// Flush any remaining buffered events
			muEvents.Lock()
			if len(eventBuf) > 0 || err != nil {
				eventsCopy := append([]types.StreamingEvent(nil), eventBuf...)
				eventBuf = nil
				if err != nil {
					eventsCopy = append(eventsCopy, types.StreamingEvent{
						Kind:    "error",
						Message: errString(err),
					})
				}
				config.Results <- types.StreamingJobResult{
					JobId:         job.ProblemId,
					UserId:        job.UserId,
					SequenceIndex: -1,
					Events:        eventsCopy,
				}
			}
			muEvents.Unlock()
			if err != nil {
				fmt.Println("Execution finished with error:", err)
			}
			log.Printf("Worker %d completed Job %d", workerID, job.ProblemId)

		case <-config.Ctx.Done():
			log.Printf("Worker %d received shutdown signal. Exiting.", workerID)
			return
		}
	}
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
