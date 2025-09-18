package worker

import (
	"context"
	"log"
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

			execCtx, cancelExec := context.WithTimeout(config.Ctx, 30*time.Second)

			stdout, stderr, err := w.ExecuteCode(execCtx, job.Code)
			config.Results <- types.JobResult{
				JobId:  job.ProblemId,
				UserId: job.UserId,
				Result: types.Result{
					Stdout: stdout,
					Stderr: stderr,
					Err: func() string {
						if err != nil {
							return err.Error()
						}
						return ""
					}(),
				},
			}
			cancelExec()

		case <-config.Ctx.Done():

			log.Printf("Worker %d received shutdown signal. Exiting.", workerID)
			return
		}
	}
}
