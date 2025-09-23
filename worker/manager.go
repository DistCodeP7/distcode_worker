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

			execCtx, cancelExec := context.WithTimeout(config.Ctx, 30*time.Second)

			stdoutCh := make(chan string)
			stderrCh := make(chan string)

			var (
				stdoutBuf []string
				stderrBuf []string
				muStdout  sync.Mutex
				muStderr  sync.Mutex
			)

			go func() {
				for line := range stdoutCh {
					muStdout.Lock()
					stdoutBuf = append(stdoutBuf, line)
					muStdout.Unlock()
				}
			}()

			go func() {
				for line := range stderrCh {
					muStderr.Lock()
					stderrBuf = append(stderrBuf, line)
					muStderr.Unlock()
				}
			}()

			go func() {
				ticker := time.NewTicker(100 * time.Millisecond)
				defer ticker.Stop()

				for {
					select {
					case <-execCtx.Done():
						return
					case <-ticker.C:
						muStdout.Lock()
						outCopy := append([]string(nil), stdoutBuf...)
						stdoutBuf = nil
						muStdout.Unlock()

						muStderr.Lock()
						errCopy := append([]string(nil), stderrBuf...)
						stderrBuf = nil
						muStderr.Unlock()

						if len(outCopy) > 0 || len(errCopy) > 0 {
							config.Results <- types.StreamingJobResult{
								JobId:  job.ProblemId,
								UserId: job.UserId,
								Result: types.StreamingResult{
									Stdout: outCopy,
									Stderr: errCopy,
									Error:  "",
								},
							}
						}
					}
				}
			}()

			err := w.ExecuteCode(execCtx, job.Code, stdoutCh, stderrCh)
			cancelExec()
			muStdout.Lock()
			outCopy := append([]string(nil), stdoutBuf...)
			stdoutBuf = nil
			muStdout.Unlock()

			muStderr.Lock()
			errCopy := append([]string(nil), stderrBuf...)
			stderrBuf = nil
			muStderr.Unlock()

			config.Results <- types.StreamingJobResult{
				JobId:  job.ProblemId,
				UserId: job.UserId,
				Result: types.StreamingResult{
					Stdout: outCopy,
					Stderr: errCopy,
					Error:  errString(err),
				},
			}

			fmt.Println("Execution finished with error:", err)

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
