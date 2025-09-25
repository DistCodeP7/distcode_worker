package main

import (
	"log"
	"sync"

	"github.com/DistCodeP7/distcode_worker/mq"
	"github.com/DistCodeP7/distcode_worker/setup"
	"github.com/DistCodeP7/distcode_worker/types"
	"github.com/DistCodeP7/distcode_worker/worker"
)

func main() {
	// Parse command line flags
	workerImageName, numWorkers, jobsCapacity := setup.ParseFlags()

	// Setup context, docker client, ensure the worker image is available and prepare worker cache.
	appResources, err := setup.SetupApp(workerImageName)
	if err != nil {
		log.Fatalf("Fatal error in setup: %v", err)
	}
	defer appResources.Cancel()
	defer appResources.DockerCli.Close()

	jobs_ch := make(chan types.JobRequest, jobsCapacity)
	results_ch := make(chan types.StreamingJobResult, jobsCapacity)
	var wg sync.WaitGroup

	// Start a goroutine to receive jobs from RabbitMQ
	go func() {
		if err := mq.StartJobConsumer(appResources.Ctx, jobs_ch); err != nil {
			log.Fatalf("MQ error: %v", err)
		}
	}()

	workerConfig := types.WorkerConfig{
		Ctx:       appResources.Ctx,
		DockerCli: appResources.DockerCli,
		Wg:        &wg,
		Jobs:      jobs_ch,
		Results:   results_ch,
	}

	log.Printf("Starting %d workers...", numWorkers)
	for i := 1; i <= numWorkers; i++ {
		wg.Add(1)
		go worker.StartWorker(&workerConfig, i)
	}

	go mq.PublishJobResults(appResources.Ctx, results_ch)
	wg.Wait()
	close(results_ch)
	log.Println("All workers have finished. Exiting.")
}
