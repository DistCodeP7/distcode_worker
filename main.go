package main

import (
	"log"
	"sync"

	"github.com/DistCodeP7/distcode_worker/reciever"
	"github.com/DistCodeP7/distcode_worker/results"
	"github.com/DistCodeP7/distcode_worker/setup"
	"github.com/DistCodeP7/distcode_worker/types"
	"github.com/DistCodeP7/distcode_worker/worker"
)

func main() {
	// Parse command line flags
	workerImageName, numWorkers, jobsCapacity := setup.ParseFlags()

	// Setup context, docker client, and ensure the worker image is available
	appResources, err := setup.SetupApp(workerImageName)
	if err != nil {
		log.Fatalf("Fatal error in setup: %v", err)
	}
	defer appResources.Cancel()
	defer appResources.DockerCli.Close()

	jobs_ch := make(chan types.JobRequest, jobsCapacity)
	results_ch := make(chan types.JobResult, jobsCapacity)
	var wg sync.WaitGroup

	// Start a goroutine to receive jobs from RabbitMQ
	go func() {
		if err := reciever.InitMessageQueue(appResources.Ctx, jobs_ch); err != nil {
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

	go results.Handle(appResources.Ctx, results_ch)
	wg.Wait()
	close(results_ch)
	log.Println("All workers have finished. Exiting.")
}
