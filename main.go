package main

import (
	"log"

	"github.com/DistCodeP7/distcode_worker/mq"
	"github.com/DistCodeP7/distcode_worker/setup"
	"github.com/DistCodeP7/distcode_worker/types"
	"github.com/DistCodeP7/distcode_worker/worker"
)

func main() {
	// Parse command line flags
	workerImageName, controllerImageName, numWorkers, jobsCapacity := setup.ParseFlags()

	// Setup context, docker client, ensure the worker image is available and prepare worker cache.
	appResources, err := setup.SetupApp(workerImageName, controllerImageName)
	if err != nil {
		log.Fatalf("Fatal error in setup: %v", err)
	}
	defer appResources.Cancel()
	defer appResources.DockerCli.Close()

	jobsCh := make(chan types.JobRequest, jobsCapacity)
	resultsCh := make(chan types.StreamingJobEvent, jobsCapacity)
	cancelJobCh := make(chan types.CancelJobRequest, jobsCapacity)

	defer close(resultsCh)

	// Start a goroutine to receive jobs from RabbitMQ
	go func() {
		if err := mq.StartJobConsumer(appResources.Ctx, jobsCh); err != nil {
			log.Fatalf("MQ error: %v", err)
		}
	}()

	// Start a goroutine to receive cancel requests from RabbitMQ
	go func() {
		if err := mq.StartJobCanceller(appResources.Ctx, cancelJobCh); err != nil {
			log.Fatalf("MQ error: %v", err)
		}
	}()

	wm, err := worker.NewWorkerManager(numWorkers, worker.NewDockerWorkerProducer(appResources.DockerCli, workerImageName))

	if err != nil {
		log.Fatalf("Failed to create worker manager: %v", err)
	}

	dispatcher := worker.NewJobDispatcher(
		cancelJobCh,
		jobsCh,
		resultsCh,
		wm,
		worker.NewDockerNetworkManager(appResources.DockerCli),
	)

	go dispatcher.Run(appResources.Ctx)
	// Start separate publishers for results and metrics
	go func() {
		if err := mq.PublishStreamingEvents(appResources.Ctx, mq.EventTypeResults, resultsCh); err != nil {
			log.Fatalf("MQ results publisher error: %v", err)
		}
	}()

	<-appResources.Ctx.Done()
	if err := wm.Shutdown(); err != nil {
		log.Printf("Error shutting down workers: %v", err)
	}

	log.Println("All workers have finished. Exiting.")
}
