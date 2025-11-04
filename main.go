package main

import (
	"log"

	"github.com/DistCodeP7/distcode_worker/mq"
	"github.com/DistCodeP7/distcode_worker/setup"
	"github.com/DistCodeP7/distcode_worker/types"
	"github.com/DistCodeP7/distcode_worker/worker"
	"github.com/jonboulle/clockwork"
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

	jobsCh := make(chan types.JobRequest, jobsCapacity)
	resultsCh := make(chan types.StreamingJobResult, jobsCapacity)
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

	workers := make([]worker.WorkerInterface, numWorkers)
	for i := range numWorkers {
		worker, err := worker.NewWorker(appResources.Ctx, appResources.DockerCli, workerImageName)
		if err != nil {
			log.Fatalf("Failed to create worker: %v", err)
		}
		workers[i] = worker
	}

	wm, err := worker.NewWorkerManager(workers)

	if err != nil {
		log.Fatalf("Failed to create worker manager: %v", err)
	}

	dispatcher := worker.NewJobDispatcher(worker.JobDispatcherConfig{
		JobChannel:     jobsCh,
		CancelJobChan:  cancelJobCh,
		ResultsChannel: resultsCh,
		WorkerManager:  wm,
		NetworkManager: worker.NewDockerNetworkManager(appResources.DockerCli),
		Clock:          clockwork.NewRealClock(),
	})

	go dispatcher.Run(appResources.Ctx)
	go mq.PublishJobResults(appResources.Ctx, resultsCh)

	<-appResources.Ctx.Done()
	if err := wm.Shutdown(); err != nil {
		log.Printf("Error shutting down workers: %v", err)
	}

	log.Println("All workers have finished. Exiting.")
}
