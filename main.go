package main

import (
	"github.com/DistCodeP7/distcode_worker/db"
	"github.com/DistCodeP7/distcode_worker/log"
	"github.com/DistCodeP7/distcode_worker/metrics"
	"github.com/DistCodeP7/distcode_worker/mq"
	"github.com/DistCodeP7/distcode_worker/setup"
	"github.com/DistCodeP7/distcode_worker/types"
	"github.com/DistCodeP7/distcode_worker/worker"
	l "github.com/sirupsen/logrus"
)

func main() {
	log.Init(l.TraceLevel, true)
	// Parse command line flags
	workerImageName, controllerImageName, numWorkers, jobsCapacity := setup.ParseFlags()
	log.Logger.WithFields(l.Fields{
		"worker_image":     workerImageName,
		"controller_image": controllerImageName,
		"num_workers":      numWorkers,
	}).Info("Application initialized")

	// Setup context, docker client, ensure the worker image is available and prepare worker cache.
	appResources, err := setup.SetupApp(workerImageName, controllerImageName)
	if err != nil {
		log.Logger.WithError(err).Fatal("Fatal error in setup")
	}
	defer appResources.Cancel()
	defer appResources.DockerCli.Close()

	jobsCh := make(chan types.Job, jobsCapacity)
	resultsCh := make(chan types.StreamingJobEvent, jobsCapacity)
	cancelJobCh := make(chan types.CancelJobRequest, jobsCapacity)

	defer close(resultsCh)

	mq.StartJobHandlers(mq.MQResources{
		AppResources: appResources,
		JobsCh:       jobsCh,
		CancelJobCh:  cancelJobCh,
		ResultsCh:    resultsCh,
	})

	db, err := db.NewPostgresRepository(appResources.Ctx)
	if err != nil {
		log.Logger.WithError(err).Fatal("Failed to connect to database")
	} else {
		log.Logger.Info("Connected to database")
	}
	defer db.Close()
	// Serve a metrics endpoint
	m := metrics.NewInMemoryMetricsCollector()
	server := metrics.NewHTTPServer(":8001", m)
	go server.Run(appResources.Ctx)

	wp := worker.NewDockerWorkerProducer(appResources.DockerCli, workerImageName)
	wm, err := worker.NewWorkerManager(numWorkers, wp)

	if err != nil {
		log.Logger.WithError(err).Fatal("Failed to create worker manager")
	}

	dispatcher := worker.NewJobDispatcher(
		cancelJobCh,
		jobsCh,
		resultsCh,
		wm,
		worker.NewDockerNetworkManager(appResources.DockerCli),
		db,
		m,
	)

	dispatcherDone := make(chan struct{})

	go func() {
		dispatcher.Run(appResources.Ctx)
		close(dispatcherDone)
	}()

	<-appResources.Ctx.Done()
	<-dispatcherDone
	if err := wm.Shutdown(); err != nil {
		log.Logger.WithError(err).Error("Error shutting down workers")
	}

	log.Logger.Info("All workers have finished. Exiting.")
}
