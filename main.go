package main

import (
	"context"
	"flag"
	"io"
	"log"
	"os"
	"os/signal"
	"slices"
	"sync"
	"syscall"
	"time"

	"github.com/DistCodeP7/distcode_worker/reciever"
	"github.com/DistCodeP7/distcode_worker/types"
	"github.com/DistCodeP7/distcode_worker/worker"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
)

type WorkerConfig struct {
	Ctx       context.Context
	DockerCli *client.Client
	Wg        *sync.WaitGroup
	Jobs      <-chan types.Job
	Results   chan<- types.JobResult
}

func main() {
	// Recieve the image name from command line flag e.g `go run main.go -i golang:1.25`
	workerImageName := flag.String("i", "golang:1.25", "The Docker image to use for workers")
	numWorkers := flag.Int("w", 4, "The number of worker goroutines to start")
	flag.Parse()

	// Setup context with cancellation on interrupt signals
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	handleSignals(cancel)

	// Initialize Docker client
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		log.Fatalf("Failed to create Docker client: %v", err)
		return
	}
	defer cli.Close()

	// Pass the value of the flag to the function
	if err := prePullImage(ctx, cli, *workerImageName); err != nil {
		log.Fatalf("Failed to pre-pull image %s: %v", *workerImageName, err)
	}

	jobs := make(chan types.Job, 10)
	results := make(chan types.JobResult, 10)

	var wg sync.WaitGroup

	go func() {
		if err := reciever.MQ(ctx, jobs); err != nil {
			log.Fatalf("MQ error: %v", err)
		}
	}()

	workerConfig := WorkerConfig{
		Ctx:       ctx,
		DockerCli: cli,
		Wg:        &wg,
		Jobs:      jobs,
		Results:   results,
	}

	log.Printf("Starting %d workers...", *numWorkers)
	for i := 1; i <= *numWorkers; i++ {
		wg.Add(1)
		go startWorker(&workerConfig, i)
	}

	// Another goroutine to collect and log results from workers
	go func() {
		for result := range results {
			log.Printf("--- Job %d Result ---", result.JobID)
			if result.Result.Err != nil {
				log.Printf("Error: %v", result.Result.Err)
			}
			if result.Result.Stdout != "" {
				log.Printf("Stdout: %s", result.Result.Stdout)
			}
			if result.Result.Stderr != "" {
				log.Printf("Stderr: %s", result.Result.Stderr)
			}
			log.Println("--------------------")
		}
	}()

	wg.Wait()
	close(results)
	log.Println("All workers have finished. Exiting.")
}

func prePullImage(ctx context.Context, cli *client.Client, imageName string) error {
	log.Printf("Pulling Docker image '%s'...", imageName)

	// Check if the image already exists locally
	images, err := cli.ImageList(ctx, image.ListOptions{})
	if err != nil {
		return err
	}
	for _, img := range images {
		if slices.Contains(img.RepoTags, imageName) {
			log.Printf("Image '%s' already exists locally. Skipping pull.", imageName)
			return nil
		}
	}

	reader, err := cli.ImagePull(ctx, imageName, image.PullOptions{})
	if err != nil {
		return err
	}
	defer reader.Close()

	// Use io.Copy to block until the pull is complete and show progress.
	_, err = io.Copy(os.Stdout, reader)
	if err != nil && err != io.EOF {
		log.Printf("Warning: Error reading image pull output: %v", err)
	}
	log.Printf("Image '%s' pulled successfully.", imageName)
	return nil
}

func startWorker(config *WorkerConfig, workerID int) {
	defer config.Wg.Done()
	log.Printf("Worker %d starting...", workerID)

	w, err := worker.New(config.Ctx, config.DockerCli)
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
			log.Printf("Worker %d picked up Job %d", workerID, job.ID)

			execCtx, cancelExec := context.WithTimeout(config.Ctx, 30*time.Second)

			stdout, stderr, err := w.ExecuteCode(execCtx, job.Code)
			config.Results <- types.JobResult{
				JobID: job.ID,
				Result: worker.Result{
					Stdout: stdout,
					Stderr: stderr,
					Err:    err,
				},
			}
			cancelExec()

		case <-config.Ctx.Done():

			log.Printf("Worker %d received shutdown signal. Exiting.", workerID)
			return
		}
	}
}

func handleSignals(cancel context.CancelFunc) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		log.Printf("Received signal %v. Initiating graceful shutdown...", sig)
		cancel()
	}()
}
