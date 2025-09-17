package setup

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"slices"
	"syscall"

	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
)

type AppResources struct {
	Ctx       context.Context
	Cancel    context.CancelFunc
	DockerCli *client.Client
}

// SetupApp initializes application resources required for running a worker.
// It sets up a cancellable context that responds to interrupt signals,
// creates a Docker client, and pre-pulls the specified worker image.
// Returns an AppResources struct containing the context, cancel function, and Docker client.
// If any step fails, it cleans up resources and returns an error.
func SetupApp(workerImageName string) (*AppResources, error) {
	// Setup context with cancellation on interrupt signals
	ctx, cancel := context.WithCancel(context.Background())
	handleSignals(cancel)

	// Initialize Docker client
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		cancel() // cleanup in case of error
		return nil, fmt.Errorf("failed to create Docker client: %w", err)
	}

	// Pre-pull the worker image
	if err := prePullImage(ctx, cli, workerImageName); err != nil {
		cli.Close()
		cancel()
		return nil, fmt.Errorf("failed to pre-pull image %s: %w", workerImageName, err)
	}

	return &AppResources{
		Ctx:       ctx,
		Cancel:    cancel,
		DockerCli: cli,
	}, nil
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

// handleSignals sets up a signal handler to listen for SIGINT and SIGTERM signals.
// When one of these signals is received, it logs the event and calls the provided
// cancel function to initiate a graceful shutdown of the application.
//
// The signal handling is performed in a separate goroutine to avoid blocking the main flow.
//
// Parameters:
//   - cancel: A context.CancelFunc that will be called when a termination signal is received.
func handleSignals(cancel context.CancelFunc) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		log.Printf("Received signal %v. Initiating graceful shutdown...", sig)
		cancel()
	}()
}
