package setup

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/mount"
	"github.com/docker/docker/client"
	"github.com/google/uuid"
)

type AppResources struct {
	Ctx             context.Context
	Cancel          context.CancelFunc
	DockerCli       *client.Client
	WorkerImage     string
	ControllerImage string
}

// SetupApp initializes application resources required for running a worker.
// It sets up a cancellable context that responds to interrupt signals,
// creates a Docker client, and pre-pulls the specified worker image.
// It also pre-warms the cache by running a dummy container.
// Returns an AppResources struct containing the context, cancel function, and Docker client.
// If any step fails, it cleans up resources and returns an error.
func SetupApp(workerImageName string, controllerImageName string) (*AppResources, error) {
	// Setup context with cancellation on interrupt signals
	ctx, cancel := context.WithCancel(context.Background())
	handleSignals(cancel)

	// Initialize Docker client
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		cancel() // cleanup in case of error
		return nil, fmt.Errorf("failed to create Docker client: %w", err)
	}
	log.Println("Docker client initialized.")

	// Pre-pull the worker image
	effectiveWorkerImage, err := prePullImage(ctx, cli, workerImageName)
	if err != nil {
		cli.Close()
		cancel()
		return nil, fmt.Errorf("failed to pre-pull image %s: %w", workerImageName, err)
	}
	log.Printf("Image '%s' is ready.", effectiveWorkerImage)

	effectiveControllerImage, err := prePullImage(ctx, cli, controllerImageName)
	if err != nil {
		cli.Close()
		cancel()
		return nil, fmt.Errorf("failed to pre-pull image %s: %w", controllerImageName, err)
	}
	log.Printf("Image '%s' is ready.", effectiveControllerImage)

	// Pre-warm the cache by running a dummy container
	if err := prewarmCache(ctx, cli, effectiveWorkerImage); err != nil {
		cli.Close()
		cancel()
		return nil, fmt.Errorf("failed to pre-warm cache: %w", err)
	}
	log.Println("Cache pre-warming completed.")

	return &AppResources{
		Ctx:             ctx,
		Cancel:          cancel,
		DockerCli:       cli,
		WorkerImage:     effectiveWorkerImage,
		ControllerImage: effectiveControllerImage,
	}, nil
}

func prewarmCache(ctx context.Context, cli *client.Client, workerImageName string) error {
	log.Println("Initializing a new prewarming worker...")
	id := uuid.NewString()

	hostPath, err := os.MkdirTemp("", "prewarming-worker-"+id)
	if err != nil {
		return fmt.Errorf("failed to create temp dir: %w", err)
	}
	defer os.RemoveAll(hostPath)

	containerConfig := &container.Config{
		Image:      workerImageName,
		Cmd:        []string{"sleep", "infinity"},
		Tty:        false,
		WorkingDir: "/app",
	}
	hostConfig := &container.HostConfig{
		NetworkMode: "none",
		Mounts: []mount.Mount{
			{Type: mount.TypeVolume, Source: "go-build-cache", Target: "/root/.cache/go-build"},
			{Type: mount.TypeBind, Source: hostPath, Target: "/app"},
		},
	}

	resp, err := cli.ContainerCreate(ctx, containerConfig, hostConfig, nil, nil, "prewarm-"+id)
	if err != nil {
		os.RemoveAll(hostPath)
		return fmt.Errorf("failed to create prewarming container: %w", err)
	}

	defer func() {
		_ = cli.ContainerRemove(context.Background(), resp.ID, container.RemoveOptions{Force: true})
	}()

	log.Printf("Starting prewarming container %s...", resp.ID[:12])
	if err := cli.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		os.RemoveAll(hostPath)
		return fmt.Errorf("failed to start prewarming container: %w", err)
	}

	warmupCode := `package main
	import (
		"fmt"
		"os"
		"net/http"
		"encoding/json"
		"crypto/sha256"
		// add more common stdlib imports
	)
	func main() { fmt.Println("Prewarm done") }`

	if err := os.WriteFile(filepath.Join(hostPath, "main.go"), []byte(warmupCode), 0644); err != nil {
		return fmt.Errorf("failed to write warmup code: %w", err)
	}

	execConfig := container.ExecOptions{
		Cmd:          []string{"go", "run", "main.go"},
		AttachStdout: true,
		AttachStderr: true,
	}

	execID, err := cli.ContainerExecCreate(ctx, resp.ID, execConfig)

	if err != nil {
		return fmt.Errorf("failed to create exec instance: %w", err)
	}

	containerCtx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()

	if err := cli.ContainerExecStart(ctx, execID.ID, container.ExecStartOptions{Detach: true}); err != nil {
		return fmt.Errorf("failed to start exec: %w", err)
	}

	inspectResp, err := cli.ContainerExecInspect(containerCtx, execID.ID)
	if err != nil {
		return fmt.Errorf("failed to inspect exec instance: %w", err)
	}

	if inspectResp.ExitCode != 0 {
		return fmt.Errorf("execution finished with non-zero exit code: %d", inspectResp.ExitCode)
	}

	return nil
}

func prePullImage(ctx context.Context, cli *client.Client, imageName string) (string, error) {
	// Check if image exists locally
	images, err := cli.ImageList(ctx, image.ListOptions{})
	if err != nil {
		return "", fmt.Errorf("failed to list local images: %w", err)
	}
	for _, img := range images {
		for _, tag := range img.RepoTags {
			if tag == imageName {
				log.Printf("Image '%s' found locally, skipping pull.", imageName)
				return imageName, nil
			}
		}
	}

	// Pull only if not found locally
	log.Printf("Pulling Docker image '%s'...", imageName)
	reader, err := cli.ImagePull(ctx, imageName, image.PullOptions{})
	if err != nil {
		return "", err
	}
	defer reader.Close()

	_, err = io.Copy(os.Stdout, reader)
	if err != nil && err != io.EOF {
		log.Printf("Warning: Error reading image pull output: %v", err)
	}

	log.Printf("Image '%s' pulled successfully.", imageName)
	return imageName, nil
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
