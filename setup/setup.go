package setup

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/DistCodeP7/distcode_worker/db"
	"github.com/DistCodeP7/distcode_worker/dockercli"
	"github.com/DistCodeP7/distcode_worker/log"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/mount"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
)

type AppResources struct {
	Ctx             context.Context
	Cancel          context.CancelFunc
	DockerCli       dockercli.DockerClient
	WorkerImage     string
	ControllerImage string
	DB              db.Repository
}

// SetupApp initializes application resources required for running a worker.
// It sets up a cancellable context that responds to interrupt signals,
// creates a Docker client, and pre-pulls the specified worker image.
// It also pre-warms the cache by running a dummy container.
// Returns an AppResources struct containing the context, cancel function, and Docker client.
// If any step fails, it cleans up resources and returns an error.
func SetupApp(workerImageName string) (*AppResources, error) {
	ctx, cancel := context.WithCancel(context.Background())
	handleSignals(cancel)

	cli, err := dockercli.NewClientFromEnv()
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create Docker client: %w", err)
	}
	log.Logger.Info("Docker client initialized")

	effectiveWorkerImage, err := prePullImage(ctx, cli, workerImageName)
	if err != nil {
		cli.Close()
		cancel()
		log.Logger.WithField("image", workerImageName).Fatal("Failed to pre-pull worker image")
	}

	if err := prewarmCache(ctx, cli, effectiveWorkerImage); err != nil {
		cli.Close()
		cancel()
		log.Logger.WithField("image", effectiveWorkerImage).Error("Failed to pre-warm cache")
		return nil, err
	}

	db, err := db.NewPostgresRepository(ctx)
	if err != nil {
		log.Logger.WithError(err).Fatal("Failed to connect to database")
	} else {
		log.Logger.Info("Connected to database")
	}

	log.Logger.WithFields(logrus.Fields{
		"worker_image": effectiveWorkerImage,
	}).Info("Application setup completed successfully")

	return &AppResources{
		Ctx:         ctx,
		Cancel:      cancel,
		DockerCli:   *cli,
		WorkerImage: effectiveWorkerImage,
		DB:          db,
	}, nil
}

func prewarmCache(ctx context.Context, cli dockercli.Client, workerImageName string) error {
	id := uuid.NewString()

	hostPath, err := os.MkdirTemp("", "prewarming-worker-"+id)
	if err != nil {
		return fmt.Errorf("failed to create temp dir: %w", err)
	}
	defer os.RemoveAll(hostPath)

	log.Logger.WithFields(logrus.Fields{
		"container_id": "prewarm-" + id,
		"temp_path":    hostPath,
	}).Info("Initializing prewarming worker")

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
		log.Logger.WithFields(logrus.Fields{"temp_path": hostPath}).Error("Failed to create prewarming container")
		return fmt.Errorf("failed to create prewarming container: %w", err)
	}

	defer func() {
		_ = cli.ContainerRemove(context.Background(), resp.ID, container.RemoveOptions{Force: true})
	}()

	if err := cli.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		log.Logger.WithField("container", resp.ID[:12]).Error("Failed to start prewarming container")
		return fmt.Errorf("failed to start prewarming container: %w", err)
	}

	warmupCode := `package main
	import (
		"fmt"
		"os"
		"net/http"
		"encoding/json"
		"crypto/sha256"
	)
	func main() { fmt.Println("Prewarm done") }`

	if err := os.WriteFile(filepath.Join(hostPath, "main.go"), []byte(warmupCode), 0644); err != nil {
		log.Logger.WithField("path", hostPath).Error("Failed to write warmup code")
		return fmt.Errorf("failed to write warmup code: %w", err)
	}

	execConfig := container.ExecOptions{
		Cmd:          []string{"go", "run", "main.go"},
		AttachStdout: true,
		AttachStderr: true,
	}

	execID, err := cli.ContainerExecCreate(ctx, resp.ID, execConfig)
	if err != nil {
		log.Logger.WithField("container", resp.ID[:12]).Error("Failed to create exec instance")
		return fmt.Errorf("failed to create exec instance: %w", err)
	}

	containerCtx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()

	if err := cli.ContainerExecStart(ctx, execID.ID, container.ExecStartOptions{Detach: true}); err != nil {
		log.Logger.WithField("exec_id", execID.ID).Error("Failed to start exec instance")
		return fmt.Errorf("failed to start exec: %w", err)
	}

	inspectResp, err := cli.ContainerExecInspect(containerCtx, execID.ID)
	if err != nil {
		log.Logger.WithField("exec_id", execID.ID).Error("Failed to inspect exec instance")
		return fmt.Errorf("failed to inspect exec instance: %w", err)
	}

	if inspectResp.ExitCode != 0 {
		log.Logger.WithFields(logrus.Fields{
			"container": resp.ID[:12],
			"exit_code": inspectResp.ExitCode,
		}).Error("Prewarm execution finished with non-zero exit code")
		return fmt.Errorf("execution finished with non-zero exit code: %d", inspectResp.ExitCode)
	}

	log.Logger.WithField("container", resp.ID[:12]).Info("Prewarm container executed successfully")
	return nil
}

func prePullImage(ctx context.Context, cli dockercli.Client, imageName string) (string, error) {
	images, err := cli.ImageList(ctx, image.ListOptions{})
	if err != nil {
		log.Logger.WithField("image", imageName).Error("Failed to list local images")
		return "", fmt.Errorf("failed to list local images: %w", err)
	}

	for _, img := range images {
		for _, tag := range img.RepoTags {
			if tag == imageName {
				log.Logger.WithField("image", imageName).Debug("Image found locally, skipping pull")
				return imageName, nil
			}
		}
	}

	log.Logger.WithField("image", imageName).Info("Pulling Docker image")
	reader, err := cli.ImagePull(ctx, imageName, image.PullOptions{})
	if err != nil {
		log.Logger.WithField("image", imageName).Error("Failed to pull image")
		return "", err
	}
	defer reader.Close()

	_, err = io.Copy(os.Stdout, reader)
	if err != nil && err != io.EOF {
		log.Logger.WithField("image", imageName).Warn("Error reading image pull output")
		return "", fmt.Errorf("failed to read image pull output: %w", err)
	}

	log.Logger.WithField("image", imageName).Debug("Image pulled successfully")
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
		log.Logger.WithField("signal", sig).Info("Received termination signal, initiating graceful shutdown")
		cancel()
	}()
}
