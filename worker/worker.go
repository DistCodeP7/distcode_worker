package worker

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/mount"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
	"github.com/google/uuid"
)

type Worker struct {
	id          string
	dockerCli   *client.Client
	containerID string
	hostPath    string
}

type Result struct {
	Stdout string
	Stderr string
	Err    error
}

func New(ctx context.Context, cli *client.Client) (*Worker, error) {
	log.Println("Initializing a new worker...")

	hostPath, err := os.MkdirTemp("", "docker-worker-*")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp dir: %w", err)
	}

	containerConfig := &container.Config{
		Image:      "golang:1.25",
		Cmd:        []string{"sleep", "infinity"},
		Tty:        false,
		WorkingDir: "/app",
	}

	hostConfig := &container.HostConfig{
		Mounts: []mount.Mount{
			{
				Type:   mount.TypeBind,
				Source: hostPath,
				Target: "/app",
			},
		},
		Resources: container.Resources{
			Memory:   512 * 1024 * 1024,
			NanoCPUs: 1_000_000_000,
		},
	}

	log.Println("Creating container...")
	resp, err := cli.ContainerCreate(ctx, containerConfig, hostConfig, nil, nil, "")
	if err != nil {
		os.RemoveAll(hostPath)
		return nil, fmt.Errorf("failed to create container: %w", err)
	}

	log.Printf("Starting container %s...", resp.ID[:12])
	if err := cli.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		os.RemoveAll(hostPath)
		return nil, fmt.Errorf("failed to start container: %w", err)
	}

	worker := &Worker{
		id:          uuid.NewString(),
		dockerCli:   cli,
		containerID: resp.ID,
		hostPath:    hostPath,
	}

	log.Printf("Worker %s initialized with container %s", worker.id, worker.containerID[:12])
	return worker, nil
}

func (w *Worker) ExecuteCode(ctx context.Context, code string) (string, string, error) {
	codePath := filepath.Join(w.hostPath, "main.go")
	if err := os.WriteFile(codePath, []byte(code), 0644); err != nil {
		return "", "", fmt.Errorf("failed to write code to file: %w", err)
	}

	execConfig := container.ExecOptions{
		Cmd:          []string{"go", "run", "main.go"},
		AttachStdout: true,
		AttachStderr: true,
	}

	execID, err := w.dockerCli.ContainerExecCreate(ctx, w.containerID, execConfig)
	if err != nil {
		return "", "", fmt.Errorf("failed to create exec instance: %w", err)
	}

	hijackedResp, err := w.dockerCli.ContainerExecAttach(ctx, execID.ID, container.ExecStartOptions{})
	if err != nil {
		return "", "", fmt.Errorf("failed to attach to exec instance: %w", err)
	}
	defer hijackedResp.Close()

	var stdoutBuf, stderrBuf bytes.Buffer

	_, err = stdcopy.StdCopy(&stdoutBuf, &stderrBuf, hijackedResp.Reader)
	if err != nil {
		return "", "", fmt.Errorf("failed to demultiplex stream: %w", err)
	}

	inspectResp, err := w.dockerCli.ContainerExecInspect(ctx, execID.ID)
	if err != nil {
		return "", "", fmt.Errorf("failed to inspect exec instance: %w", err)
	}

	if inspectResp.ExitCode != 0 {
		// Even with a non-zero exit, stdout and stderr might contain useful info.
		return stdoutBuf.String(), stderrBuf.String(),
			fmt.Errorf("execution finished with non-zero exit code: %d", inspectResp.ExitCode)
	}

	return stdoutBuf.String(), stderrBuf.String(), nil
}

func (w *Worker) Stop(ctx context.Context) error {
	log.Printf("Stopping worker %s and removing container %s", w.id, w.containerID[:12])

	if err := w.dockerCli.ContainerStop(ctx, w.containerID, container.StopOptions{Timeout: nil}); err != nil {
		log.Printf("Warning: failed to gracefully stop container %s: %v", w.containerID[:12], err)
	}

	err := w.dockerCli.ContainerRemove(ctx, w.containerID, container.RemoveOptions{Force: true})
	if err != nil {
		return fmt.Errorf("failed to remove container: %w", err)
	}

	if err := os.RemoveAll(w.hostPath); err != nil {
		return fmt.Errorf("failed to remove host path: %w", err)
	}

	return nil
}
