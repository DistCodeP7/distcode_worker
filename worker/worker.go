package worker

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

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

func New(ctx context.Context, cli *client.Client) (*Worker, error) {
	log.Println("Initializing a new worker...")
	id := uuid.NewString()

	hostPath, err := os.MkdirTemp("", "docker-worker-"+id)
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
		NetworkMode: "none",
		Runtime: "runsc",
		Mounts: []mount.Mount{
			{
				Type:   mount.TypeBind,
				Source: hostPath,
				Target: "/app",
			},
		},
		Resources: container.Resources{
			CPUShares:      512,
			NanoCPUs:       500_000_000,       // 0.5 CPU
			Memory:         256 * 1024 * 1024, // 256MB
			PidsLimit:      ptrInt64(50),      // max 50 processes
			MemorySwap:     512 * 1024 * 1024, // 512MB - gVisor needs swap > memory
			OomKillDisable: ptrBool(false),    // enable OOM killer
			Ulimits: []*container.Ulimit{
				{Name: "cpu", Soft: 30, Hard: 30},        // 30s CPU limit
				{Name: "nofile", Soft: 1024, Hard: 1024}, // max open files
			},
		},
	}

	log.Println("Creating container...")
	resp, err := cli.ContainerCreate(ctx, containerConfig, hostConfig, nil, nil, "worker-"+id)
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
		id:          id,
		dockerCli:   cli,
		containerID: resp.ID,
		hostPath:    hostPath,
	}

	log.Printf("Worker %s initialized with container %s", worker.id, worker.containerID[:12])
	return worker, nil
}

func (w *Worker) warmupWorker(ctx context.Context) error {
	warmupcode := `package main 
	import "fmt"
	func main() {
		fmt.Println("Warmup")
	}`

	_, _, err := w.ExecuteCode(ctx, warmupcode)
	return err
}

func ptrBool(b bool) *bool {
	return &b
}

func ptrInt64(i int) *int64 {
	v := int64(i)
	return &v
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

	// Use separate contexts for execution and inspection
	exec_ctx, execCancel := context.WithTimeout(ctx, 90*time.Second)
	defer execCancel()
	
	hijackedResp, err := w.dockerCli.ContainerExecAttach(exec_ctx, execID.ID, container.ExecStartOptions{
		Detach: false,
		Tty:    false,
	})
	if err != nil {
		return "", "", fmt.Errorf("failed to attach to exec instance: %w", err)
	}
	defer hijackedResp.Close()

	var stdoutBuf, stderrBuf bytes.Buffer

	_, err = stdcopy.StdCopy(&stdoutBuf, &stderrBuf, hijackedResp.Reader)
	if err != nil {
		return "", "", fmt.Errorf("failed to demultiplex stream: %w", err)
	}

	// Use a fresh context for inspection to avoid timeout issues
	inspect_ctx, inspectCancel := context.WithTimeout(ctx, 30*time.Second)
	defer inspectCancel()

	inspectResp, err := w.dockerCli.ContainerExecInspect(inspect_ctx, execID.ID)
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
