package worker

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/DistCodeP7/distcode_worker/utils"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/mount"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
)

type Worker struct {
	containerID string
	dockerCli   *client.Client
	hostPath    string
}

type WorkerInterface interface {
	ID() string
	ConnectToNetwork(ctx context.Context, networkName, alias string) error
	DisconnectFromNetwork(ctx context.Context, networkName string) error
	Stop(ctx context.Context) error
	ExecuteCode(ctx context.Context, code string, stdoutCh, stderrCh chan string) error
}

var _ WorkerInterface = (*Worker)(nil)

func NewWorker(ctx context.Context, cli *client.Client, workerImageName string) (*Worker, error) {
	log.Println("Initializing a new worker...")

	hostPath, err := os.MkdirTemp("", "docker-worker-*")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp dir: %w", err)
	}

	containerConfig := &container.Config{
		Image:      workerImageName,
		Cmd:        []string{"sleep", "infinity"},
		Tty:        false,
		WorkingDir: "/app",
	}

	hostConfig := &container.HostConfig{
		Mounts: []mount.Mount{
			{
				Type:   mount.TypeBind,
				Source: hostPath,
				Target: "/tmp/dsnet-code",
			},
			{
				Type:   mount.TypeVolume,
				Source: "go-build-cache",
				Target: "/root/.cache/go-build",
			},
		},
		Resources: container.Resources{
			CPUShares:      512,
			NanoCPUs:       500_000_000,          // 0.5 CPU
			Memory:         256 * 1024 * 1024,    // 256MB
			PidsLimit:      utils.PtrInt64(50),   // max 50 processes
			MemorySwap:     512 * 1024 * 1024,    // 512MB - gVisor needs swap > memory
			OomKillDisable: utils.PtrBool(false), // enable OOM killer
			Ulimits: []*container.Ulimit{
				{Name: "cpu", Soft: 30, Hard: 30},        // 30s CPU limit
				{Name: "nofile", Soft: 1024, Hard: 1024}, // max open files
			},
		},
	}

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
		containerID: resp.ID,
		dockerCli:   cli,
		hostPath:    hostPath,
	}

	log.Printf("Worker initialized with container %s", worker.containerID[:12])
	return worker, nil
}

func (w *Worker) ID() string {
	return w.containerID
}

func (w *Worker) ConnectToNetwork(ctx context.Context, networkName, alias string) error {
	return w.dockerCli.NetworkConnect(ctx, networkName, w.containerID, &network.EndpointSettings{
		Aliases: []string{alias},
	})
}

func (w *Worker) DisconnectFromNetwork(ctx context.Context, networkName string) error {
	return w.dockerCli.NetworkDisconnect(ctx, networkName, w.containerID, true)
}

func (w *Worker) Stop(ctx context.Context) error {
	log.Printf("Stopping and removing container %s", w.containerID[:12])

	if err := w.dockerCli.ContainerStop(ctx, w.containerID, container.StopOptions{Timeout: nil}); err != nil {
		log.Printf("Warning: failed to gracefully stop container %s: %v", w.containerID[:12], err)
	}

	if err := w.dockerCli.ContainerRemove(ctx, w.containerID, container.RemoveOptions{Force: true}); err != nil {
		return fmt.Errorf("failed to remove container: %w", err)
	}

	if err := os.RemoveAll(w.hostPath); err != nil {
		return fmt.Errorf("failed to remove host path: %w", err)
	}

	return nil
}

func (w *Worker) ExecuteCode(ctx context.Context, code string, stdoutCh, stderrCh chan string) error {
	codePath := filepath.Join(w.hostPath, "main.go")
	if err := os.WriteFile(codePath, []byte(code), 0644); err != nil {
		return fmt.Errorf("failed to write code to file: %w", err)
	}

	execConfig := container.ExecOptions{
		Cmd:          []string{"go", "run", "/tmp/dsnet-code/main.go"},
		AttachStdout: true,
		AttachStderr: true,
		WorkingDir:   "/app",
	}

	execID, err := w.dockerCli.ContainerExecCreate(ctx, w.containerID, execConfig)
	if err != nil {
		return fmt.Errorf("failed to create exec instance: %w", err)
	}

	containerCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	hijackedResp, err := w.dockerCli.ContainerExecAttach(containerCtx, execID.ID, container.ExecStartOptions{
		Detach: false,
		Tty:    false,
	})
	if err != nil {
		return fmt.Errorf("failed to attach to exec instance: %w", err)
	}
	defer hijackedResp.Close()

	stdoutWriter := newChannelWriter(stdoutCh)
	stderrWriter := newChannelWriter(stderrCh)
	done := make(chan error)

	go func() {
		_, err := stdcopy.StdCopy(
			stdoutWriter,
			stderrWriter,
			hijackedResp.Reader,
		)
		stdoutWriter.Flush()
		stderrWriter.Flush()
		done <- err
	}()

	if err := <-done; err != nil {
		return fmt.Errorf("failed to stream output: %w", err)
	}

	inspectResp, err := w.dockerCli.ContainerExecInspect(containerCtx, execID.ID)
	if err != nil {
		return fmt.Errorf("failed to inspect exec instance: %w", err)
	}

	if inspectResp.ExitCode != 0 {
		return fmt.Errorf("execution finished with non-zero exit code: %d", inspectResp.ExitCode)
	}

	return nil
}

type channelWriter struct {
	ch  chan string
	buf bytes.Buffer
}

func newChannelWriter(ch chan string) *channelWriter {
	return &channelWriter{ch: ch}
}

// Write implements the io.Writer interface for channelWriter.
// It writes the provided byte slice to an internal buffer, splitting the input at newline characters.
// For each complete line (ending with '\n'), it sends the buffered string to the associated channel and resets the buffer.
func (cw *channelWriter) Write(p []byte) (int, error) {
	total := 0
	for len(p) > 0 {
		i := bytes.IndexByte(p, '\n')
		if i == -1 {
			cw.buf.Write(p)
			total += len(p)
			break
		}
		cw.buf.Write(p[:i])
		cw.ch <- cw.buf.String()
		cw.buf.Reset()
		p = p[i+1:]
		total += i + 1
	}
	return total, nil
}

func (cw *channelWriter) Flush() {
	if cw.buf.Len() > 0 {
		cw.ch <- cw.buf.String()
		cw.buf.Reset()
	}
}
