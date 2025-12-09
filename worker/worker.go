// Package worker wraps the Docker API for executing Golang applications using dsnet
package worker

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/DistCodeP7/distcode_worker/dockercli"
	"github.com/DistCodeP7/distcode_worker/log"
	t "github.com/DistCodeP7/distcode_worker/types"
	"github.com/DistCodeP7/distcode_worker/utils"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/mount"
	"github.com/docker/docker/pkg/stdcopy"
	"github.com/sirupsen/logrus"
)

type DockerWorker struct {
	alias       string
	containerID string
	dockerCli   dockercli.WorkerRuntime
}

type Worker interface {
	ID() string
	Alias() string
	Stop(ctx context.Context) error
	ExecuteCommand(ctx context.Context, options ExecuteCommandOptions) error
	ReadFile(ctx context.Context, path string) ([]byte, error)
}

var _ Worker = (*DockerWorker)(nil)

// ID returns the internal container ID of the worker.
func (w *DockerWorker) ID() string {
	return w.containerID
}

// Alias returns the alias of the worker.
func (w *DockerWorker) Alias() string {
	return w.alias
}

// Stop stops and removes the container. Logs only container stop/removal events.
func (w *DockerWorker) Stop(ctx context.Context) error {
	log.Logger.Tracef("Stopping container %s", w.containerID[:12])

	timeout := 3
	if err := w.dockerCli.ContainerStop(ctx, w.containerID, container.StopOptions{Timeout: &timeout}); err != nil {
		log.Logger.Warnf("Failed to gracefully stop container %s: %v", w.containerID[:12], err)
	}

	if err := w.dockerCli.ContainerRemove(ctx, w.containerID, container.RemoveOptions{Force: true}); err != nil {
		return fmt.Errorf("failed to forcefully remove container: %w", err)
	}

	log.Logger.Infof("Container %s stopped and removed", w.containerID[:12])
	return nil
}

// NewWorker creates and starts a new Docker container and returns the Worker instance.
// Errors are returned for the caller to log if needed.
func NewWorker(ctx context.Context, cli dockercli.Client, workerImageName string, spec t.NodeSpec) (*DockerWorker, error) {
	envVars := make([]string, 0, len(spec.Envs))
	for _, env := range spec.Envs {
		envVars = append(envVars, fmt.Sprintf("%s=%s", env.Key, env.Value))
	}

	containerConfig := &container.Config{
		Image:      workerImageName,
		Cmd:        []string{"sleep", "infinity"},
		Env:        envVars,
		Tty:        false,
		WorkingDir: "/app/tmp",
	}

	hostConfig := &container.HostConfig{
		Mounts: []mount.Mount{
			{
				Type:   mount.TypeVolume,
				Source: "go-build-cache",
				Target: "/root/.cache/go-build",
			},
		},
		Resources: container.Resources{
			CPUShares:      512,
			NanoCPUs:       1_000_000_000,
			Memory:         512 * 1024 * 1024,
			MemorySwap:     1024 * 1024 * 1024,
			PidsLimit:      utils.PtrInt64(1024),
			OomKillDisable: utils.PtrBool(false),
			Ulimits: []*container.Ulimit{
				{Name: "cpu", Soft: 30, Hard: 30},
				{Name: "nofile", Soft: 1024, Hard: 1024},
			},
		},
	}

	resp, err := cli.ContainerCreate(ctx, containerConfig, hostConfig, nil, nil, "")
	if err != nil {
		return nil, fmt.Errorf("failed to create container: %w", err)
	}

	w := &DockerWorker{
		containerID: resp.ID,
		dockerCli:   cli,
		alias:       spec.Alias,
	}

	if err := cli.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		_ = w.Stop(ctx)
		return nil, fmt.Errorf("failed to start container: %w", err)
	}

	tarStream, err := utils.CreateTarStream(spec.Files)
	if err != nil {
		_ = w.Stop(ctx)
		return nil, fmt.Errorf("failed to create tar stream: %w", err)
	}

	if err := cli.CopyToContainer(ctx, w.ID(), "/app/tmp", tarStream, container.CopyToContainerOptions{
		AllowOverwriteDirWithFile: true,
	}); err != nil {
		_ = w.Stop(ctx)
		return nil, fmt.Errorf("failed to copy spec files to container: %w", err)
	}

	log.Logger.WithFields(
		logrus.Fields{
			"alias":        spec.Alias,
			"container_id": resp.ID[:12],
		},
	).Info("Creating worker container")

	log.Logger.Infof("Worker initialized with container %s", w.containerID[:12])
	return w, nil
}

type ExecuteCommandOptions struct {
	Cmd          string
	Envs         []t.EnvironmentVariable
	OutputWriter io.Writer
}

// ExecuteCommand executes a command and streams stdout/stderr.
// Only logs cancellation; errors are returned for the caller to handle/log.
func (w *DockerWorker) ExecuteCommand(ctx context.Context, e ExecuteCommandOptions) error {
	execEnvStrings := make([]string, 0, len(e.Envs))
	for _, env := range e.Envs {
		execEnvStrings = append(execEnvStrings, fmt.Sprintf("%s=%s", env.Key, env.Value))
	}

	execConfig := container.ExecOptions{
		Cmd:          []string{"/bin/sh", "-c", e.Cmd},
		WorkingDir:   "/app/tmp",
		AttachStdout: true,
		AttachStderr: true,
		Env:          execEnvStrings,
	}

	execID, err := w.dockerCli.ContainerExecCreate(ctx, w.containerID, execConfig)
	if err != nil {
		return fmt.Errorf("failed to create exec instance: %w", err)
	}

	resp, err := w.dockerCli.ContainerExecAttach(ctx, execID.ID, container.ExecStartOptions{
		Detach: false,
		Tty:    false,
	})
	if err != nil {
		return fmt.Errorf("failed to attach to exec instance: %w", err)
	}
	defer resp.Close()

	done := make(chan error, 1)
	go func() {
		_, err := stdcopy.StdCopy(e.OutputWriter, e.OutputWriter, resp.Reader)
		done <- err
	}()

	select {
	case <-ctx.Done():
		log.Logger.Warnf("Job cancelled in container %s", w.containerID[:12])
		return ctx.Err()
	case err := <-done:
		if err != nil && err != io.EOF {
			return fmt.Errorf("failed to stream output: %w", err)
		}
	}

	inspectResp, err := w.dockerCli.ContainerExecInspect(ctx, execID.ID)
	if err != nil {
		return fmt.Errorf("failed to inspect exec instance: %w", err)
	}
	if inspectResp.ExitCode != 0 {
		return fmt.Errorf("execution finished with non-zero exit code: %d", inspectResp.ExitCode)
	}

	return nil
}

func (w *DockerWorker) ReadFile(ctx context.Context, path string) ([]byte, error) {
	reader, _, err := w.dockerCli.CopyFromContainer(ctx, w.containerID, path)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	tr := tar.NewReader(reader)
	_, err = tr.Next()
	if err != nil {
		return nil, err
	}

	buf := new(bytes.Buffer)
	if _, err := io.Copy(buf, tr); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}
