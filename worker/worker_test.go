package worker

import (
	"archive/tar"
	"bufio"
	"bytes"
	"context"
	"io"
	"net"
	"testing"

	ty "github.com/DistCodeP7/distcode_worker/types"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/network"
	"github.com/google/uuid"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

type MockDockerCli struct {
	ContainerCreateFn      func(ctx context.Context, config *container.Config, host *container.HostConfig, networking interface{}, platform interface{}, containerName string) (container.CreateResponse, error)
	ContainerStartFn       func(ctx context.Context, containerID string, opts container.StartOptions) error
	ContainerStopFn        func(ctx context.Context, containerID string, opts container.StopOptions) error
	ContainerRemoveFn      func(ctx context.Context, containerID string, opts container.RemoveOptions) error
	CopyToContainerFn      func(ctx context.Context, containerID, path string, content io.Reader, opts container.CopyToContainerOptions) error
	CopyFromContainerFn    func(ctx context.Context, containerID, path string) (io.ReadCloser, container.PathStat, error)
	ContainerExecCreateFn  func(ctx context.Context, id string, opts container.ExecOptions) (container.ExecCreateResponse, error)
	ContainerExecAttachFn  func(ctx context.Context, id string, opts container.ExecStartOptions) (types.HijackedResponse, error)
	ContainerExecInspectFn func(ctx context.Context, id string) (container.ExecInspect, error)
	ContainerExecStartFn   func(ctx context.Context, id string, opts container.ExecStartOptions) error
	CloseFn                func() error
}

func (m *MockDockerCli) ContainerCreate(ctx context.Context, config *container.Config, hostConfig *container.HostConfig, networkingConfig *network.NetworkingConfig, platform *v1.Platform, containerName string) (container.CreateResponse, error) {
	return m.ContainerCreateFn(ctx, config, hostConfig, networkingConfig, platform, containerName)
}

func (m *MockDockerCli) ContainerStart(ctx context.Context, containerID string, opts container.StartOptions) error {
	return m.ContainerStartFn(ctx, containerID, opts)
}

func (m *MockDockerCli) ContainerStop(ctx context.Context, containerID string, opts container.StopOptions) error {
	return m.ContainerStopFn(ctx, containerID, opts)
}

func (m *MockDockerCli) ContainerRemove(ctx context.Context, containerID string, opts container.RemoveOptions) error {
	return m.ContainerRemoveFn(ctx, containerID, opts)
}

func (m *MockDockerCli) CopyToContainer(ctx context.Context, containerID, path string, content io.Reader, opts container.CopyToContainerOptions) error {
	return m.CopyToContainerFn(ctx, containerID, path, content, opts)
}

func (m *MockDockerCli) CopyFromContainer(ctx context.Context, containerID, path string) (io.ReadCloser, container.PathStat, error) {
	return m.CopyFromContainerFn(ctx, containerID, path)
}

func (m *MockDockerCli) ContainerExecCreate(ctx context.Context, id string, opts container.ExecOptions) (container.ExecCreateResponse, error) {
	return m.ContainerExecCreateFn(ctx, id, opts)
}

func (m *MockDockerCli) ContainerExecStart(ctx context.Context, id string, opts container.ExecStartOptions) error {
	return m.ContainerExecStartFn(ctx, id, opts)
}

func (m *MockDockerCli) ContainerExecAttach(ctx context.Context, id string, opts container.ExecStartOptions) (types.HijackedResponse, error) {
	return m.ContainerExecAttachFn(ctx, id, opts)
}

func (m *MockDockerCli) ContainerExecInspect(ctx context.Context, id string) (container.ExecInspect, error) {
	return m.ContainerExecInspectFn(ctx, id)
}

func (m *MockDockerCli) Close() error {
	return m.CloseFn()
}

var (
	id = uuid.NewString()
)

func TestDockerWorker_ID_Alias(t *testing.T) {
	w := &DockerWorker{containerID: id, alias: "node-x"}
	if w.ID() != id {
		t.Errorf("expected ID %s, got %s", id, w.ID())
	}
	if w.Alias() != "node-x" {
		t.Errorf("expected Alias node-x, got %s", w.Alias())
	}
}

func TestDockerWorker_Stop(t *testing.T) {
	stopCalled := false
	removeCalled := false

	mock := &MockDockerCli{
		ContainerStopFn: func(ctx context.Context, id string, opts container.StopOptions) error {
			stopCalled = true
			return nil
		},
		ContainerRemoveFn: func(ctx context.Context, id string, opts container.RemoveOptions) error {
			removeCalled = true
			return nil
		},
	}

	w := &DockerWorker{dockerCli: mock, containerID: uuid.NewString()}
	if err := w.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}

	if !stopCalled {
		t.Error("expected ContainerStop to be called")
	}
	if !removeCalled {
		t.Error("expected ContainerRemove to be called")
	}
}

func TestDockerWorker_ExecuteCommand_Cancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Context is cancelled immediately

	mock := &MockDockerCli{
		ContainerExecCreateFn: func(ctx context.Context, id string, opts container.ExecOptions) (container.ExecCreateResponse, error) {
			return container.ExecCreateResponse{ID: "exec123"}, nil
		},
		ContainerExecAttachFn: func(ctx context.Context, id string, opts container.ExecStartOptions) (types.HijackedResponse, error) {
			client, server := net.Pipe()
			server.Close()

			return types.HijackedResponse{
				Conn:   client,
				Reader: bufio.NewReader(client),
			}, nil
		},
		ContainerExecInspectFn: func(ctx context.Context, id string) (container.ExecInspect, error) {
			return container.ExecInspect{ExitCode: 0}, nil
		},
	}

	w := &DockerWorker{dockerCli: mock, containerID: id}
	err := w.ExecuteCommand(ctx, ExecuteCommandOptions{
		Cmd:          "echo",
		OutputWriter: io.Discard,
	})

	if err == nil || ctx.Err() == nil {
		t.Fatalf("expected cancellation error, got %v", err)
	}
}

// Test ReadFile tar extraction
func TestDockerWorker_ReadFile(t *testing.T) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	data := []byte("hello world")
	tw.WriteHeader(&tar.Header{Name: "test.txt", Size: int64(len(data))})
	tw.Write(data)
	tw.Close()

	mock := &MockDockerCli{
		CopyFromContainerFn: func(ctx context.Context, id, path string) (io.ReadCloser, container.PathStat, error) {
			return io.NopCloser(bytes.NewReader(buf.Bytes())), container.PathStat{}, nil
		},
	}

	w := &DockerWorker{dockerCli: mock, containerID: id}
	out, err := w.ReadFile(context.Background(), "/app/tmp/test.txt")
	if err != nil {
		t.Fatal(err)
	}

	if string(out) != "hello world" {
		t.Fatalf("expected 'hello world', got %q", string(out))
	}
}

// Test NewWorker env assembly
func TestNewWorker_EnvAssembly(t *testing.T) {
	var receivedConfig *container.Config

	mock := &MockDockerCli{
		ContainerCreateFn: func(ctx context.Context, config *container.Config, host *container.HostConfig, n interface{}, p interface{}, name string) (container.CreateResponse, error) {
			receivedConfig = config
			return container.CreateResponse{ID: id}, nil
		},
		ContainerStartFn: func(ctx context.Context, id string, opts container.StartOptions) error { return nil },
		CopyToContainerFn: func(ctx context.Context, id, path string, content io.Reader, opts container.CopyToContainerOptions) error {
			return nil
		},
	}

	spec := ty.NodeSpec{
		Alias: "alias",
		Envs: []ty.EnvironmentVariable{
			{Key: "X", Value: "1"},
			{Key: "Y", Value: "2"},
		},
	}

	_, err := NewWorker(context.Background(), mock, "alpine", spec, container.Resources{})
	if err != nil {
		t.Fatal(err)
	}

	if len(receivedConfig.Env) != 2 || receivedConfig.Env[0] != "X=1" || receivedConfig.Env[1] != "Y=2" {
		t.Fatalf("wrong env formatting: %v", receivedConfig.Env)
	}
}
