package dockercli

import (
	"context"
	"io"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

// --- 1. Provisioning Interfaces (Used only during creation) ---

type ContainerProvisioner interface {
	ContainerCreate(ctx context.Context, config *container.Config, hostConfig *container.HostConfig, networkingConfig *network.NetworkingConfig, platform *v1.Platform, containerName string) (container.CreateResponse, error)
	ContainerStart(ctx context.Context, containerID string, options container.StartOptions) error
	CopyToContainer(ctx context.Context, containerID, path string, content io.Reader, options container.CopyToContainerOptions) error
}

type ImageProvisioner interface {
	ImageList(ctx context.Context, options image.ListOptions) ([]image.Summary, error)
	ImagePull(ctx context.Context, ref string, options image.PullOptions) (io.ReadCloser, error)
}

// --- 2. Runtime Interfaces (Used by the active Worker) ---

type ContainerController interface {
	ContainerStop(ctx context.Context, containerID string, options container.StopOptions) error
	ContainerRemove(ctx context.Context, containerID string, options container.RemoveOptions) error
}

type CommandExecutor interface {
	ContainerExecCreate(ctx context.Context, container string, config container.ExecOptions) (types.IDResponse, error)
	ContainerExecAttach(ctx context.Context, execID string, config container.ExecStartOptions) (types.HijackedResponse, error)
	ContainerExecStart(ctx context.Context, execID string, config container.ExecStartOptions) error
	ContainerExecInspect(ctx context.Context, execID string) (container.ExecInspect, error)
}

type FileReader interface {
	CopyFromContainer(ctx context.Context, containerID, srcPath string) (io.ReadCloser, container.PathStat, error)
}

type NetworkConnector interface {
	NetworkCreate(ctx context.Context, name string, options network.CreateOptions) (network.CreateResponse, error)
	NetworkRemove(ctx context.Context, networkID string) error
	NetworkConnect(ctx context.Context, networkID, containerID string, config *network.EndpointSettings) error
	NetworkDisconnect(ctx context.Context, networkID, containerID string, force bool) error
}

// --- Aggregate Interfaces ---

// WorkerRuntime represents ONLY the methods the Worker struct needs after it is running.
type WorkerRuntime interface {
	ContainerController
	CommandExecutor
	FileReader
}

// FullClient represents the full capabilities (for Setup/NewWorker).
type Client interface {
	WorkerRuntime
	ContainerProvisioner
	ImageProvisioner
	NetworkConnector
	Close() error
}

// --- Implementation ---

type DockerClient struct {
	cli *client.Client
}

// Ensure implementation satisfies the FullClient interface
var _ Client = (*DockerClient)(nil)

func NewClientFromEnv() (*DockerClient, error) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, err
	}
	return &DockerClient{cli: cli}, nil
}

// NewClient wraps an existing raw docker client.
func NewClient(cli *client.Client) *DockerClient {
	return &DockerClient{cli: cli}
}

// --- ContainerAPI Implementation ---

func (d *DockerClient) ContainerCreate(ctx context.Context, config *container.Config, hostConfig *container.HostConfig, networkingConfig *network.NetworkingConfig, platform *v1.Platform, containerName string) (container.CreateResponse, error) {
	return d.cli.ContainerCreate(ctx, config, hostConfig, networkingConfig, platform, containerName)
}

func (d *DockerClient) ContainerStart(ctx context.Context, containerID string, options container.StartOptions) error {
	return d.cli.ContainerStart(ctx, containerID, options)
}

func (d *DockerClient) ContainerStop(ctx context.Context, containerID string, options container.StopOptions) error {
	return d.cli.ContainerStop(ctx, containerID, options)
}

func (d *DockerClient) ContainerRemove(ctx context.Context, containerID string, options container.RemoveOptions) error {
	return d.cli.ContainerRemove(ctx, containerID, options)
}

// --- ExecAPI Implementation ---

func (d *DockerClient) ContainerExecCreate(ctx context.Context, container string, config container.ExecOptions) (types.IDResponse, error) {
	return d.cli.ContainerExecCreate(ctx, container, config)
}

func (d *DockerClient) ContainerExecAttach(ctx context.Context, execID string, config container.ExecStartOptions) (types.HijackedResponse, error) {
	return d.cli.ContainerExecAttach(ctx, execID, config)
}

func (d *DockerClient) ContainerExecStart(ctx context.Context, execID string, config container.ExecStartOptions) error {
	return d.cli.ContainerExecStart(ctx, execID, config)
}

func (d *DockerClient) ContainerExecInspect(ctx context.Context, execID string) (container.ExecInspect, error) {
	return d.cli.ContainerExecInspect(ctx, execID)
}

// --- FileSystemAPI Implementation ---

func (d *DockerClient) CopyToContainer(ctx context.Context, containerID, path string, content io.Reader, options container.CopyToContainerOptions) error {
	return d.cli.CopyToContainer(ctx, containerID, path, content, options)
}

func (d *DockerClient) CopyFromContainer(ctx context.Context, containerID, srcPath string) (io.ReadCloser, container.PathStat, error) {
	return d.cli.CopyFromContainer(ctx, containerID, srcPath)
}

// --- NetworkAPI Implementation ---

func (d *DockerClient) NetworkCreate(ctx context.Context, name string, options network.CreateOptions) (network.CreateResponse, error) {
	return d.cli.NetworkCreate(ctx, name, options)
}

func (d *DockerClient) NetworkRemove(ctx context.Context, networkID string) error {
	return d.cli.NetworkRemove(ctx, networkID)
}

func (d *DockerClient) NetworkConnect(ctx context.Context, networkID, containerID string, config *network.EndpointSettings) error {
	return d.cli.NetworkConnect(ctx, networkID, containerID, config)
}

func (d *DockerClient) NetworkDisconnect(ctx context.Context, networkID, containerID string, force bool) error {
	return d.cli.NetworkDisconnect(ctx, networkID, containerID, force)
}

// --- ImageAPI Implementation ---

func (d *DockerClient) ImageList(ctx context.Context, options image.ListOptions) ([]image.Summary, error) {
	return d.cli.ImageList(ctx, options)
}

func (d *DockerClient) ImagePull(ctx context.Context, ref string, options image.PullOptions) (io.ReadCloser, error) {
	return d.cli.ImagePull(ctx, ref, options)
}

func (d *DockerClient) Close() error {
	return d.cli.Close()
}
