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
	NetworkInspect(ctx context.Context, networkID string, options network.InspectOptions) (network.Inspect, error)
}

// --- Aggregate Interfaces ---

// WorkerRuntime represents ONLY the methods the Worker struct needs after it is running.
type WorkerRuntime interface {
	ContainerController
	CommandExecutor
	FileReader
}

// Client represents the full capabilities (for Setup/NewWorker).
type Client interface {
	WorkerRuntime
	ContainerProvisioner
	ImageProvisioner
	NetworkConnector
	Close() error
}

// --- Implementation ---

type DockerClient struct {
	*client.Client
}

// Ensure implementation satisfies the Client interface
var _ Client = (*DockerClient)(nil)

func NewClientFromEnv() (*DockerClient, error) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, err
	}
	// Initialize the embedded field
	return &DockerClient{Client: cli}, nil
}

// NewClient wraps an existing raw docker client.
func NewClient(cli *client.Client) *DockerClient {
	return &DockerClient{Client: cli}
}
