package worker

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/google/uuid"
)

type NetworkManager interface {
	CreateAndConnect(ctx context.Context, workers []WorkerInterface) (func(), error)
}

type DockerNetworkManager struct {
	dockerCli *client.Client
}

func NewDockerNetworkManager(cli *client.Client) *DockerNetworkManager {
	return &DockerNetworkManager{dockerCli: cli}
}

// Ensure it satisfies the interface.
var _ NetworkManager = (*DockerNetworkManager)(nil)

func (dnm *DockerNetworkManager) CreateAndConnect(ctx context.Context, workers []WorkerInterface) (func(), error) {
	networkName := "job-" + uuid.NewString()
	_, err := dnm.dockerCli.NetworkCreate(ctx, networkName, network.CreateOptions{Driver: "bridge"})
	if err != nil {
		return nil, fmt.Errorf("failed to create network %s: %w", networkName, err)
	}
	log.Printf("Created network %s", networkName)

	for i, worker := range workers {
		alias := fmt.Sprintf("worker-%d", i)
		if err := worker.ConnectToNetwork(ctx, networkName, alias); err != nil {
			log.Printf("Failed to connect worker %s to network %s: %v", worker.ID(), networkName, err)
			_ = dnm.dockerCli.NetworkRemove(ctx, networkName)
			return nil, err
		}
	}

	cleanup := func() {
		log.Printf("Cleaning up network %s", networkName)
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		for _, worker := range workers {
			if err := worker.DisconnectFromNetwork(cleanupCtx, networkName); err != nil {
				log.Printf("Error during worker network disconnect: %v", err)
			}
		}

		if err := dnm.dockerCli.NetworkRemove(cleanupCtx, networkName); err != nil {
			log.Printf("Error removing network %s: %v", networkName, err)
		}

	}

	return cleanup, nil
}
