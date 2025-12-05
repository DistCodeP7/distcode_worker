package worker

import (
	"context"
	"fmt"
	"time"

	"github.com/DistCodeP7/distcode_worker/dockercli"
	"github.com/DistCodeP7/distcode_worker/log"
	"github.com/docker/docker/api/types/network"
	"github.com/google/uuid"
)

type NetworkManager interface {
	CreateAndConnect(ctx context.Context, workers []WorkerInterface) (cleanup func(), networkName string, err error)
}

type DockerNetworkManager struct {
	dockerCli dockercli.NetworkConnector
}

func NewDockerNetworkManager(cli dockercli.NetworkConnector) *DockerNetworkManager {
	return &DockerNetworkManager{dockerCli: cli}
}

// Ensure it satisfies the interface.
var _ NetworkManager = (*DockerNetworkManager)(nil)

func (dnm *DockerNetworkManager) CreateAndConnect(ctx context.Context, workers []WorkerInterface) (func(), string, error) {
	networkName := "job-" + uuid.NewString()
	_, err := dnm.dockerCli.NetworkCreate(ctx, networkName, network.CreateOptions{Driver: "bridge"})
	if err != nil {
		return nil, "", fmt.Errorf("failed to create network %s: %w", networkName, err)
	}
	log.Logger.Tracef("Created network %s", networkName)

	for _, worker := range workers {
		if err := worker.ConnectToNetwork(ctx, networkName, worker.Alias()); err != nil {
			log.Logger.Warnf("Failed to connect worker %s to network %s: %v", worker.ID(), networkName, err)
			_ = dnm.dockerCli.NetworkRemove(ctx, networkName)
			return nil, "", err
		}
	}

	cleanup := func() {
		log.Logger.Tracef("Cleaning up network %s", networkName)
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		for _, worker := range workers {
			if err := worker.DisconnectFromNetwork(cleanupCtx, networkName); err != nil {
				log.Logger.Warnf("Error during worker network disconnect: %v", err)
			}
		}

		if err := dnm.dockerCli.NetworkRemove(cleanupCtx, networkName); err != nil {
			log.Logger.Warnf("Error removing network %s: %v", networkName, err)
		}

	}

	return cleanup, networkName, nil
}
