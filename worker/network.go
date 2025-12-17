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
	CreateAndConnect(ctx context.Context, workers []Worker) (cleanup func(), networkName string, err error)
}

type DockerNetworkManager struct {
	dockerCli dockercli.NetworkConnector
}

func NewDockerNetworkManager(cli dockercli.NetworkConnector) *DockerNetworkManager {
	return &DockerNetworkManager{dockerCli: cli}
}

var _ NetworkManager = (*DockerNetworkManager)(nil)

func (dnm *DockerNetworkManager) CreateAndConnect(
	ctx context.Context,
	workers []Worker,
) (func(), string, error) {

	networkName := "job-" + uuid.NewString()

	_, err := dnm.dockerCli.NetworkCreate(
		ctx,
		networkName,
		network.CreateOptions{Driver: "bridge"},
	)
	if err != nil {
		return nil, "", fmt.Errorf("failed to create network %s: %w", networkName, err)
	}

	log.Logger.Tracef("Created network %s", networkName)

	for _, worker := range workers {
		if err := dnm.dockerCli.NetworkConnect(
			ctx,
			networkName,
			worker.ID(),
			&network.EndpointSettings{
				Aliases: []string{worker.Alias()},
			},
		); err != nil {

			log.Logger.Warnf(
				"Failed to connect worker %s to network %s: %v",
				worker.ID(),
				networkName,
				err,
			)

			_ = dnm.dockerCli.NetworkRemove(ctx, networkName)
			return nil, "", err
		}
	}

	cleanup := func() {
		log.Logger.Tracef("Cleaning up network %s", networkName)

		cleanupCtx, cancel := context.WithTimeout(context.Background(), 1*time.Hour)
		defer cancel()

		// Best-effort disconnect
		for _, worker := range workers {
			if err := dnm.dockerCli.NetworkDisconnect(
				cleanupCtx,
				networkName,
				worker.ID(),
				true,
			); err != nil {
				log.Logger.Debugf(
					"Network disconnect failed (%s / %s): %v",
					networkName,
					worker.ID(),
					err,
				)
			}
		}

		// Wait until Docker reports no active endpoints
		for {
			net, err := dnm.dockerCli.NetworkInspect(cleanupCtx, networkName, network.InspectOptions{})
			if err != nil {
				log.Logger.Warnf(
					"Failed to inspect network %s during cleanup: %v",
					networkName,
					err,
				)
				return
			}

			if len(net.Containers) == 0 {
				break
			}

			select {
			case <-cleanupCtx.Done():
				log.Logger.Warnf(
					"Timeout waiting for network %s endpoints to detach",
					networkName,
				)
				return
			case <-time.After(200 * time.Millisecond):
			}
		}

		// Retry removal
		for i := 0; i < 5; i++ {
			if err := dnm.dockerCli.NetworkRemove(cleanupCtx, networkName); err == nil {
				log.Logger.Tracef("Removed network %s", networkName)
				return
			}

			select {
			case <-cleanupCtx.Done():
				log.Logger.Warnf(
					"Timeout removing network %s",
					networkName,
				)
				return
			case <-time.After(500 * time.Millisecond):
			}
		}

		log.Logger.Warnf("Failed to remove network %s after retries", networkName)
	}

	return cleanup, networkName, nil
}
