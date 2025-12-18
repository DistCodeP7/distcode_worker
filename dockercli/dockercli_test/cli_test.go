package dockercli_test

import (
	"context"
	"testing"

	"github.com/DistCodeP7/distcode_worker/dockercli"
	"github.com/DistCodeP7/distcode_worker/types"
	"github.com/DistCodeP7/distcode_worker/utils"
	"github.com/DistCodeP7/distcode_worker/worker"
	"github.com/docker/docker/api/types/container"
)

func TestCleanupWorkers(t *testing.T) {
	cli, err := dockercli.NewClientFromEnv()
	if err != nil {
		t.Fatalf("Failed to create Docker client: %v", err)
	}
	defer cli.Close()

	spec := types.NodeSpec{
		Alias: "test-worker-cleanup",
		Files: types.FileMap{
			types.Path("main.go"): types.SourceCode(`package main; func main() {}`),
		},
		BuildCommand: "go build -o prog main.go",
		EntryCommand: "./prog",
	}
	worker.NewWorker(t.Context(), cli, "ghcr.io/distcodep7/dsnet:latest", spec, container.Resources{
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
	})
	removedIDs, err := cli.CleanupWorkers(context.Background())

	if err != nil {
		t.Fatalf("Failed to cleanup workers: %v", err)
	}

	if len(removedIDs) == 0 {
		t.Errorf("Expected at least one worker to be removed, got 0")
	}
}
