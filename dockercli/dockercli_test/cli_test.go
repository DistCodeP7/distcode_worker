package dockercli_test

import (
	"context"
	"testing"

	"github.com/DistCodeP7/distcode_worker/dockercli"
	"github.com/DistCodeP7/distcode_worker/types"
	"github.com/DistCodeP7/distcode_worker/worker"
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
	worker.NewWorker(t.Context(), cli, "ghcr.io/distcodep7/dsnet:latest", spec)
	removedIDs, err := cli.CleanupWorkers(context.Background())

	if err != nil {
		t.Fatalf("Failed to cleanup workers: %v", err)
	}

	if len(removedIDs) == 0 {
		t.Errorf("Expected at least one worker to be removed, got 0")
	}
}
