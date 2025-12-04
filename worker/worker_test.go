package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/DistCodeP7/distcode_worker/types"
	"github.com/distcodep7/dsnet/testing/disttest"
	docker "github.com/docker/docker/client"
)

var (
	testWorker *Worker
	testCtx    context.Context
)

func TestMain(m *testing.M) {
	ctx := context.Background()
	cli, err := docker.NewClientWithOpts(docker.FromEnv)
	if err != nil {
		panic(err)
	}

	tr := []disttest.TestResult{
		{
			Type:       disttest.TypeSuccess,
			Name:       "sample-test",
			DurationMs: 123,
			Message:    "All good",
		},
	}

	jsonData, err := json.MarshalIndent(tr, "", "  ")
	if err != nil {
		panic(err)
	}
	spec := types.NodeSpec{
		Alias: "test-worker",
		Files: types.FileMap{
			types.Path("results.json"): types.SourceCode(jsonData),
		},
	}
	w, err := NewWorker(ctx, cli, "alpine:latest", spec)
	if err != nil {
		panic(err)
	}

	testWorker = w
	testCtx = ctx

	code := m.Run()

	_ = testWorker.Stop(ctx)
	os.Exit(code)
}

func TestWorkerResults(t *testing.T) {
	tr, err := testWorker.ReadTestResults(testCtx, "app/tmp/results.json")
	if err != nil {
		t.Fatalf("ReadTestResults failed: %v", err)
	}

	tr1 := tr[0]
	if tr1.Type != disttest.TypeSuccess {
		t.Errorf("Expected Type %v, got %v", disttest.TypeSuccess, tr1.Type)
	}
	if tr1.Name != "sample-test" {
		t.Errorf("Expected Name %v, got %v", "sample-test", tr1.Name)
	}
	if tr1.DurationMs != 123 {
		t.Errorf("Expected DurationMs %v, got %v", 123, tr1.DurationMs)
	}
	if tr1.Message != "All good" {
		t.Errorf("Expected Message %v, got %v", "All good", tr1.Message)
	}
}

func TestWorker_Alias(t *testing.T) {
	w := &Worker{alias: "alias-test"}
	if got := w.Alias(); got != "alias-test" {
		t.Errorf("Alias() = %v, want %v", got, "alias-test")
	}
}

func TestWorker_ExecuteCommand(t *testing.T) {
	var buf bytes.Buffer
	err := testWorker.ExecuteCommand(testCtx, ExecuteCommandOptions{
		Cmd:          "echo 123",
		OutputWriter: &buf,
	})
	if err != nil {
		t.Fatalf("exec failed: %v", err)
	}
	if strings.TrimSpace(buf.String()) != "123" {
		t.Fatalf("unexpected output: %s", buf.String())
	}
}
