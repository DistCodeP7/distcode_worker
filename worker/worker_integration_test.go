package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/DistCodeP7/distcode_worker/endpoints/metrics"
	"github.com/DistCodeP7/distcode_worker/log"
	"github.com/DistCodeP7/distcode_worker/types"
	"github.com/distcodep7/dsnet/testing/disttest"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
	docker "github.com/docker/docker/client"
	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

var (
	testWorker *DockerWorker
	testCtx    context.Context
)

func TestMain(m *testing.M) {
	imageName := "ghcr.io/distcodep7/dsnet:latest"
	ctx := context.Background()
	cli, err := docker.NewClientWithOpts(docker.FromEnv, docker.WithVersion("1.48"))
	if err != nil {
		panic(err)
	}

	reader, err := cli.ImagePull(ctx, imageName, image.PullOptions{})
	if err != nil {
		log.Logger.WithField("image", imageName).Error("Failed to pull image")
		panic(err)
	}
	defer reader.Close()
	_, err = io.Copy(io.Discard, reader)
	if err != nil {
		log.Logger.WithError(err).Error("Failed to read pull output")
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
	w, err := NewWorker(ctx, cli, imageName, spec)
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
	byte_tr, err := testWorker.ReadFile(testCtx, "app/tmp/results.json")
	if err != nil {
		t.Fatalf("ReadTestResults failed: %v", err)
	}

	var tr []disttest.TestResult
	if err := json.Unmarshal(byte_tr, &tr); err != nil {
		t.Fatalf("Failed to unmarshal test results: %v", err)
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
	w := &DockerWorker{alias: "alias-test"}
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

func TestWorker_ExecuteCommand_Timeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(testCtx, 500*time.Millisecond)
	defer cancel()

	var buf bytes.Buffer
	err := testWorker.ExecuteCommand(ctx, ExecuteCommandOptions{
		Cmd:          "sleep 2",
		OutputWriter: &buf,
	})

	if err == nil {
		t.Fatal("Expected error due to context timeout, got nil")
	}

	if ctx.Err() != context.DeadlineExceeded {
		t.Errorf("Expected context deadline exceeded, got: %v", ctx.Err())
	}
}

func TestWorker_ExecuteCommand_ExitError(t *testing.T) {
	var buf bytes.Buffer

	err := testWorker.ExecuteCommand(testCtx, ExecuteCommandOptions{
		Cmd:          "false",
		OutputWriter: &buf,
	})

	if err == nil {
		t.Fatal("Expected error for non-zero exit code, got nil")
	}

	expectedPart := "execution finished with non-zero exit code"
	if !strings.Contains(err.Error(), expectedPart) {
		t.Errorf("Expected error message containing %q, got %q", expectedPart, err.Error())
	}
}

func TestWorker_ExecuteCommand_EnvVars(t *testing.T) {
	var buf bytes.Buffer
	err := testWorker.ExecuteCommand(testCtx, ExecuteCommandOptions{
		Cmd: "echo $TEST_RUNTIME_ENV",
		Envs: []types.EnvironmentVariable{
			{Key: "TEST_RUNTIME_ENV", Value: "RuntimeValue123"},
		},
		OutputWriter: &buf,
	})

	if err != nil {
		t.Fatalf("exec failed: %v", err)
	}

	if strings.TrimSpace(buf.String()) != "RuntimeValue123" {
		t.Errorf("Expected output 'RuntimeValue123', got %q", buf.String())
	}
}

func TestNewWorker_Lifecycle_WithScript(t *testing.T) {
	scriptContent := `#!/bin/sh
echo "Hello from script"
echo "My Key is $MY_Start_KEY"
`
	spec := types.NodeSpec{
		Alias: "lifecycle-worker",
		Files: types.FileMap{
			types.Path("run.sh"): types.SourceCode(scriptContent),
		},
		Envs: []types.EnvironmentVariable{
			{Key: "MY_Start_KEY", Value: "StartValue999"},
		},
	}

	cli, err := docker.NewClientWithOpts(
		docker.FromEnv,
		docker.WithVersion("1.48"),
	)
	if err != nil {
		t.Fatal(err)
	}

	w, err := NewWorker(testCtx, cli, "ghcr.io/distcodep7/dsnet:latest", spec)
	if err != nil {
		t.Fatalf("Failed to create worker: %v", err)
	}

	defer w.Stop(context.Background())

	var buf bytes.Buffer
	err = w.ExecuteCommand(testCtx, ExecuteCommandOptions{
		Cmd:          "chmod +x run.sh && ./run.sh",
		OutputWriter: &buf,
	})

	if err != nil {
		t.Fatalf("Execute failed: %v. Output: %s", err, buf.String())
	}

	output := buf.String()

	if !strings.Contains(output, "Hello from script") {
		t.Error("Output did not contain script echo")
	}

	if !strings.Contains(output, "My Key is StartValue999") {
		t.Error("Output did not contain environment variable value")
	}
}

func TestWorker_NetworkManager_Integration(t *testing.T) {
	ctx := context.Background()
	cli, err := docker.NewClientWithOpts(
		docker.FromEnv,
		docker.WithVersion("1.48"),
	)
	if err != nil {
		t.Fatal(err)
	}

	aliasA := "manager-node-a"
	aliasB := "manager-node-b"

	specA := types.NodeSpec{Alias: aliasA}
	workerA, err := NewWorker(ctx, cli, "ghcr.io/distcodep7/dsnet:latest", specA)
	if err != nil {
		t.Fatalf("Failed to create Worker A: %v", err)
	}
	defer workerA.Stop(context.Background())

	specB := types.NodeSpec{Alias: aliasB}
	workerB, err := NewWorker(ctx, cli, "ghcr.io/distcodep7/dsnet:latest", specB)
	if err != nil {
		t.Fatalf("Failed to create Worker B: %v", err)
	}
	defer workerB.Stop(context.Background())

	netManager := NewDockerNetworkManager(cli)

	workers := []Worker{workerA, workerB}
	cleanup, netName, err := netManager.CreateAndConnect(ctx, workers)
	if err != nil {
		t.Fatalf("NetworkManager failed to connect workers: %v", err)
	}

	defer cleanup()

	t.Logf("Network Manager created network: %s", netName)
	t.Logf("Attempting to ping %s from %s...", aliasB, aliasA)

	var bufA bytes.Buffer
	err = workerA.ExecuteCommand(ctx, ExecuteCommandOptions{
		Cmd:          fmt.Sprintf("ping -c 4 %s", aliasB),
		OutputWriter: &bufA,
	})

	if err != nil {
		t.Fatalf("Worker A failed to ping Worker B. Error: %v\nOutput:\n%s", err, bufA.String())
	}

	if !strings.Contains(bufA.String(), "0% packet loss") {
		t.Errorf("Ping output did not indicate success:\n%s", bufA.String())
	}

	t.Logf("Attempting to ping %s from %s...", aliasA, aliasB)

	var bufB bytes.Buffer
	err = workerB.ExecuteCommand(ctx, ExecuteCommandOptions{
		Cmd:          fmt.Sprintf("ping -c 4 %s", aliasA),
		OutputWriter: &bufB,
	})

	if err != nil {
		t.Fatalf("Worker B failed to ping Worker A. Error: %v\nOutput:\n%s", err, bufB.String())
	}
}

func TestIntegration_JobDispatcher_Cancel_Full(t *testing.T) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	require.NoError(t, err)

	workerImage := "ghcr.io/distcodep7/dsnet:latest"
	jobStore := new(MockJobStore)
	metrics := metrics.NewInMemoryMetricsCollector()
	netManager := NewDockerNetworkManager(cli)
	wp := NewDockerWorkerProducer(cli, workerImage)
	wm, err := NewWorkerManager(2, wp)
	require.NoError(t, err)

	jobsCh := make(chan types.Job, 1)
	resultsCh := make(chan types.StreamingJobEvent, 100)
	cancelCh := make(chan types.CancelJobRequest, 1)

	dispatcher := NewJobDispatcher(
		cancelCh,
		jobsCh,
		resultsCh,
		wm,
		netManager,
		jobStore,
		metrics,
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go dispatcher.Run(ctx)
	defer wm.Shutdown()
	defer cli.Close()

	done := make(chan struct{})

	jobStore.On("SaveResult",
		mock.Anything, mock.Anything,
		types.OutcomeCanceled,
		mock.Anything, mock.Anything, mock.Anything, mock.Anything,
	).Run(func(args mock.Arguments) {
		close(done)
	}).Return(nil)

	goModContent := []byte("module worker-test\ngo 1.20\n")
	sleepCode := []byte(`
		package main
		import (
			"fmt"
			"time"
		)
		func main() {
			fmt.Println("Job starting, going to sleep...")
			
			time.Sleep(10 * time.Second) 
			fmt.Println("Job finished naturally (This should not happen)")
		}
	`)

	jobID := uuid.New()
	job := types.Job{
		JobUID:  jobID,
		Timeout: 20,
		TestNode: types.NodeSpec{
			Alias:        "main_node",
			BuildCommand: "go build -o app main.go",
			EntryCommand: "./app",
			Files: types.FileMap{
				"go.mod":  types.SourceCode(goModContent),
				"main.go": types.SourceCode(sleepCode),
			},
		},
		SubmissionNodes: []types.NodeSpec{},
	}

	jobsCh <- job

	time.Sleep(2 * time.Second)

	fmt.Println(">>> Triggering Cancellation...")
	cancelCh <- types.CancelJobRequest{
		JobUID: jobID,
	}

	select {
	case <-done:

	case <-time.After(5 * time.Second):
		t.Fatal("Test timed out waiting for job cancellation to process")
	}

	jobStore.AssertExpectations(t)
}
