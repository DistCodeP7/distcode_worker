package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/DistCodeP7/distcode_worker/types"
	"github.com/distcodep7/dsnet/testing/disttest"
	"github.com/docker/docker/api/types/network"
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

	// We expect the error to likely be context.DeadlineExceeded or a wrapped error
	if ctx.Err() != context.DeadlineExceeded {
		t.Errorf("Expected context deadline exceeded, got: %v", ctx.Err())
	}
}

func TestWorker_ExecuteCommand_ExitError(t *testing.T) {
	var buf bytes.Buffer
	// 'false' is a shell command that exits with 1
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

	cli, _ := docker.NewClientWithOpts(docker.FromEnv)
	w, err := NewWorker(testCtx, cli, "alpine:latest", spec)
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

func TestWorker_Network_Operations(t *testing.T) {
	cli, err := docker.NewClientWithOpts(docker.FromEnv)
	if err != nil {
		t.Fatal(err)
	}

	// 1. Create a temporary network
	netName := "test-worker-net-" + strings.ToLower(testWorker.Alias())
	netResp, err := cli.NetworkCreate(testCtx, netName, network.CreateOptions{})
	if err != nil {
		t.Fatalf("Failed to create temp network: %v", err)
	}
	// Ensure network cleanup
	defer func() {
		_ = cli.NetworkRemove(context.Background(), netResp.ID)
	}()

	// 2. Connect the existing testWorker to this network
	netAlias := "worker-net-alias"
	err = testWorker.ConnectToNetwork(testCtx, netName, netAlias)
	if err != nil {
		t.Fatalf("Failed to connect to network: %v", err)
	}

	// 3. Verify connection by inspecting the container
	containerJSON, err := cli.ContainerInspect(testCtx, testWorker.ID())
	if err != nil {
		t.Fatalf("Failed to inspect container: %v", err)
	}

	if _, ok := containerJSON.NetworkSettings.Networks[netName]; !ok {
		t.Errorf("Container is not attached to network %s", netName)
	}

	// 4. Disconnect
	err = testWorker.DisconnectFromNetwork(testCtx, netName)
	if err != nil {
		t.Fatalf("Failed to disconnect from network: %v", err)
	}

	// 5. Verify disconnection
	containerJSON, err = cli.ContainerInspect(testCtx, testWorker.ID())
	if err != nil {
		t.Fatalf("Failed to inspect container: %v", err)
	}
	if _, ok := containerJSON.NetworkSettings.Networks[netName]; ok {
		t.Errorf("Container is still attached to network %s after disconnect", netName)
	}
}
