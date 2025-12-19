package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DistCodeP7/distcode_worker/dockercli"
	"github.com/DistCodeP7/distcode_worker/log"
	"github.com/DistCodeP7/distcode_worker/types"
	"github.com/distcodep7/dsnet/testing/disttest"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	docker "github.com/docker/docker/client"
)

var (
	testWorker *DockerWorker
	testCtx    context.Context
)

func TestMain(m *testing.M) {
	imageName := "ghcr.io/distcodep7/dsnet:latest"
	ctx := context.Background()
	cli, err := dockercli.NewClientFromEnv("1.48")
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
	w, err := NewWorker(ctx, cli, imageName, spec, container.Resources{})
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

	if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
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

	w, err := NewWorker(testCtx, cli, "ghcr.io/distcodep7/dsnet:latest", spec, container.Resources{})
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
	specB := types.NodeSpec{Alias: aliasB}

	var workerA, workerB Worker
	var errA, errB error
	var wg sync.WaitGroup

	wg.Go(func() {
		workerA, errA = NewWorker(ctx, cli, "ghcr.io/distcodep7/dsnet:latest", specA, container.Resources{})
	})

	wg.Go(func() {
		workerB, errB = NewWorker(ctx, cli, "ghcr.io/distcodep7/dsnet:latest", specB, container.Resources{})
	})

	wg.Wait()

	if errA != nil {
		t.Fatalf("Failed to create Worker A: %v", errA)
	}

	if errB != nil {
		t.Fatalf("Failed to create Worker B: %v", errB)
	}
	defer workerB.Stop(context.Background())
	defer workerA.Stop(context.Background())

	netManager := NewDockerNetworkManager(cli)
	workers := []Worker{workerA, workerB}

	cleanup, netName, err := netManager.CreateAndConnect(ctx, workers)
	if err != nil {
		t.Fatalf("NetworkManager failed to connect workers: %v", err)
	}
	defer cleanup()

	t.Logf("Network Manager created network: %s", netName)

	var bufA bytes.Buffer
	var bufB bytes.Buffer
	var err1 error
	var err2 error

	// Ping from A to B
	wg.Go(func() {
		err1 = workerA.ExecuteCommand(ctx, ExecuteCommandOptions{
			Cmd:          fmt.Sprintf("timeout 0.2 ping -c 1 %s", aliasB),
			OutputWriter: &bufA,
		})
	})

	// Ping from B to A
	wg.Go(func() {
		err2 = workerB.ExecuteCommand(ctx, ExecuteCommandOptions{
			Cmd:          fmt.Sprintf("timeout 0.2 ping -c 1 %s", aliasA),
			OutputWriter: &bufB,
		})
	})

	wg.Wait()

	if !strings.Contains(bufA.String(), "0% packet loss") {
		t.Errorf("Ping output did not indicate success:\n%s", bufA.String())
	}
	if err1 != nil {
		t.Fatalf("Worker A failed to ping Worker B. Error: %v\nOutput:\n%s", err1, bufA.String())
	}

	if err2 != nil {
		t.Fatalf("Worker B failed to ping Worker A. Error: %v\nOutput:\n%s", err2, bufB.String())
	}
}

func TestWorker_EnforcesMemoryLimit(t *testing.T) {
	oomCode := `
    package main
    import (
        "fmt"
        "time"
    )
    func main() {
        const size = 50 * 1024 * 1024
        buf := make([]byte, size)
        for i := 0; i < size; i += 4096 {
            buf[i] = 1
        }
        time.Sleep(1 * time.Second)
    }
    `

	spec := types.NodeSpec{
		Alias: "oom-test-worker",
		Files: types.FileMap{
			types.Path("main.go"): types.SourceCode(oomCode),
		},
	}

	cli := testWorker.dockerCli
	w, err := NewWorker(testCtx, cli.(NewWorkerCli), "ghcr.io/distcodep7/dsnet:latest", spec, container.Resources{
		CPUShares:  512,
		NanoCPUs:   1_000_000_000,
		Memory:     50 * 1024 * 1024,
		MemorySwap: 50 * 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("Failed to create worker: %v", err)
	}
	defer w.Stop(context.Background())

	var buf bytes.Buffer
	err = w.ExecuteCommand(testCtx, ExecuteCommandOptions{
		Cmd:          "go run main.go",
		OutputWriter: &buf,
	})

	if err == nil {
		t.Logf("Output: %s", buf.String())
		t.Fatal("Expected error due to Memory Limit Exceeded (OOM), but command succeeded.")
	}

	expectedErrorFragment := "non-zero exit code"
	if !strings.Contains(err.Error(), expectedErrorFragment) {
		t.Errorf("Expected error to mention non-zero exit code (OOM), got: %v", err)
	}
}

func TestWorker_EnforcesPIDLimit(t *testing.T) {

	forkBombCode := `
    package main
    import (
        "fmt"
        "runtime"
        "sync"
        "time"
    )
    func main() {
        var wg sync.WaitGroup
        for i := 0; i < 31; i++ {
            wg.Add(1)
            go func() {
                defer wg.Done()
                runtime.LockOSThread() 
                time.Sleep(10 * time.Second)
            }()
        }
        wg.Wait()
    }
    `

	spec := types.NodeSpec{
		Alias: "pid-test-worker",
		Files: types.FileMap{"main.go": types.SourceCode(forkBombCode)},
	}

	cli := testWorker.dockerCli
	w, err := NewWorker(testCtx, cli.(NewWorkerCli), "ghcr.io/distcodep7/dsnet:latest", spec, container.Resources{
		Ulimits: []*container.Ulimit{
			{Name: "cpu", Soft: 30, Hard: 30},
			{Name: "nofile", Soft: 30, Hard: 30},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create worker: %v", err)
	}
	defer w.Stop(context.Background())

	var buf bytes.Buffer
	err = w.ExecuteCommand(testCtx, ExecuteCommandOptions{
		Cmd:          "go run main.go",
		OutputWriter: &buf,
	})

	if err == nil {
		t.Fatal("Expected error due to PID limit, but command succeeded.")
	}

	t.Logf("Caught PID limit error: %v", err)
}
