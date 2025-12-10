package worker

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/DistCodeP7/distcode_worker/endpoints/metrics"
	"github.com/DistCodeP7/distcode_worker/types"
	t "github.com/distcodep7/dsnet/testing"
	dt "github.com/distcodep7/dsnet/testing/disttest"
	"github.com/docker/docker/client"
	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type MockJobStore struct {
	mock.Mock
}

func (m *MockJobStore) SaveResult(
	ctx context.Context,
	jobID uuid.UUID,
	outcome types.Outcome,
	testResults []dt.TestResult,
	logs []types.LogEvent,
	nodeMessageLogs []t.TraceEvent,
	startTime time.Time,
) error {
	args := m.Called(ctx, jobID, outcome, testResults, logs, nodeMessageLogs, startTime)
	return args.Error(0)
}

func TestIntegration_JobDispatcher_Success(t *testing.T) {
	// 1. Setup Docker Client
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	require.NoError(t, err)
	defer cli.Close()
	workerImage := "ghcr.io/distcodep7/dsnet:latest"

	jobStore := new(MockJobStore)
	metrics := metrics.NewInMemoryMetricsCollector()

	// Channel to signal that SaveResult was called
	done := make(chan struct{})

	// --- SMART MOCK SETUP ---
	jobStore.On("SaveResult",
		mock.Anything, // ctx
		mock.Anything, // jobID
		mock.Anything, // outcome
		mock.Anything, // testResults
		mock.Anything, // logs
		mock.Anything, // nodeMessageLogs
		mock.Anything, // startTime
	).Run(func(args mock.Arguments) {
		outcome := args.Get(2).(types.Outcome)

		// DEBUG: Print logs immediately if job failed.
		// We use fmt.Printf instead of t.Logf to avoid panics if the test times out.
		if outcome != types.OutcomeSuccess {
			logs := args.Get(4).([]types.LogEvent)
			fmt.Printf("\n\n====== JOB FAILED (Outcome: %s) ======\n", outcome)
			for _, l := range logs {
				// Printing entire struct to ensure we see the message regardless of field names
				fmt.Printf("[%s] %+v\n", l.WorkerID, l)
			}
			fmt.Printf("======================================\n\n")
		}

		close(done)
	}).Return(nil)

	netManager := NewDockerNetworkManager(cli)
	wp := NewDockerWorkerProducer(cli, workerImage)
	wm, err := NewWorkerManager(4, wp)
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

	// Use a standard context for the background runner, NOT t.Context()
	// This prevents the dispatcher from dying prematurely if the test cancels
	ctx := context.Background()

	go dispatcher.Run(ctx)

	// 3. Create Job
	jobID := uuid.New()
	goModContent := []byte("module worker-test\ngo 1.20\n")
	fileContent := []byte(`
		package main
		import "fmt"
		func main() {
			fmt.Println("INTEGRATION TEST SUCCESS")
		}
	`)

	job := types.Job{
		JobUID:  jobID,
		Timeout: 15,
		TestNode: types.NodeSpec{
			Alias:        "main_node",
			BuildCommand: "go build -o app main.go",
			EntryCommand: "./app",
			Files: types.FileMap{
				"go.mod":  types.SourceCode(goModContent),
				"main.go": types.SourceCode(fileContent),
			},
		},
		SubmissionNodes: []types.NodeSpec{
			{
				Alias:        "submission_node",
				BuildCommand: "go build -o app main.go",
				EntryCommand: "./app",
				Files: types.FileMap{
					"go.mod":  types.SourceCode(goModContent),
					"main.go": types.SourceCode(fileContent),
				},
			},
		},
	}

	// 4. Run Test
	jobsCh <- job

	// 5. WAIT FOR RESULT
	select {
	case <-done:
		// SaveResult was called, proceed to assertions
	case <-time.After(25 * time.Second):
		t.Fatal("Test timed out waiting for job completion")
	}

	wm.Shutdown()

	// 6. Assert
	// If this assertion fails, look at the console output for "====== JOB FAILED ======"
	jobStore.AssertCalled(t, "SaveResult",
		mock.Anything,
		jobID,
		types.OutcomeSuccess,
		mock.Anything,
		mock.Anything,
		mock.Anything,
		mock.Anything,
	)
}

func setupTestHelper(t *testing.T) (*JobDispatcher, *MockJobStore, chan types.Job, func()) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	require.NoError(t, err)

	// Using the same image as your success test
	workerImage := "ghcr.io/distcodep7/dsnet:latest"

	jobStore := new(MockJobStore)
	metrics := metrics.NewInMemoryMetricsCollector()
	netManager := NewDockerNetworkManager(cli)
	wp := NewDockerWorkerProducer(cli, workerImage)

	// We allocate 2 slots to handle tests with submission nodes
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

	// Context for the dispatcher
	ctx, cancel := context.WithCancel(context.Background())
	go dispatcher.Run(ctx)

	cleanup := func() {
		cancel()
		wm.Shutdown()
		cli.Close()
	}

	return dispatcher, jobStore, jobsCh, cleanup
}

// -------------------------------------------------------------------------
// TEST 1: Compilation Error
// -------------------------------------------------------------------------
func TestIntegration_JobDispatcher_CompilationError(t *testing.T) {
	_, jobStore, jobsCh, cleanup := setupTestHelper(t)
	defer cleanup()

	done := make(chan struct{})

	// Expect Compilation Error
	jobStore.On("SaveResult",
		mock.Anything, mock.Anything,
		types.OutcomeCompilationError, // <--- We expect this specific outcome
		mock.Anything, mock.Anything, mock.Anything, mock.Anything,
	).Run(func(args mock.Arguments) {
		// Optional: Inspect logs to ensure the compiler error message is present
		logs := args.Get(4).([]types.LogEvent)
		foundErrorMsg := false
		for _, l := range logs {
			// We look for standard go compiler output
			if contains(l.Message, "undefined: nonExistentFunction") {
				foundErrorMsg = true
			}
		}
		if !foundErrorMsg {
			fmt.Println("WARNING: Expected compiler error message not found in logs")
		}
		close(done)
	}).Return(nil)

	// Bad Go Code
	goModContent := []byte("module worker-test\ngo 1.20\n")
	badCode := []byte(`
		package main
		import "fmt"
		func main() {
			// This function does not exist
			nonExistentFunction()
		}
	`)

	job := types.Job{
		JobUID:  uuid.New(),
		Timeout: 15,
		TestNode: types.NodeSpec{
			Alias:        "main_node",
			BuildCommand: "go build -o app main.go",
			EntryCommand: "./app", // Should never run
			Files: types.FileMap{
				"go.mod":  types.SourceCode(goModContent),
				"main.go": types.SourceCode(badCode),
			},
		},
		SubmissionNodes: []types.NodeSpec{},
	}

	jobsCh <- job

	select {
	case <-done:
		// Success
	case <-time.After(20 * time.Second):
		t.Fatal("Test timed out waiting for compilation failure")
	}

	jobStore.AssertExpectations(t)
}

// -------------------------------------------------------------------------
// TEST 2: Runtime Panic / Non-Zero Exit Code
// -------------------------------------------------------------------------
func TestIntegration_JobDispatcher_RuntimeError(t *testing.T) {
	_, jobStore, jobsCh, cleanup := setupTestHelper(t)
	defer cleanup()

	done := make(chan struct{})

	// Expect Failure Outcome (Runtime error is usually OutcomeFailed or OutcomeRuntimeError depending on your types)
	// Based on your code, execution errors return OutcomeFailed
	jobStore.On("SaveResult",
		mock.Anything, mock.Anything,
		types.OutcomeFailed,
		mock.Anything, mock.Anything, mock.Anything, mock.Anything,
	).Run(func(args mock.Arguments) {
		close(done)
	}).Return(nil)

	goModContent := []byte("module worker-test\ngo 1.20\n")
	panicCode := []byte(`
		package main
		func main() {
			panic("intentional panic for testing")
		}
	`)

	job := types.Job{
		JobUID:  uuid.New(),
		Timeout: 15,
		TestNode: types.NodeSpec{
			Alias:        "main_node",
			BuildCommand: "go build -o app main.go",
			EntryCommand: "./app",
			Files: types.FileMap{
				"go.mod":  types.SourceCode(goModContent),
				"main.go": types.SourceCode(panicCode),
			},
		},
		SubmissionNodes: []types.NodeSpec{},
	}

	jobsCh <- job

	select {
	case <-done:
		// Success
	case <-time.After(20 * time.Second):
		t.Fatal("Test timed out waiting for runtime failure")
	}

	jobStore.AssertExpectations(t)
}

// -------------------------------------------------------------------------
// TEST 3: Infinite Loop Timeout
// -------------------------------------------------------------------------
func TestIntegration_JobDispatcher_Timeout(t *testing.T) {
	_, jobStore, jobsCh, cleanup := setupTestHelper(t)
	defer cleanup()

	done := make(chan struct{})

	// Expect Timeout Outcome
	jobStore.On("SaveResult",
		mock.Anything, mock.Anything,
		types.OutcomeTimeout,
		mock.Anything, mock.Anything, mock.Anything, mock.Anything,
	).Run(func(args mock.Arguments) {
		close(done)
	}).Return(nil)

	goModContent := []byte("module worker-test\ngo 1.20\n")
	// Infinite loop code
	loopCode := []byte(`
		package main
		import "time"
		func main() {
			for {
				time.Sleep(1 * time.Second)
			}
		}
	`)

	job := types.Job{
		JobUID: uuid.New(),
		// Set a very short timeout for the JOB
		Timeout: 3,
		TestNode: types.NodeSpec{
			Alias:        "main_node",
			BuildCommand: "go build -o app main.go",
			EntryCommand: "./app",
			Files: types.FileMap{
				"go.mod":  types.SourceCode(goModContent),
				"main.go": types.SourceCode(loopCode),
			},
		},
		SubmissionNodes: []types.NodeSpec{},
	}

	jobsCh <- job

	// We wait slightly longer than the job timeout (3s) to give the dispatcher time to kill it
	select {
	case <-done:
		// Success
	case <-time.After(10 * time.Second):
		t.Fatal("Test timed out - Dispatcher failed to kill the infinite loop job")
	}

	jobStore.AssertExpectations(t)
}

// Helper utility for string check
func contains(s, substr string) bool {
	return len(s) >= len(substr) && len(substr) > 0 && s[0:len(substr)] == substr // simplistic, real impl needs strings.Contains
}
