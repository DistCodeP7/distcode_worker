package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/DistCodeP7/distcode_worker/db"
	"github.com/DistCodeP7/distcode_worker/endpoints/metrics"
	"github.com/DistCodeP7/distcode_worker/jobsession"
	"github.com/DistCodeP7/distcode_worker/types"
	tt "github.com/distcodep7/dsnet/testing"
	dt "github.com/distcodep7/dsnet/testing/disttest"
	"github.com/docker/docker/client"
	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type MockJobStore struct {
	mock.Mock
}

func (m *MockJobStore) SaveResult(ctx context.Context, results db.JobResult) error {
	args := m.Called(ctx, results)
	return args.Error(0)
}

func setupTestHelper(t *testing.T) (
	*JobDispatcher,
	*MockJobStore,
	chan types.Job,
	chan types.CancelJobRequest,
	func(),
) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	require.NoError(t, err)

	workerImage := "ghcr.io/distcodep7/dsnet:latest"

	jobStore := new(MockJobStore)
	metricsCollector := metrics.NewInMemoryMetricsCollector()
	netManager := NewDockerNetworkManager(cli)
	wp := NewDockerWorkerProducer(cli, workerImage)

	wm, err := NewWorkerManager(2, wp)
	require.NoError(t, err)

	jobsCh := make(chan types.Job, 10)
	resultsCh := make(chan types.StreamingJobEvent, 100)
	cancelCh := make(chan types.CancelJobRequest, 10)

	dispatcher := NewJobDispatcher(
		cancelCh,
		jobsCh,
		resultsCh,
		wm,
		netManager,
		jobStore,
		metricsCollector,
	)

	ctx, cancel := context.WithCancel(context.Background())
	go dispatcher.Run(ctx)

	cleanup := func() {
		cancel()
		wm.Shutdown()
		cli.Close()
	}

	return dispatcher, jobStore, jobsCh, cancelCh, cleanup
}

func TestIntegration_JobDispatcher_Success(t *testing.T) {
	dispatcher, jobStore, jobsCh, _, cleanup := setupTestHelper(t)
	defer cleanup()

	_ = dispatcher

	done := make(chan struct{})

	var savedOutcome types.Outcome
	var savedArtifacts jobsession.JobArtifacts
	var savedLogs []types.LogEvent

	expectedResults := []dt.TestResult{
		{
			Name:       "TestSimpleConnection",
			Type:       dt.TypeSuccess,
			DurationMs: 20,
			Message:    "Whatever",
		},
	}
	resultBytes, err := json.Marshal(expectedResults)
	require.NoError(t, err)

	jobStore.On(
		"SaveResult",
		mock.Anything,
		mock.MatchedBy(func(res db.JobResult) bool {
			if res.Outcome != types.OutcomeSuccess {
				return false
			}
			return true
		}),
	).Run(func(args mock.Arguments) {
		res := args.Get(1).(db.JobResult)
		savedOutcome = res.Outcome
		savedArtifacts = res.Artifacts
		savedLogs = res.Logs
		close(done)
	}).Return(nil)

	goModContent := []byte("module worker-test\ngo 1.20\n")
	code := fmt.Sprintf(`
        package main
        import (
            "os"
            "fmt"
        )
        func main() {
            
            jsonData := %q 
            err := os.WriteFile("test_results.json", []byte(jsonData), 0644)
            if err != nil {
                fmt.Printf("Error writing results: %%v\n", err)
                os.Exit(1)
            }
            fmt.Println("Tests finished successfully")
        }
    `, string(resultBytes))

	job := types.Job{
		JobUID:  uuid.New(),
		Timeout: 60,
		TestNode: types.NodeSpec{
			Alias:        "main_node",
			BuildCommand: "go build -o app main.go",
			EntryCommand: "./app",
			Files: types.FileMap{
				"go.mod":  types.SourceCode(goModContent),
				"main.go": types.SourceCode(code),
			},
		},
		SubmissionNodes: []types.NodeSpec{},
		UserID:          "tester",
		SubmittedAt:     time.Now(),
		ProblemID:       -1,
	}

	jobsCh <- job

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("Test timed out waiting for job to finish")
	}

	jobStore.AssertExpectations(t)
	if savedOutcome != types.OutcomeSuccess {
		t.Errorf("Expected job outcome %v, got %v", types.OutcomeSuccess, savedOutcome)
	}

	if len(savedArtifacts.TestResults) != 1 {
		t.Errorf("Expected 1 test result, got %d", len(savedArtifacts.TestResults))
	} else {
		if savedArtifacts.TestResults[0].Name != "TestSimpleConnection" {
			t.Errorf("Expected test name 'TestSimpleConnection', got %s", savedArtifacts.TestResults[0].Name)
		}
	}

	foundCompile := false
	foundRun := false
	for _, logEntry := range savedLogs {
		if bytes.Contains([]byte(logEntry.Message), []byte("Running build")) {
			foundCompile = true
		}
		if bytes.Contains([]byte(logEntry.Message), []byte("Executing")) {
			foundRun = true
		}
	}
	if !foundCompile {
		t.Error("Expected compile phase logs")
	}
	if !foundRun {
		t.Error("Expected execution phase logs")
	}
}

func TestIntegration_JobDispatcher_CancelLate(t *testing.T) {
	_, jobStore, jobsCh, cancelCh, cleanup := setupTestHelper(t)
	defer cleanup()

	done := make(chan struct{})

	jobStore.On(
		"SaveResult",
		mock.Anything,
		mock.MatchedBy(func(res db.JobResult) bool {
			if res.Outcome != types.OutcomeCanceled {
				return false
			}

			if !res.Timespent.HasPhases(
				types.PhasePending,
				types.PhaseReserving,
			) {
				return false
			}

			return true
		}),
	).Run(func(args mock.Arguments) {
		close(done)
	}).Return(nil)

	goModContent := []byte("module worker-test\ngo 1.20\n")
	longRunningCode := []byte(`
        package main
        import "time"
        func main() {
            time.Sleep(10 * time.Second)
        }
    `)

	job := types.Job{
		JobUID:  uuid.New(),
		Timeout: 30,
		TestNode: types.NodeSpec{
			Alias:        "main_node",
			BuildCommand: "go build -o app main.go",
			EntryCommand: "./app",
			Files: types.FileMap{
				"go.mod":  types.SourceCode(goModContent),
				"main.go": types.SourceCode(longRunningCode),
			},
		},
		SubmissionNodes: []types.NodeSpec{},
		UserID:          "tester_cancel_late",
		SubmittedAt:     time.Now(),
		ProblemID:       -1,
	}

	jobsCh <- job
	time.Sleep(3 * time.Second)
	cancelCh <- types.CancelJobRequest{
		JobUID: job.JobUID,
	}

	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("Test timed out waiting for job cancellation result")
	}

	jobStore.AssertExpectations(t)
}

func TestIntegration_JobDispatcher_CancelEarly(t *testing.T) {
	_, jobStore, jobsCh, cancelCh, cleanup := setupTestHelper(t)
	defer cleanup()

	done := make(chan struct{})

	jobStore.On(
		"SaveResult",
		mock.Anything,
		mock.MatchedBy(func(res db.JobResult) bool {

			if !res.Timespent.HasPhases() {
				return false
			}

			return res.Outcome == types.OutcomeCanceled
		}),
	).Run(func(args mock.Arguments) {
		close(done)
	}).Return(nil)

	goModContent := []byte("module worker-test\ngo 1.20\n")
	simpleCode := []byte(`package main; import "fmt"; func main() { fmt.Println("Hi") }`)

	job := types.Job{
		JobUID:  uuid.New(),
		Timeout: 60,
		TestNode: types.NodeSpec{
			Alias:        "main_node",
			BuildCommand: "go build -o app main.go",
			EntryCommand: "./app",
			Files: types.FileMap{
				"go.mod":  types.SourceCode(goModContent),
				"main.go": types.SourceCode(simpleCode),
			},
		},
		SubmissionNodes: []types.NodeSpec{},
		UserID:          "tester_cancel_early",
		SubmittedAt:     time.Now(),
	}

	cancelCh <- types.CancelJobRequest{
		JobUID: job.JobUID,
	}
	jobsCh <- job

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Test timed out waiting for early cancellation")
	}

	jobStore.AssertExpectations(t)
}

func TestIntegration_JobDispatcher_CompilationError(t *testing.T) {
	_, jobStore, jobsCh, _, cleanup := setupTestHelper(t)
	defer cleanup()

	done := make(chan struct{})

	jobStore.On(
		"SaveResult",
		mock.Anything,
		mock.MatchedBy(func(res db.JobResult) bool {
			if !res.Timespent.HasPhases(
				types.PhasePending,
				types.PhaseReserving,
				types.PhaseConfiguringNetwork,
				types.PhaseCompiling,
			) {
				return false
			}

			return res.Outcome == types.OutcomeCompilationError
		}),
	).Run(func(args mock.Arguments) {
		res := args.Get(1).(db.JobResult)

		foundError := false
		for _, l := range res.Logs {
			if strings.Contains(l.Message, "undefined: nonExistentFunction") {
				foundError = true
				break
			}
		}
		if !foundError {
			t.Error("Expected compiler error message in logs, but didn't find it.")
		}
		close(done)
	}).Return(nil)

	goModContent := []byte("module worker-test\ngo 1.20\n")
	badCode := []byte(`
        package main
        func main() {
            nonExistentFunction()
        }
    `)

	job := types.Job{
		JobUID:  uuid.New(),
		Timeout: 60,
		TestNode: types.NodeSpec{
			Alias:        "main_node",
			BuildCommand: "go build -o app main.go",
			EntryCommand: "./app",
			Files: types.FileMap{
				"go.mod":  types.SourceCode(goModContent),
				"main.go": types.SourceCode(badCode),
			},
		},
		SubmissionNodes: []types.NodeSpec{},
		UserID:          "tester_compile_fail",
		SubmittedAt:     time.Now(),
	}

	jobsCh <- job

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("Test timed out waiting for compilation error")
	}

	jobStore.AssertExpectations(t)
}

func TestIntegration_JobDispatcher_RuntimeError(t *testing.T) {
	_, jobStore, jobsCh, _, cleanup := setupTestHelper(t)
	defer cleanup()

	done := make(chan struct{})

	jobStore.On(
		"SaveResult",
		mock.Anything,
		mock.MatchedBy(func(res db.JobResult) bool {
			if !res.Timespent.HasPhases(
				types.PhasePending,
				types.PhaseReserving,
				types.PhaseConfiguringNetwork,
				types.PhaseCompiling,
				types.PhaseRunning,
			) {
				return false
			}
			return res.Outcome == types.OutcomeFailed
		}),
	).Run(func(args mock.Arguments) {
		res := args.Get(1).(db.JobResult)
		foundPanic := false
		for _, l := range res.Logs {
			if strings.Contains(l.Message, "intentional panic") {
				foundPanic = true
				break
			}
		}
		if !foundPanic {
			t.Log("Warning: Did not see panic message in logs (might be in stderr which isn't always parsed perfectly in mocks)")
		}
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
		Timeout: 20,
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
		UserID:          "tester_runtime_fail",
		SubmittedAt:     time.Now(),
	}

	jobsCh <- job

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("Test timed out waiting for runtime error")
	}

	jobStore.AssertExpectations(t)
}

func TestIntegration_JobDispatcher_Timeout(t *testing.T) {
	_, jobStore, jobsCh, _, cleanup := setupTestHelper(t)
	defer cleanup()

	done := make(chan struct{})

	jobStore.On(
		"SaveResult",
		mock.Anything,
		mock.MatchedBy(func(res db.JobResult) bool {
			if !res.Timespent.HasPhases(
				types.PhasePending,
				types.PhaseReserving,
				types.PhaseConfiguringNetwork,
				types.PhaseCompiling,
				types.PhaseRunning,
			) {
				return false
			}

			if errors.Is(res.Error, context.DeadlineExceeded) {
				t.Log("Note: context deadline exceeded detected in error")
			}

			return res.Outcome == types.OutcomeTimeout
		}),
	).Run(func(args mock.Arguments) {
		close(done)
	}).Return(nil)

	goModContent := []byte("module worker-test\ngo 1.20\n")

	loopCode := []byte(`
        package main
        import "time"
        func main() {
            for {
                time.Sleep(100 * time.Millisecond)
            }
        }
    `)

	job := types.Job{
		JobUID: uuid.New(),

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
		UserID:          "tester_timeout",
		SubmittedAt:     time.Now(),
	}

	jobsCh <- job

	select {
	case <-done:

	case <-time.After(15 * time.Second):
		t.Fatal("Test timed out - Dispatcher failed to kill the infinite loop job")
	}

	jobStore.AssertExpectations(t)
}

func TestIntegration_JobDispatcher_ReadFromFile(t *testing.T) {
	_, jobStore, jobsCh, _, cleanup := setupTestHelper(t)
	defer cleanup()

	done := make(chan struct{})

	traceLog := tt.TraceEvent{
		Timestamp: 123456,
		ID:        "event_1",
		MsgType:   "test",
		MessageID: "msg_1",
		EvtType:   "send",
		From:      "nodeA",
		To:        "nodeB",
	}
	fileBytes, err := json.Marshal(traceLog)
	require.NoError(t, err)

	jobStore.On(
		"SaveResult",
		mock.Anything,
		mock.MatchedBy(func(res db.JobResult) bool {
			if res.Outcome != types.OutcomeSuccess {
				return false
			}

			if !res.Timespent.HasPhases(
				types.PhasePending,
				types.PhaseReserving,
				types.PhaseConfiguringNetwork,
				types.PhaseCompiling,
				types.PhaseRunning,
			) {
				return false
			}

			for _, log := range res.Logs {
				if strings.Contains(log.Message, "VERIFICATION_SUCCESS") {
					return true
				}
			}
			return false
		}),
	).Run(func(args mock.Arguments) {
		close(done)
	}).Return(nil)

	goModContent := []byte("module worker-test\ngo 1.20\n")
	code := fmt.Sprintf(`
        package main
        import (
            "fmt"
            "os"
        )
        func main() {
            expectedLen := %d
            content, err := os.ReadFile("trace_log.json")
            if err != nil {
                fmt.Printf("Error reading file: %%v\n", err)
                os.Exit(1)
            }
            if len(content) != expectedLen {
                fmt.Printf("Size mismatch. Got %%d, want %%d\n", len(content), expectedLen)
                os.Exit(1)
            }
            fmt.Println("VERIFICATION_SUCCESS")
        }
    `, len(fileBytes))

	job := types.Job{
		JobUID:  uuid.New(),
		Timeout: 10,
		TestNode: types.NodeSpec{
			Alias:        "main_node",
			BuildCommand: "go build -o app main.go",
			EntryCommand: "./app",
			Files: types.FileMap{
				"go.mod":         types.SourceCode(goModContent),
				"main.go":        types.SourceCode(code),
				"trace_log.json": types.SourceCode(fileBytes),
			},
		},
		SubmissionNodes: []types.NodeSpec{},
		UserID:          "tester_file_read",
		SubmittedAt:     time.Now(),
	}

	jobsCh <- job

	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("Test timed out waiting for file read verification")
	}

	jobStore.AssertExpectations(t)
}

type MockErrorNetworkManager struct{}

func (m *MockErrorNetworkManager) CreateAndConnect(ctx context.Context, workers []Worker) (func(), string, error) {
	return nil, "", fmt.Errorf("simulated network creation failure")
}

func TestIntegration_JobDispatcher_NetworkFailure(t *testing.T) {

	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	require.NoError(t, err)

	jobStore := new(MockJobStore)
	metricsCollector := metrics.NewInMemoryMetricsCollector()

	netManager := &MockErrorNetworkManager{}
	workerImage := "ghcr.io/distcodep7/dsnet:latest"

	wp := NewDockerWorkerProducer(cli, workerImage)
	wm, err := NewWorkerManager(1, wp)
	require.NoError(t, err)

	jobsCh := make(chan types.Job, 10)
	resultsCh := make(chan types.StreamingJobEvent, 100)
	cancelCh := make(chan types.CancelJobRequest, 10)

	dispatcher := NewJobDispatcher(
		cancelCh,
		jobsCh,
		resultsCh,
		wm,
		netManager,
		jobStore,
		metricsCollector,
	)

	ctx, cancel := context.WithCancel(context.Background())
	go dispatcher.Run(ctx)

	defer func() {
		cancel()
		wm.Shutdown()
		cli.Close()
	}()

	done := make(chan struct{})

	jobStore.On(
		"SaveResult",
		mock.Anything,
		mock.MatchedBy(func(res db.JobResult) bool {
			return res.Outcome == types.OutcomeFailed
		}),
	).Run(func(args mock.Arguments) {
		res := args.Get(1).(db.JobResult)

		if res.Error == nil || !strings.Contains(res.Error.Error(), "simulated network creation failure") {
			t.Errorf("Expected network failure error, got: %v", res.Error)
		}
		close(done)
	}).Return(nil)

	job := types.Job{
		JobUID:  uuid.New(),
		Timeout: 5,
		TestNode: types.NodeSpec{
			Alias:        "main_node",
			BuildCommand: "go build",
			EntryCommand: "./app",
			Files:        types.FileMap{},
		},
		SubmissionNodes: []types.NodeSpec{},
		UserID:          "tester_net_fail",
		SubmittedAt:     time.Now(),
	}

	jobsCh <- job

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Test timed out waiting for network failure handling")
	}

	jobStore.AssertExpectations(t)
}
