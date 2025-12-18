package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
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

// --- Updated Mock Implementation ---

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

func TestIntegration_JobDispatcher_SuccessExpanded(t *testing.T) {
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
		mock.Anything, // Context
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
            // Simulate running tests by just writing the result file
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
				"main.go": types.SourceCode([]byte(code)),
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

	// Updated: Use MatchedBy to check the Outcome inside the db.JobResult struct
	jobStore.On(
		"SaveResult",
		mock.Anything,
		mock.MatchedBy(func(res db.JobResult) bool {
			return res.Outcome == types.OutcomeCanceled
		}),
	).Run(func(args mock.Arguments) {
		close(done)
	}).Return(nil)

	goModContent := []byte("module worker-test\ngo 1.20\n")
	badCode := []byte(`
        package main
        import "time"
        func main() {
            time.Sleep(60 * time.Second)
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
		UserID:          "Something",
		SubmittedAt:     time.Now(),
		ProblemID:       -1,
	}

	jobsCh <- job

	time.Sleep(5 * time.Second)
	fmt.Println("Sending cancellation")
	cancelCh <- types.CancelJobRequest{
		JobUID: job.JobUID,
	}

	select {
	case <-done:

	case <-time.After(30 * time.Second):
		t.Fatal("Test timed out waiting for cancel failure")
	}

	jobStore.AssertExpectations(t)
}

func TestIntegration_JobDispatcher_CancelEarly(t *testing.T) {
	_, jobStore, jobsCh, cancelCh, cleanup := setupTestHelper(t)
	defer cleanup()

	done := make(chan struct{})

	// Updated: Use MatchedBy
	jobStore.On(
		"SaveResult",
		mock.Anything,
		mock.MatchedBy(func(res db.JobResult) bool {
			return res.Outcome == types.OutcomeCanceled
		}),
	).Run(func(args mock.Arguments) {
		close(done)
	}).Return(nil)

	goModContent := []byte("module worker-test\ngo 1.20\n")
	badCode := []byte(`
        package main
        import "time"
        func main() {
            time.Sleep(60 * time.Second)
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
		UserID:          "Something",
		SubmittedAt:     time.Now(),
		ProblemID:       -1,
	}

	cancelCh <- types.CancelJobRequest{
		JobUID: job.JobUID,
	}
	jobsCh <- job

	select {
	case <-done:

	case <-time.After(30 * time.Second):
		t.Fatal("Test timed out waiting for cancel failure")
	}

	jobStore.AssertExpectations(t)
}

func TestIntegration_JobDispatcher_CompilationError(t *testing.T) {
	_, jobStore, jobsCh, _, cleanup := setupTestHelper(t)
	defer cleanup()

	done := make(chan struct{})

	// Updated: Use MatchedBy for Outcome, Run for log inspection
	jobStore.On(
		"SaveResult",
		mock.Anything,
		mock.MatchedBy(func(res db.JobResult) bool {
			return res.Outcome == types.OutcomeCompilationError
		}),
	).Run(func(args mock.Arguments) {
		res := args.Get(1).(db.JobResult)
		foundErrorMsg := false
		for _, l := range res.Logs {
			if contains(l.Message, "undefined: nonExistentFunction") {
				foundErrorMsg = true
			}
		}
		if !foundErrorMsg {
			fmt.Println("WARNING: Expected compiler error message not found in logs")
		}
		close(done)
	}).Return(nil)

	goModContent := []byte("module worker-test\ngo 1.20\n")
	badCode := []byte(`
        package main
        import "fmt"
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
		UserID:          "Something",
		SubmittedAt:     time.Now(),
		ProblemID:       -1,
	}

	jobsCh <- job

	select {
	case <-done:

	case <-time.After(30 * time.Second):
		t.Fatal("Test timed out waiting for compilation failure")
	}

	jobStore.AssertExpectations(t)
}

func TestIntegration_JobDispatcher_RuntimeError(t *testing.T) {
	_, jobStore, jobsCh, _, cleanup := setupTestHelper(t)
	defer cleanup()

	done := make(chan struct{})

	// Updated: Use MatchedBy
	jobStore.On(
		"SaveResult",
		mock.Anything,
		mock.MatchedBy(func(res db.JobResult) bool {
			return res.Outcome == types.OutcomeFailed
		}),
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
		UserID:          "Something",
		SubmittedAt:     time.Now(),
		ProblemID:       -1,
	}

	jobsCh <- job

	select {
	case <-done:

	case <-time.After(20 * time.Second):
		t.Fatal("Test timed out waiting for runtime failure")
	}

	jobStore.AssertExpectations(t)
}

func TestIntegration_JobDispatcher_Timeout(t *testing.T) {
	_, jobStore, jobsCh, _, cleanup := setupTestHelper(t)
	defer cleanup()

	done := make(chan struct{})

	// Updated: Use MatchedBy
	jobStore.On(
		"SaveResult",
		mock.Anything,
		mock.MatchedBy(func(res db.JobResult) bool {
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
                time.Sleep(1 * time.Second)
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
		UserID:          "Something",
		SubmittedAt:     time.Now(),
		ProblemID:       -1,
	}

	jobsCh <- job

	select {
	case <-done:

	case <-time.After(10 * time.Second):
		t.Fatal("Test timed out - Dispatcher failed to kill the infinite loop job")
	}

	jobStore.AssertExpectations(t)
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && len(substr) > 0 && s[0:len(substr)] == substr
}

func TestIntegration_JobDispatcher_ReadFromFile(t *testing.T) {
	_, jobStore, jobsCh, _, cleanup := setupTestHelper(t)
	defer cleanup()

	done := make(chan struct{})
	traceLog := tt.TraceEvent{
		Timestamp: 1,
		ID:        uuid.NewString(),
		MsgType:   "something",
		MessageID: "Foo",
		EvtType:   "recv",
		From:      "A",
		To:        "B",
		VectorClock: map[string]uint64{
			"A": 1, "B": 1,
		},
		Payload: nil,
	}

	b, err := json.Marshal(traceLog)
	require.NoError(t, err)
	fileContentToInject := append(b, '\n')

	jobStore.On(
		"SaveResult",
		mock.Anything,
		mock.MatchedBy(func(res db.JobResult) bool {
			if res.Outcome != types.OutcomeSuccess {
				return false
			}

			foundConfirmation := false
			for _, log := range res.Logs {
				if bytes.Contains([]byte(log.Message), []byte("VERIFICATION_SUCCESS")) {
					foundConfirmation = true
					break
				}
			}
			return foundConfirmation
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
            
            // Attempt to read the injected file
            content, err := os.ReadFile("trace_log.jsonl")
            if err != nil {
                fmt.Printf("FAILURE: Could not read file: %%v\n", err)
                os.Exit(1)
            }
            
            // Verify data integrity
            if len(content) != expectedLen {
                fmt.Printf("FAILURE: Length mismatch. Got %%d, want %%d\n", len(content), expectedLen)
                os.Exit(1)
            }

            // If we get here, the file was injected correctly
            fmt.Println("VERIFICATION_SUCCESS")
        }
    `, len(fileContentToInject))

	job := types.Job{
		JobUID:  uuid.New(),
		Timeout: 5,
		TestNode: types.NodeSpec{
			Alias:        "main_node",
			BuildCommand: "go build -o app main.go",
			EntryCommand: "./app",
			Files: types.FileMap{
				"go.mod":          types.SourceCode(goModContent),
				"main.go":         types.SourceCode([]byte(code)),
				"trace_log.jsonl": types.SourceCode(fileContentToInject),
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
	case <-time.After(10 * time.Second):
		t.Fatal("Test timed out - Dispatcher failed to process job or verification failed")
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
	defer cli.Close()

	workerImage := "ghcr.io/distcodep7/dsnet:latest"

	jobStore := new(MockJobStore)
	metricsCollector := metrics.NewInMemoryMetricsCollector()

	netManager := &MockErrorNetworkManager{}

	wp := NewDockerWorkerProducer(cli, workerImage)
	wm, err := NewWorkerManager(1, wp)
	require.NoError(t, err)
	defer wm.Shutdown()

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
		metricsCollector,
	)

	go dispatcher.Run(context.Background())

	done := make(chan struct{})

	// Updated: Use MatchedBy
	jobStore.On(
		"SaveResult",
		mock.Anything,
		mock.MatchedBy(func(res db.JobResult) bool {
			return res.Outcome == types.OutcomeFailed
		}),
	).Run(func(args mock.Arguments) {
		close(done)
	}).Return(nil)

	jobID := uuid.New()
	job := types.Job{
		JobUID:  jobID,
		Timeout: 5,
		TestNode: types.NodeSpec{
			Alias:        "main_node",
			BuildCommand: "go build",
			EntryCommand: "./app",
			Files:        types.FileMap{},
		},
		SubmissionNodes: []types.NodeSpec{},
	}

	jobsCh <- job

	select {
	case <-done:

	case <-time.After(5 * time.Second):
		t.Fatal("Test timed out waiting for network failure handling")
	}

	jobStore.AssertExpectations(t)
}
