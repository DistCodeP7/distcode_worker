package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/DistCodeP7/distcode_worker/endpoints/metrics"
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

func (m *MockJobStore) SaveResult(
	ctx context.Context,
	jobID uuid.UUID,
	outcome types.Outcome,
	testResults []dt.TestResult,
	logs []types.LogEvent,
	nodeMessageLogs []tt.TraceEvent,
	startTime time.Time,
	queued_at time.Time,
) error {
	args := m.Called(ctx, jobID, outcome, testResults, logs, nodeMessageLogs, startTime)
	return args.Error(0)
}

func TestIntegration_JobDispatcher_Success(t *testing.T) {

	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	require.NoError(t, err)
	defer cli.Close()
	workerImage := "ghcr.io/distcodep7/dsnet:latest"

	jobStore := new(MockJobStore)
	metrics := metrics.NewInMemoryMetricsCollector()

	done := make(chan struct{})

	jobStore.On("SaveResult",
		mock.Anything,
		mock.Anything,
		mock.Anything,
		mock.Anything,
		mock.Anything,
		mock.Anything,
		mock.Anything,
	).Run(func(args mock.Arguments) {
		outcome := args.Get(2).(types.Outcome)

		if outcome != types.OutcomeSuccess {
			logs := args.Get(4).([]types.LogEvent)
			fmt.Printf("\n\n====== JOB FAILED (Outcome: %s) ======\n", outcome)
			for _, l := range logs {

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

	ctx := context.Background()

	go dispatcher.Run(ctx)

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

	jobsCh <- job

	select {
	case <-done:

	case <-time.After(25 * time.Second):
		t.Fatal("Test timed out waiting for job completion")
	}

	wm.Shutdown()

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
	go dispatcher.Run(ctx)

	cleanup := func() {
		cancel()
		wm.Shutdown()
		cli.Close()
	}

	return dispatcher, jobStore, jobsCh, cleanup
}

func TestIntegration_JobDispatcher_CompilationError(t *testing.T) {
	_, jobStore, jobsCh, cleanup := setupTestHelper(t)
	defer cleanup()

	done := make(chan struct{})

	jobStore.On("SaveResult",
		mock.Anything, mock.Anything,
		types.OutcomeCompilationError,
		mock.Anything, mock.Anything, mock.Anything, mock.Anything,
	).Run(func(args mock.Arguments) {

		logs := args.Get(4).([]types.LogEvent)
		foundErrorMsg := false
		for _, l := range logs {

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
	_, jobStore, jobsCh, cleanup := setupTestHelper(t)
	defer cleanup()

	done := make(chan struct{})

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

	case <-time.After(20 * time.Second):
		t.Fatal("Test timed out waiting for runtime failure")
	}

	jobStore.AssertExpectations(t)
}

func TestIntegration_JobDispatcher_Timeout(t *testing.T) {
	_, jobStore, jobsCh, cleanup := setupTestHelper(t)
	defer cleanup()

	done := make(chan struct{})

	jobStore.On("SaveResult",
		mock.Anything, mock.Anything,
		types.OutcomeTimeout,
		mock.Anything, mock.Anything, mock.Anything, mock.Anything,
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
	_, jobStore, jobsCh, cleanup := setupTestHelper(t)
	defer cleanup()

	done := make(chan struct{})

	jobStore.On("SaveResult",
		mock.Anything, mock.Anything,
		types.OutcomeSuccess,
		mock.Anything, mock.Anything, mock.Anything, mock.Anything,
	).Run(func(args mock.Arguments) {
		close(done)
	}).Return(nil)

	goModContent := []byte("module worker-test\ngo 1.20\n")

	fileContent := []byte(`
		package main
		import "fmt"
		func main() {
			fmt.Println("INTEGRATION TEST SUCCESS")
		}
	`)

	trace_log := tt.TraceEvent{
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

	b, err := json.Marshal(trace_log)
	require.NoError(t, err)

	job := types.Job{
		JobUID: uuid.New(),

		Timeout: 3,
		TestNode: types.NodeSpec{
			Alias:        "main_node",
			BuildCommand: "go build -o app main.go",
			EntryCommand: "./app",
			Files: types.FileMap{
				"go.mod":          types.SourceCode(goModContent),
				"main.go":         types.SourceCode(fileContent),
				"trace_log.jsonl": types.SourceCode(append(b, '\n')),
			},
		},
		SubmissionNodes: []types.NodeSpec{},
	}

	jobsCh <- job

	select {
	case <-done:

	case <-time.After(10 * time.Second):
		t.Fatal("Test timed out - Dispatcher failed to kill the infinite loop job")
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
	metrics := metrics.NewInMemoryMetricsCollector()

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
		metrics,
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go dispatcher.Run(ctx)

	done := make(chan struct{})

	jobStore.On("SaveResult",
		mock.Anything, mock.Anything,
		types.OutcomeFailed,
		mock.Anything, mock.Anything, mock.Anything, mock.Anything,
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
