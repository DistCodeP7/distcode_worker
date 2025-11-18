package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/DistCodeP7/distcode_worker/types"
	"github.com/jonboulle/clockwork"

	"github.com/stretchr/testify/assert"
)

func TestSendJobErrorResult(t *testing.T) {
	resultsChan := make(chan types.StreamingJobEvent, 1)
	dispatcher := &JobDispatcher{
		resultsChannel: resultsChan,
	}
	dispatcher.sendJobError(types.JobRequest{
		ProblemId:    1,
		UserId:       "42",
		Code:         []string{"print('Hello, World!')"},
		TimeoutLimit: 1,
	}, errors.New("Test error"))

	result := <-resultsChan
	assert.Equal(t, "error", result.Events[0].Kind)
	assert.Equal(t, "Test error", *result.Events[0].Message)
	assert.Equal(t, 1, result.ProblemId)
	assert.Equal(t, "42", result.UserId)
	assert.Equal(t, -1, result.SequenceIndex)
}

// TestRequestWorkerReservation_WithReleaseAndFakeClock verifies that worker reservation blocks when no workers are available,
// and resumes correctly when a worker is released and the fake clock triggers a retry.
// It creates a mock worker manager and dispatcher with a fake clock, reserves the only worker, then attempts to reserve again in a goroutine.
// The test ensures that the reservation blocks until the worker is released and the clock is triggered, at which point the reservation succeeds.
func TestRequestWorkerReservation_WithReleaseAndFakeClock(t *testing.T) {
	worker := CreateMockWorkers(1)
	wm, err := NewWorkerManager(worker)
	assert.NoError(t, err)

	fc := clockwork.NewFakeClock()
	dispatcher := &JobDispatcher{
		workerManager: wm,
		Clock:         fc,
	}

	ctx := t.Context()
	jobId, workersNeeded := 1, 1
	workers, err := dispatcher.workerManager.ReserveWorkers(jobId, workersNeeded)
	assert.NoError(t, err)
	assert.Len(t, workers, 1)

	// Try reserving again — should block until clock fires
	done := make(chan struct{})
	go func() {
		_, err := dispatcher.requestWorkerReservation(ctx, 2, 1)
		assert.NoError(t, err)
		close(done)
	}()

	// Allow goroutine to attempt reservation and block
	time.Sleep(10 * time.Millisecond)

	// Release the first job so the worker becomes free
	err = dispatcher.workerManager.ReleaseJob(1)
	assert.NoError(t, err)

	// Trigger the fake clock so retry loop wakes up
	fc.Advance(1 * time.Second)
	select {
	case <-done:
		// success
	case <-time.After(500 * time.Millisecond):
		t.Fatal("reservation did not complete after release + clock trigger")
	}
}

// TestRequestWorkerReservation_ContextCanceled verifies that the worker reservation
// is correctly canceled when the context's deadline is exceeded.
// It uses a fake clock to advance time past the context timeout,
// ensuring the function returns a context.DeadlineExceeded error.
func TestRequestWorkerReservation_ContextCanceled(t *testing.T) {
	// Setup with no workers, so reservation will always fail and retry.
	wm, err := NewWorkerManager([]WorkerInterface{})
	assert.NoError(t, err)

	fc := clockwork.NewFakeClock()
	dispatcher := &JobDispatcher{
		workerManager: wm,
		Clock:         fc,
	}

	// Create a context with a very short timeout.
	timeout := 100 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	errChan := make(chan error, 1)
	go func() {
		_, err := dispatcher.requestWorkerReservation(ctx, 1, 1)
		errChan <- err
	}()

	assert.NoError(t, fc.BlockUntilContext(ctx, 1))
	fc.Advance(timeout + 1*time.Millisecond)

	// Check that the function returned the expected error.
	select {
	case err := <-errChan:
		assert.Error(t, err, "Expected an error due to context cancellation")
		assert.True(t, errors.Is(err, context.DeadlineExceeded), "Error should be context.DeadlineExceeded")
	case <-time.After(500 * time.Millisecond):
		t.Fatal("requestWorkerReservation did not return after context was canceled")
	}
}

// ControllableMockWorker sends a message and then blocks until its context is cancelled.
// This simulates a long-running job and gives the test control over its lifecycle.
type ControllableMockWorker struct {
	MockWorker
	messageSent chan struct{}
}

func (m *ControllableMockWorker) ExecuteCode(ctx context.Context, code string, stdoutCh, stderrCh chan string) error {
	// This message is intended to be picked up by the periodic flush.
	stdoutCh <- "periodic flush message"
	if m.messageSent != nil {
		close(m.messageSent) // Signal that the message has been sent
	}

	// Block until the job context is cancelled.
	<-ctx.Done()
	return nil
}

// MockWorkerManager provides a mock implementation of the WorkerManagerInterface.
type MockWorkerManager struct {
	ReserveWorkersFunc func(jobId, jobSize int) ([]WorkerInterface, error)
	ReleaseJobFunc     func(jobId int) error
	ShutdownFunc       func() error
}

func (m *MockWorkerManager) ReserveWorkers(jobId, jobSize int) ([]WorkerInterface, error) {
	return m.ReserveWorkersFunc(jobId, jobSize)
}

func (m *MockWorkerManager) ReleaseJob(jobId int) error {
	return m.ReleaseJobFunc(jobId)
}

func (m *MockWorkerManager) Shutdown() error {
	if m.ShutdownFunc != nil {
		return m.ShutdownFunc()
	}
	return nil
}

// MockNetworkManager provides a mock implementation of the NetworkManagerInterface.
type MockNetworkManager struct {
	CreateAndConnectFunc func(ctx context.Context, workers []WorkerInterface) (cleanup func(), err error)
}

func (m *MockNetworkManager) CreateAndConnect(ctx context.Context, workers []WorkerInterface) (cleanup func(), err error) {
	return m.CreateAndConnectFunc(ctx, workers)
}

// TestProcessJob_SendsPeriodicAndFinalFlush verifies that processJob correctly sends
// an initial message from a periodic flush and a final message upon completion.
// It uses a fake clock and mock dependencies to precisely control the timing and
// behavior of the job processing flow, ensuring deterministic results.
func TestProcessJob_SendsPeriodicAndFinalFlush(t *testing.T) {
	fc := clockwork.NewFakeClock()
	resultsChan := make(chan types.StreamingJobEvent, 2)
	flushInterval := 100 * time.Millisecond

	messageSent := make(chan struct{})
	mockWorkers := []WorkerInterface{&ControllableMockWorker{
		MockWorker:  MockWorker{id: "W1"},
		messageSent: messageSent,
	}}

	mockWM := &MockWorkerManager{
		ReserveWorkersFunc: func(jobId, jobSize int) ([]WorkerInterface, error) {
			return mockWorkers, nil
		},
		ReleaseJobFunc: func(jobId int) error {
			return nil
		},
	}

	mockNM := &MockNetworkManager{
		CreateAndConnectFunc: func(ctx context.Context, workers []WorkerInterface) (cleanup func(), err error) {
			return func() {}, nil
		},
	}

	dispatcher := NewJobDispatcher(JobDispatcherConfig{
		ResultsChannel: resultsChan,
		WorkerManager:  mockWM,
		NetworkManager: mockNM,
		Clock:          fc,
		FlushInterval:  flushInterval,
	})

	job := types.JobRequest{
		ProblemId:    1,
		UserId:       "42",
		Code:         []string{"some code"},
		TimeoutLimit: 10,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	jobDone := make(chan struct{})
	go func() {
		dispatcher.processJob(ctx, job)
		close(jobDone)
	}()

	assert.Eventually(t, func() bool {
		select {
		case <-messageSent:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond, "timed out waiting for worker to send message")

	fc.Advance(flushInterval)
	var firstResult types.StreamingJobEvent
	select {
	case firstResult = <-resultsChan:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for the first (periodic) result")
	}

	assert.Equal(t, 1, firstResult.ProblemId)
	assert.Equal(t, 0, firstResult.SequenceIndex)
	assert.Len(t, firstResult.Events, 1)
	assert.Equal(t, "periodic flush message", *firstResult.Events[0].Message)
	assert.Equal(t, "stdout", firstResult.Events[0].Kind)

	cancel()

	select {
	case <-jobDone:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for processJob to finish")
	}

	var finalResult types.StreamingJobEvent
	select {
	case finalResult = <-resultsChan:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for the final result")
	}
	assert.Equal(t, 1, finalResult.ProblemId)
	assert.Equal(t, -1, finalResult.SequenceIndex, "Final message must have SequenceIndex -1")
}
