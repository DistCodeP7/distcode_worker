package worker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type MockWorker struct {
	id          string
	stopCalled  bool
	stopError   error
	stopDelayMs int
	mu          sync.Mutex
}

func (m *MockWorker) ID() string {
	return m.id
}

func (m *MockWorker) Stop(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stopCalled = true
	if m.stopDelayMs > 0 {
		time.Sleep(time.Duration(m.stopDelayMs) * time.Millisecond)
	}
	return m.stopError
}

func (m *MockWorker) ConnectToNetwork(ctx context.Context, networkName, alias string) error {
	return nil
}

func (m *MockWorker) DisconnectFromNetwork(ctx context.Context, networkName string) error {
	return nil
}

const MockOutput = "Mock output"

func (m *MockWorker) ExecuteCode(ctx context.Context, code string, stdoutCh, stderrCh chan string) error {
	stdoutCh <- MockOutput
	return nil
}

func (m *MockWorker) WasStopped() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.stopCalled
}

func CreateMockWorkers(count int) []WorkerInterface {
	workers := make([]WorkerInterface, count)
	for i := range count {
		workers[i] = &MockWorker{id: fmt.Sprintf("W%d", i+1)}
	}
	return workers
}

func TestNewWorkerManager(t *testing.T) {

	t.Run("Success", func(t *testing.T) {
		workers := CreateMockWorkers(3)
		manager, err := NewWorkerManager(workers)
		assert.NoError(t, err)
		assert.NotNil(t, manager)
		assert.Len(t, manager.workers, 3, "Should have 3 workers in the map")
		assert.Len(t, manager.idleWorkers, 3, "Should have 3 workers in the idle list")
	})

	t.Run("ErrorEmptyID", func(t *testing.T) {
		workers := []WorkerInterface{
			&MockWorker{id: "W1"},
			&MockWorker{id: ""},
			&MockWorker{id: "W3"},
		}
		manager, err := NewWorkerManager(workers)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "worker at index 1 has an empty ID")
		assert.Nil(t, manager)
	})
}

func TestReserveWorkers(t *testing.T) {
	workers := CreateMockWorkers(5)
	manager, _ := NewWorkerManager(workers)
	jobID := 101

	t.Run("Success", func(t *testing.T) {
		reserved, err := manager.ReserveWorkers(jobID, 3)

		assert.NoError(t, err)
		assert.Len(t, reserved, 3, "Should reserve exactly 3 workers")
		assert.Len(t, manager.idleWorkers, 2, "Idle list should reduce by 3")
		assert.Len(t, manager.jobPool[jobID], 3, "Job pool should contain 3 workers for the job")
		reserved[0] = nil
		assert.NotNil(t, manager.jobPool[jobID][0], "Internal job pool shouldn't be affected by mutation")
	})

	t.Run("ErrorNotEnoughWorkers", func(t *testing.T) {

		_, err := manager.ReserveWorkers(102, 3)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not enough idle workers currently")
		assert.Len(t, manager.idleWorkers, 2, "Idle list should remain unchanged")
	})
}

func TestReleaseJob(t *testing.T) {
	workers := CreateMockWorkers(5)
	manager, _ := NewWorkerManager(workers)
	jobID := 201

	manager.ReserveWorkers(jobID, 3)

	initialIdleCount := len(manager.idleWorkers)

	t.Run("Success", func(t *testing.T) {
		err := manager.ReleaseJob(jobID)

		assert.NoError(t, err)
		assert.Len(t, manager.jobPool, 0, "Job pool should be empty")
		assert.Len(t, manager.idleWorkers, initialIdleCount+3, "Idle workers should increase by 3")
		assert.NotContains(t, manager.jobPool, jobID, "Job ID should be removed from job pool")

		// Check that thereleased workers are back in the idle list
		idleIDs := make(map[string]bool)
		for _, w := range manager.idleWorkers {
			idleIDs[w.ID()] = true
		}
		for i := 0; i < 3; i++ {
			assert.Contains(t, idleIDs, fmt.Sprintf("W%d", i+1), "Released worker should be in idle list")
		}
	})

	t.Run("ErrorJobNotFound", func(t *testing.T) {
		err := manager.ReleaseJob(999)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "jobId 999 not found")
	})
}

func TestShutdown(t *testing.T) {
	workers := CreateMockWorkers(3)

	mockWorkers := []*MockWorker{workers[0].(*MockWorker), workers[1].(*MockWorker), workers[2].(*MockWorker)}
	manager, _ := NewWorkerManager(workers)

	t.Run("Success", func(t *testing.T) {
		err := manager.Shutdown()

		assert.NoError(t, err)
		assert.True(t, mockWorkers[0].WasStopped(), "Worker 1 Stop() should be called")
		assert.True(t, mockWorkers[1].WasStopped(), "Worker 2 Stop() should be called")

		assert.Nil(t, manager.workers, "Internal workers map should be nil")
		assert.Nil(t, manager.idleWorkers, "Internal idle list should be nil")
		assert.Nil(t, manager.jobPool, "Internal job pool should be nil")
	})

	t.Run("ErrorPropagation", func(t *testing.T) {
		workers := CreateMockWorkers(3)
		mockWorkers := []*MockWorker{
			workers[0].(*MockWorker),
			workers[1].(*MockWorker),
			workers[2].(*MockWorker),
		}
		expectedErr := errors.New("W2 failed to stop")
		mockWorkers[1].stopError = expectedErr

		manager, _ = NewWorkerManager(workers)
		err := manager.Shutdown()

		assert.ErrorIs(t, err, expectedErr, "Should return the first error encountered")
		assert.True(t, mockWorkers[0].WasStopped(), "All workers should attempt to stop")
		assert.True(t, mockWorkers[1].WasStopped(), "All workers should attempt to stop")
	})

	t.Run("ConcurrencyTest", func(t *testing.T) {
		workers := CreateMockWorkers(5)
		manager, _ := NewWorkerManager(workers)

		manager.ReserveWorkers(301, 2)

		var wg sync.WaitGroup
		wg.Add(3)

		go func() {
			defer wg.Done()
			manager.Shutdown()
		}()
		go func() {
			defer wg.Done()

			manager.ReleaseJob(301)
		}()
		go func() {
			defer wg.Done()

			manager.ReserveWorkers(302, 1)
		}()

		wg.Wait()

	})
}
