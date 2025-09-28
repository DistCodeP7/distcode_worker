package worker

import (
	"errors"
	"testing"
	"time"

	"github.com/DistCodeP7/distcode_worker/types"
	"github.com/jonboulle/clockwork"

	"github.com/stretchr/testify/assert"
)

func TestSendJobErrorResult(t *testing.T) {
	resultsChan := make(chan types.StreamingJobResult, 1)
	dispatcher := &JobDispatcher{
		resultsChannel: resultsChan,
	}
	dispatcher.sendJobError(types.JobRequest{
		ProblemId:    1,
		UserId:       42,
		Code:         []string{"print('Hello, World!')"},
		TimeoutLimit: 1,
	}, errors.New("Test error"))

	result := <-resultsChan
	assert.Equal(t, "error", result.Events[0].Kind)
	assert.Equal(t, "Test error", result.Events[0].Message)
	assert.Equal(t, 1, result.JobId)
	assert.Equal(t, 42, result.UserId)
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
