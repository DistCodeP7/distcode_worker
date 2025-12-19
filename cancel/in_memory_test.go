package cancel

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jonboulle/clockwork"
)

func TestCancellationTracker_Track_AddsJobID(t *testing.T) {
	retention := 2 * time.Minute
	gcInterval := 1 * time.Minute
	fakeClock := clockwork.NewFakeClock()

	ct := &CancellationTracker{
		pending:    make(map[string]time.Time),
		retention:  retention,
		gcInterval: gcInterval,
		clock:      fakeClock,
	}

	jobID := uuid.New()
	ct.Track(jobID)

	ct.mu.Lock()
	expiry, exists := ct.pending[jobID.String()]
	ct.mu.Unlock()

	if !exists {
		t.Fatalf("Expected jobID to be tracked, but it was not found")
	}

	expectedExpiry := fakeClock.Now().Add(retention)
	if !expiry.Equal(expectedExpiry) {
		t.Errorf("Expected expiry %v, got %v", expectedExpiry, expiry)
	}
}
func TestCancellationTracker_IsCancelled_NotTracked(t *testing.T) {
	retention := 2 * time.Minute
	gcInterval := 1 * time.Minute
	fakeClock := clockwork.NewFakeClock()

	ct := &CancellationTracker{
		pending:    make(map[string]time.Time),
		retention:  retention,
		gcInterval: gcInterval,
		clock:      fakeClock,
	}

	jobID := uuid.New()
	if ct.IsCancelled(jobID) {
		t.Errorf("Expected IsCancelled to return false for untracked jobID")
	}
}

func TestCancellationTracker_IsCancelled_TrackedAndNotExpired(t *testing.T) {
	retention := 2 * time.Minute
	gcInterval := 1 * time.Minute
	fakeClock := clockwork.NewFakeClock()

	ct := &CancellationTracker{
		pending:    make(map[string]time.Time),
		retention:  retention,
		gcInterval: gcInterval,
		clock:      fakeClock,
	}

	jobID := uuid.New()
	ct.Track(jobID)

	if !ct.IsCancelled(jobID) {
		t.Errorf("Expected IsCancelled to return true for tracked and not expired jobID")
	}
}

func TestCancellationTracker_IsCancelled_TrackedButExpired(t *testing.T) {
	retention := 2 * time.Minute
	gcInterval := 1 * time.Minute
	fakeClock := clockwork.NewFakeClock()

	ct := &CancellationTracker{
		pending:    make(map[string]time.Time),
		retention:  retention,
		gcInterval: gcInterval,
		clock:      fakeClock,
	}

	jobID := uuid.New()
	ct.Track(jobID)

	// Advance clock past expiry
	fakeClock.Advance(3 * time.Minute)

	if ct.IsCancelled(jobID) {
		t.Errorf("Expected IsCancelled to return false for expired jobID")
	}
}
func TestCancellationTracker_StartGC_RemovesExpiredEntries(t *testing.T) {
	retention := 2 * time.Minute
	gcInterval := 1 * time.Minute
	fakeClock := clockwork.NewFakeClock()

	ct := &CancellationTracker{
		pending:    make(map[string]time.Time),
		retention:  retention,
		gcInterval: gcInterval,
		clock:      fakeClock,
	}

	jobID := uuid.New()
	ct.Track(jobID)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start GC in a goroutine
	go ct.StartGC(ctx)

	// Advance clock past expiry and GC interval
	fakeClock.Advance(3 * time.Minute)
	// Trigger ticker
	fakeClock.BlockUntil(1)
	fakeClock.Advance(gcInterval)

	// Wait a moment for GC to run
	time.Sleep(10 * time.Millisecond)

	ct.mu.Lock()
	_, exists := ct.pending[jobID.String()]
	ct.mu.Unlock()

	if exists {
		t.Errorf("Expected expired jobID to be removed by GC")
	}
}

func TestCancellationTracker_StartGC_StopsOnContextCancel(t *testing.T) {
	retention := 2 * time.Minute
	gcInterval := 1 * time.Minute
	fakeClock := clockwork.NewFakeClock()

	ct := &CancellationTracker{
		pending:    make(map[string]time.Time),
		retention:  retention,
		gcInterval: gcInterval,
		clock:      fakeClock,
	}

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		ct.StartGC(ctx)
		close(done)
	}()

	cancel()

	select {
	case <-done:
		// Success: StartGC exited
	case <-time.After(100 * time.Millisecond):
		t.Errorf("Expected StartGC to exit after context cancellation")
	}
}
