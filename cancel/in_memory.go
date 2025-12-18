package cancel

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jonboulle/clockwork"
)

type CancellationTracker struct {
	mu         sync.Mutex
	pending    map[string]time.Time
	retention  time.Duration
	gcInterval time.Duration
	clock      clockwork.Clock
}

func NewCancellationTracker(retention, gcInterval time.Duration) *CancellationTracker {
	return &CancellationTracker{
		pending:    make(map[string]time.Time),
		retention:  retention,
		gcInterval: gcInterval,
		clock:      clockwork.NewRealClock(),
	}
}

// Track adds a cancellation request that is valid for the retention period
func (ct *CancellationTracker) Track(jobID uuid.UUID) {
	ct.mu.Lock()
	defer ct.mu.Unlock()

	// Set expiration time in the future
	ct.pending[jobID.String()] = ct.clock.Now().Add(ct.retention)
}

// IsCancelled checks if a valid cancellation request exists for this job
// It returns true if the job was cancelled and the request hasn't expired
func (ct *CancellationTracker) IsCancelled(jobID uuid.UUID) bool {
	ct.mu.Lock()
	defer ct.mu.Unlock()

	expiry, exists := ct.pending[jobID.String()]
	if !exists {
		return false
	}

	// If found but expired, treat as not cancelled (GC will clean it up)
	if ct.clock.Now().After(expiry) {
		return false
	}

	return true
}

// StartGC starts the garbage collection loop in a blocking manner (call in a goroutine)
func (ct *CancellationTracker) StartGC(ctx context.Context) {
	ticker := ct.clock.NewTicker(ct.gcInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.Chan():
			ct.cleanup()
		}
	}
}

func (ct *CancellationTracker) cleanup() {
	ct.mu.Lock()
	defer ct.mu.Unlock()

	now := ct.clock.Now()
	for id, expiry := range ct.pending {
		if now.After(expiry) {
			delete(ct.pending, id)
		}
	}
}
