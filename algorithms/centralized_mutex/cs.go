package centralized_mutex

import (
	"sync"
	"time"
)

// CriticalSection serialises work so only one node executes the CS at a time.
type CriticalSection struct {
	mu sync.Mutex
}

func (cs *CriticalSection) Work(nodeID string, dur time.Duration, f func()) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	f()
	time.Sleep(dur)
}
