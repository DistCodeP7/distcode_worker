package metrics

import (
	"encoding/json"
	"sync"
	"time"
)

type InMemoryMetricsCollector struct {
	mu            sync.Mutex
	CurrentJobs   int64
	JobTotal      int64
	JobSuccess    int64
	JobFailure    int64
	JobCanceled   int64
	JobTimeout    int64
	TotalDuration time.Duration
	StartTime     time.Time
}

func NewInMemoryMetricsCollector() *InMemoryMetricsCollector {
	return &InMemoryMetricsCollector{
		StartTime: time.Now(),
	}
}

var _ JobMetricsCollector = (*InMemoryMetricsCollector)(nil)

// Helper to lock and increment to ensure code is clean
func (m *InMemoryMetricsCollector) inc(field *int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	*field++
}
func (m *InMemoryMetricsCollector) dec(field *int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	*field--
}

func (m *InMemoryMetricsCollector) DecCurrentJobs() { m.dec(&m.CurrentJobs) }
func (m *InMemoryMetricsCollector) IncCurrentJobs() { m.inc(&m.CurrentJobs) }
func (m *InMemoryMetricsCollector) IncJobTotal()    { m.inc(&m.JobTotal) }
func (m *InMemoryMetricsCollector) IncJobSuccess()  { m.inc(&m.JobSuccess) }
func (m *InMemoryMetricsCollector) IncJobFailure()  { m.inc(&m.JobFailure) }
func (m *InMemoryMetricsCollector) IncJobCanceled() { m.inc(&m.JobCanceled) }
func (m *InMemoryMetricsCollector) IncJobTimeout()  { m.inc(&m.JobTimeout) }
func (m *InMemoryMetricsCollector) ObserveJobDuration(seconds float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.TotalDuration += time.Duration(seconds * float64(time.Second))
}

func (m *InMemoryMetricsCollector) JSON() []byte {
	m.mu.Lock()
	defer m.mu.Unlock()

	avg := 0.0
	// Avoid division by zero
	if m.JobTotal > 0 {
		avg = m.TotalDuration.Seconds() / float64(m.JobTotal)
	}

	data := map[string]interface{}{
		"current_jobs":      m.CurrentJobs,
		"job_total":         m.JobTotal,
		"job_success":       m.JobSuccess,
		"job_failure":       m.JobFailure,
		"job_canceled":      m.JobCanceled,
		"job_timeout":       m.JobTimeout,
		"job_total_seconds": m.TotalDuration.Seconds(),
		"job_avg_seconds":   avg,
		"start_time":        m.StartTime.Format(time.RFC3339),
		"uptime_seconds":    time.Since(m.StartTime).Seconds(),
	}
	b, _ := json.MarshalIndent(data, "", "  ")
	return b
}
