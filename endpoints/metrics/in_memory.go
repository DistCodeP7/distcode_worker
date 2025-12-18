package metrics

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/DistCodeP7/distcode_worker/types"
)

type InMemoryMetricsCollector struct {
	mu                sync.Mutex
	CurrentJobs       int64
	JobTotal          int64
	JobSuccess        int64
	JobFailure        int64
	JobCanceled       int64
	JobTimeout        int64
	TotalDuration     time.Duration
	StartTime         time.Time
	TimeSpentInPhases map[types.Phase]time.Duration
}

func NewInMemoryMetricsCollector() *InMemoryMetricsCollector {
	return &InMemoryMetricsCollector{
		StartTime:         time.Now(),
		TimeSpentInPhases: make(map[types.Phase]time.Duration),
	}
}

var _ JobMetricsCollector = (*InMemoryMetricsCollector)(nil)

func (m *InMemoryMetricsCollector) TrackTimeSpent(timeSpent map[types.Phase]time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for phase, duration := range timeSpent {
		m.TimeSpentInPhases[phase] += duration
	}
}

func (m *InMemoryMetricsCollector) IncJobOutcome(outcome types.Outcome) {
	switch outcome {
	case types.OutcomeSuccess:
		m.incJobSuccess()
	case types.OutcomeFailed, types.OutcomeCompilationError:
		m.incJobFailure()
	case types.OutcomeCanceled:
		m.incJobCanceled()
	case types.OutcomeTimeout:
		m.incJobTimeout()
	}

}

// StartJob records the start of a job and returns a function to call when the job ends
func (m *InMemoryMetricsCollector) StartJob() func() {
	m.incJobTotal()
	m.incCurrentJobs()
	startTime := time.Now()
	return func() {
		duration := time.Since(startTime)
		m.mu.Lock()
		defer m.mu.Unlock()
		m.CurrentJobs--
		m.TotalDuration += duration
	}
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
		"current_jobs":         m.CurrentJobs,
		"job_total":            m.JobTotal,
		"job_success":          m.JobSuccess,
		"job_failure":          m.JobFailure,
		"job_canceled":         m.JobCanceled,
		"job_timeout":          m.JobTimeout,
		"job_total_seconds":    m.TotalDuration.Seconds(),
		"job_avg_seconds":      avg,
		"start_time":           m.StartTime.Format(time.RFC3339),
		"uptime_seconds":       time.Since(m.StartTime).Seconds(),
		"time_spent_in_phases": m.timeSpentInPhases(),
	}
	b, _ := json.MarshalIndent(data, "", "  ")
	return b
}

func (m *InMemoryMetricsCollector) PartName() string {
	return "in_memory_metrics"
}

func (m *InMemoryMetricsCollector) timeSpentInPhases() map[string]float64 {
	phaseData := make(map[string]float64)
	for phase, duration := range m.TimeSpentInPhases {
		phaseData[string(phase)] = duration.Seconds()
	}
	return phaseData
}

func (m *InMemoryMetricsCollector) incJobSuccess()  { m.inc(&m.JobSuccess) }
func (m *InMemoryMetricsCollector) incJobFailure()  { m.inc(&m.JobFailure) }
func (m *InMemoryMetricsCollector) incJobCanceled() { m.inc(&m.JobCanceled) }
func (m *InMemoryMetricsCollector) incJobTimeout()  { m.inc(&m.JobTimeout) }
func (m *InMemoryMetricsCollector) incCurrentJobs() { m.inc(&m.CurrentJobs) }
func (m *InMemoryMetricsCollector) incJobTotal()    { m.inc(&m.JobTotal) }

// Helper to lock and increment to ensure code is clean
func (m *InMemoryMetricsCollector) inc(field *int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	*field++
}
