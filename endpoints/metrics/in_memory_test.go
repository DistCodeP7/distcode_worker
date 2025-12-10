package metrics

import (
	"encoding/json"
	"testing"
	"time"
)

func TestInitialState(t *testing.T) {
	m := NewInMemoryMetricsCollector()

	if m.CurrentJobs != 0 ||
		m.JobTotal != 0 ||
		m.JobSuccess != 0 ||
		m.JobFailure != 0 ||
		m.JobCanceled != 0 ||
		m.JobTimeout != 0 {
		t.Fatalf("initial counters not zero: %+v", m)
	}

	if time.Since(m.StartTime) < 0 {
		t.Fatalf("start time should be in the past")
	}
}

func TestIncrementCounters(t *testing.T) {
	m := NewInMemoryMetricsCollector()

	m.IncCurrentJobs()
	m.IncJobTotal()
	m.IncJobSuccess()
	m.IncJobFailure()
	m.IncJobCanceled()
	m.IncJobTimeout()

	if m.CurrentJobs != 1 ||
		m.JobTotal != 1 ||
		m.JobSuccess != 1 ||
		m.JobFailure != 1 ||
		m.JobCanceled != 1 ||
		m.JobTimeout != 1 {
		t.Fatalf("counters did not increment correctly: %+v", m)
	}

	m.DecCurrentJobs()
	if m.CurrentJobs != 0 {
		t.Fatalf("DecCurrentJobs did not work: %d", m.CurrentJobs)
	}
}

func TestObserveJobDuration(t *testing.T) {
	m := NewInMemoryMetricsCollector()

	m.IncJobTotal()
	m.ObserveJobDuration(2.5) // seconds

	if m.TotalDuration.Seconds() != 2.5 {
		t.Fatalf("TotalDuration = %v, want 2.5s", m.TotalDuration.Seconds())
	}
}

func TestJSON_Output(t *testing.T) {
	m := NewInMemoryMetricsCollector()

	m.IncCurrentJobs()
	m.IncJobTotal()
	m.IncJobSuccess()
	m.ObserveJobDuration(3.0)

	out := m.JSON()

	var data map[string]interface{}
	if err := json.Unmarshal(out, &data); err != nil {
		t.Fatalf("JSON unmarshal error: %v", err)
	}

	// Field correctness
	if data["current_jobs"] != float64(1) {
		t.Fatalf("current_jobs=%v, want 1", data["current_jobs"])
	}
	if data["job_total"] != float64(1) {
		t.Fatalf("job_total=%v, want 1", data["job_total"])
	}
	if data["job_success"] != float64(1) {
		t.Fatalf("job_success=%v, want 1", data["job_success"])
	}

	if data["job_total_seconds"] != 3.0 {
		t.Fatalf("job_total_seconds=%v, want 3.0", data["job_total_seconds"])
	}
	if data["job_avg_seconds"] != 3.0 {
		t.Fatalf("job_avg_seconds=%v, want 3.0", data["job_avg_seconds"])
	}

	// Basic validation for string timestamp
	if _, err := time.Parse(time.RFC3339, data["start_time"].(string)); err != nil {
		t.Fatalf("start_time not RFC3339: %v", data["start_time"])
	}

	// Uptime should be >= 0
	if data["uptime_seconds"].(float64) < 0 {
		t.Fatalf("uptime_seconds < 0: %v", data["uptime_seconds"])
	}
}

func TestPartName(t *testing.T) {
	m := NewInMemoryMetricsCollector()
	if m.PartName() != "in_memory_metrics" {
		t.Fatalf("PartName=%q, want in_memory_metrics", m.PartName())
	}
}

// --- Integration test: concurrency safety ---

func TestConcurrencySafety(t *testing.T) {
	m := NewInMemoryMetricsCollector()

	const n = 5000
	const dur = 0.001

	done := make(chan struct{}, n)

	for range n {
		go func() {
			m.IncCurrentJobs()
			m.DecCurrentJobs()
			m.IncJobTotal()
			m.ObserveJobDuration(dur)
			done <- struct{}{}
		}()
	}

	for range n {
		<-done
	}

	if m.JobTotal != n {
		t.Fatalf("JobTotal=%d, want %d", m.JobTotal, n)
	}

	if m.TotalDuration.Seconds() != float64(n)*dur {
		t.Fatalf("TotalDuration=%v, want %v", m.TotalDuration.Seconds(), float64(n)*dur)
	}
}
