package metrics

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/DistCodeP7/distcode_worker/types"
)

func TestTrackTimeSpent_NewPhases(t *testing.T) {
	mc := NewInMemoryMetricsCollector()

	phases := map[types.Phase]time.Duration{
		types.Phase("compile"): 2 * time.Second,
		types.Phase("run"):     3 * time.Second,
	}

	mc.TrackTimeSpent(phases)

	if got := mc.TimeSpentInPhases[("compile")]; got != 2*time.Second {
		t.Errorf("expected compile phase to be 2s, got %v", got)
	}
	if got := mc.TimeSpentInPhases[("run")]; got != 3*time.Second {
		t.Errorf("expected run phase to be 3s, got %v", got)
	}
}

func TestTrackTimeSpent_Accumulates(t *testing.T) {
	mc := NewInMemoryMetricsCollector()

	mc.TrackTimeSpent(map[types.Phase]time.Duration{
		"compile": 1 * time.Second,
	})
	mc.TrackTimeSpent(map[types.Phase]time.Duration{
		"compile": 4 * time.Second,
	})

	if got := mc.TimeSpentInPhases[("compile")]; got != 5*time.Second {
		t.Errorf("expected compile phase to accumulate to 5s, got %v", got)
	}
}

func TestTrackTimeSpent_EmptyInput(t *testing.T) {
	mc := NewInMemoryMetricsCollector()
	mc.TrackTimeSpent(map[types.Phase]time.Duration{})

	if len(mc.TimeSpentInPhases) != 0 {
		t.Errorf("expected no phases to be tracked, got %v", mc.TimeSpentInPhases)
	}
}
func TestIncJobOutcome_Success(t *testing.T) {
	mc := NewInMemoryMetricsCollector()
	mc.IncJobOutcome(types.OutcomeSuccess)
	if mc.JobSuccess != 1 {
		t.Errorf("expected JobSuccess to be 1, got %d", mc.JobSuccess)
	}
	if mc.JobFailure != 0 || mc.JobCanceled != 0 || mc.JobTimeout != 0 {
		t.Errorf("unexpected increment in other outcome counters")
	}
}

func TestIncJobOutcome_Failed(t *testing.T) {
	mc := NewInMemoryMetricsCollector()
	mc.IncJobOutcome(types.OutcomeFailed)
	if mc.JobFailure != 1 {
		t.Errorf("expected JobFailure to be 1, got %d", mc.JobFailure)
	}
	if mc.JobSuccess != 0 || mc.JobCanceled != 0 || mc.JobTimeout != 0 {
		t.Errorf("unexpected increment in other outcome counters")
	}
}

func TestIncJobOutcome_CompilationError(t *testing.T) {
	mc := NewInMemoryMetricsCollector()
	mc.IncJobOutcome(types.OutcomeCompilationError)
	if mc.JobFailure != 1 {
		t.Errorf("expected JobFailure to be 1 for compilation error, got %d", mc.JobFailure)
	}
	if mc.JobSuccess != 0 || mc.JobCanceled != 0 || mc.JobTimeout != 0 {
		t.Errorf("unexpected increment in other outcome counters")
	}
}

func TestIncJobOutcome_Canceled(t *testing.T) {
	mc := NewInMemoryMetricsCollector()
	mc.IncJobOutcome(types.OutcomeCanceled)
	if mc.JobCanceled != 1 {
		t.Errorf("expected JobCanceled to be 1, got %d", mc.JobCanceled)
	}
	if mc.JobSuccess != 0 || mc.JobFailure != 0 || mc.JobTimeout != 0 {
		t.Errorf("unexpected increment in other outcome counters")
	}
}

func TestIncJobOutcome_Timeout(t *testing.T) {
	mc := NewInMemoryMetricsCollector()
	mc.IncJobOutcome(types.OutcomeTimeout)
	if mc.JobTimeout != 1 {
		t.Errorf("expected JobTimeout to be 1, got %d", mc.JobTimeout)
	}
	if mc.JobSuccess != 0 || mc.JobFailure != 0 || mc.JobCanceled != 0 {
		t.Errorf("unexpected increment in other outcome counters")
	}
}

func TestIncJobOutcome_UnknownOutcome(t *testing.T) {
	mc := NewInMemoryMetricsCollector()
	// Use an outcome not covered by the switch
	mc.IncJobOutcome("unknown")
	if mc.JobSuccess != 0 || mc.JobFailure != 0 || mc.JobCanceled != 0 || mc.JobTimeout != 0 {
		t.Errorf("no counters should be incremented for unknown outcome")
	}
}
func TestStartJob_IncrementsAndDecrementsCounters(t *testing.T) {
	mc := NewInMemoryMetricsCollector()

	endJob := mc.StartJob()

	if mc.JobTotal != 1 {
		t.Errorf("expected JobTotal to be 1 after StartJob, got %d", mc.JobTotal)
	}
	if mc.CurrentJobs != 1 {
		t.Errorf("expected CurrentJobs to be 1 after StartJob, got %d", mc.CurrentJobs)
	}

	// Simulate job completion
	endJob()

	if mc.CurrentJobs != 0 {
		t.Errorf("expected CurrentJobs to be 0 after job ends, got %d", mc.CurrentJobs)
	}
	if mc.JobTotal != 1 {
		t.Errorf("expected JobTotal to remain 1 after job ends, got %d", mc.JobTotal)
	}
}

func TestStartJob_AccumulatesTotalDuration(t *testing.T) {
	mc := NewInMemoryMetricsCollector()

	endJob := mc.StartJob()
	// Simulate some work
	time.Sleep(10 * time.Millisecond)
	endJob()

	if mc.TotalDuration < 10*time.Millisecond {
		t.Errorf("expected TotalDuration to be at least 10ms, got %v", mc.TotalDuration)
	}
}

func TestStartJob_MultipleJobs(t *testing.T) {
	mc := NewInMemoryMetricsCollector()

	endJob1 := mc.StartJob()
	endJob2 := mc.StartJob()

	if mc.JobTotal != 2 {
		t.Errorf("expected JobTotal to be 2 after starting two jobs, got %d", mc.JobTotal)
	}
	if mc.CurrentJobs != 2 {
		t.Errorf("expected CurrentJobs to be 2 after starting two jobs, got %d", mc.CurrentJobs)
	}

	endJob1()
	if mc.CurrentJobs != 1 {
		t.Errorf("expected CurrentJobs to be 1 after ending one job, got %d", mc.CurrentJobs)
	}
	endJob2()
	if mc.CurrentJobs != 0 {
		t.Errorf("expected CurrentJobs to be 0 after ending both jobs, got %d", mc.CurrentJobs)
	}
}
func TestJSON_EmptyCollector(t *testing.T) {
	mc := NewInMemoryMetricsCollector()
	jsonBytes := mc.JSON()

	var data map[string]interface{}
	if err := json.Unmarshal(jsonBytes, &data); err != nil {
		t.Fatalf("failed to unmarshal JSON: %v", err)
	}

	if data["current_jobs"] != float64(0) {
		t.Errorf("expected current_jobs to be 0, got %v", data["current_jobs"])
	}
	if data["job_total"] != float64(0) {
		t.Errorf("expected job_total to be 0, got %v", data["job_total"])
	}
	if data["job_success"] != float64(0) {
		t.Errorf("expected job_success to be 0, got %v", data["job_success"])
	}
	if data["job_failure"] != float64(0) {
		t.Errorf("expected job_failure to be 0, got %v", data["job_failure"])
	}
	if data["job_canceled"] != float64(0) {
		t.Errorf("expected job_canceled to be 0, got %v", data["job_canceled"])
	}
	if data["job_timeout"] != float64(0) {
		t.Errorf("expected job_timeout to be 0, got %v", data["job_timeout"])
	}
	if data["job_total_seconds"] != float64(0) {
		t.Errorf("expected job_total_seconds to be 0, got %v", data["job_total_seconds"])
	}
	if data["job_avg_seconds"] != float64(0) {
		t.Errorf("expected job_avg_seconds to be 0, got %v", data["job_avg_seconds"])
	}
	if _, ok := data["start_time"].(string); !ok {
		t.Errorf("expected start_time to be a string, got %T", data["start_time"])
	}
	if _, ok := data["uptime_seconds"].(float64); !ok {
		t.Errorf("expected uptime_seconds to be a float64, got %T", data["uptime_seconds"])
	}
	if phases, ok := data["time_spent_in_phases"].(map[string]interface{}); !ok {
		t.Errorf("expected time_spent_in_phases to be a map, got %T", data["time_spent_in_phases"])
	} else if len(phases) != 0 {
		t.Errorf("expected time_spent_in_phases to be empty, got %v", phases)
	}
}

func TestJSON_WithData(t *testing.T) {
	mc := NewInMemoryMetricsCollector()
	mc.JobSuccess = 2
	mc.JobFailure = 1
	mc.JobCanceled = 1
	mc.JobTimeout = 1
	mc.JobTotal = 5
	mc.CurrentJobs = 1
	mc.TotalDuration = 10 * time.Second
	mc.TimeSpentInPhases[("compile")] = 3 * time.Second
	mc.TimeSpentInPhases[("run")] = 7 * time.Second

	jsonBytes := mc.JSON()

	var data map[string]interface{}
	if err := json.Unmarshal(jsonBytes, &data); err != nil {
		t.Fatalf("failed to unmarshal JSON: %v", err)
	}

	if data["current_jobs"] != float64(1) {
		t.Errorf("expected current_jobs to be 1, got %v", data["current_jobs"])
	}
	if data["job_total"] != float64(5) {
		t.Errorf("expected job_total to be 5, got %v", data["job_total"])
	}
	if data["job_success"] != float64(2) {
		t.Errorf("expected job_success to be 2, got %v", data["job_success"])
	}
	if data["job_failure"] != float64(1) {
		t.Errorf("expected job_failure to be 1, got %v", data["job_failure"])
	}
	if data["job_canceled"] != float64(1) {
		t.Errorf("expected job_canceled to be 1, got %v", data["job_canceled"])
	}
	if data["job_timeout"] != float64(1) {
		t.Errorf("expected job_timeout to be 1, got %v", data["job_timeout"])
	}
	if data["job_total_seconds"] != float64(10) {
		t.Errorf("expected job_total_seconds to be 10, got %v", data["job_total_seconds"])
	}
	if data["job_avg_seconds"] != float64(2) {
		t.Errorf("expected job_avg_seconds to be 2, got %v", data["job_avg_seconds"])
	}
	if phases, ok := data["time_spent_in_phases"].(map[string]interface{}); ok {
		if phases["compile"] != float64(3) {
			t.Errorf("expected compile phase to be 3, got %v", phases["compile"])
		}
		if phases["run"] != float64(7) {
			t.Errorf("expected run phase to be 7, got %v", phases["run"])
		}
	} else {
		t.Errorf("expected time_spent_in_phases to be a map, got %T", data["time_spent_in_phases"])
	}
}

func TestJSON_JobAvgSecondsZeroWhenNoJobs(t *testing.T) {
	mc := NewInMemoryMetricsCollector()
	mc.TotalDuration = 10 * time.Second
	// JobTotal is 0
	jsonBytes := mc.JSON()

	var data map[string]interface{}
	if err := json.Unmarshal(jsonBytes, &data); err != nil {
		t.Fatalf("failed to unmarshal JSON: %v", err)
	}

	if data["job_avg_seconds"] != float64(0) {
		t.Errorf("expected job_avg_seconds to be 0 when job_total is 0, got %v", data["job_avg_seconds"])
	}
}
