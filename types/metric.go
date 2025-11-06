package types

import "time"

type Metric struct {
    WorkerID  string        `json:"worker_id"`
    JobID     string        `json:"job_id"`
    StartTime time.Time     `json:"start_time"`
    EndTime   time.Time     `json:"end_time"`
    DeltaTime time.Duration `json:"delta_time"`
}
