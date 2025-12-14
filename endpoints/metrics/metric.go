package metrics

import "github.com/DistCodeP7/distcode_worker/types"

type JobMetricsCollector interface {
	IncJobTotal()
	IncJobOutcome(outcome types.Outcome)
	IncCurrentJobs()
	DecCurrentJobs()
	ObserveJobDuration(seconds float64)
	JSON() []byte
}
