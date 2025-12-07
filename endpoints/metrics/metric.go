package metrics

type JobMetricsCollector interface {
	IncJobTotal()
	IncJobSuccess()
	IncJobFailure()
	IncJobCanceled()
	IncJobTimeout()
	IncCurrentJobs()
	DecCurrentJobs()
	ObserveJobDuration(seconds float64)
	JSON() []byte
}
