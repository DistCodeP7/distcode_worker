package metrics

type JobMetricsCollector interface {
	IncJobTotal()
	IncJobSuccess()
	IncJobFailure()
	IncJobCanceled()
	IncJobTimeout()
	ObserveJobDuration(seconds float64)
	JSON() []byte
}
