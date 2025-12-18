package metrics

import (
	"time"

	"github.com/DistCodeP7/distcode_worker/types"
)

type JobMetricsCollector interface {
	JSON() []byte
	StartJob() func()
	IncJobOutcome(outcome types.Outcome)
	TrackTimeSpent(timeSpent map[types.Phase]time.Duration)
}
