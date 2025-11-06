package metric

import (
	"context"
	"time"

	"github.com/DistCodeP7/distcode_worker/mq"
	"github.com/DistCodeP7/distcode_worker/types"
)

func RecordMetric(ctx context.Context, workerID string, jobID string, startT time.Time, endT time.Time) error {
	deltaT := endT.Sub(startT)

	m := &types.Metric{
		WorkerID:  workerID,
		JobID:     jobID,
		StartTime: startT,
		EndTime:   endT,
		DeltaTime: deltaT,
	}

	return mq.Publish(ctx, m, "metrics")
}