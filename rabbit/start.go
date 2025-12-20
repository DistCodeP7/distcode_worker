package rabbit

import (
	"os"

	"github.com/DistCodeP7/distcode_worker/log"
	"github.com/DistCodeP7/distcode_worker/setup"
	"github.com/DistCodeP7/distcode_worker/types"
)

type MQResources struct {
	AppResources *setup.AppResources
	JobsCh       chan types.Job
	CancelJobCh  chan types.CancelJobRequest
	ResultsCh    chan types.StreamingJobEvent
}

func StartJobHandlers(resources MQResources) {
	url := os.Getenv("MQ_URL")

	go func() {
		if err := StartJobConsumer(resources.AppResources.Ctx, url, resources.JobsCh); err != nil {
			log.Logger.WithError(err).Error("MQ error")
		}
	}()

	go func() {
		if err := StartJobCanceller(resources.AppResources.Ctx, url, resources.CancelJobCh); err != nil {
			log.Logger.WithError(err).Error("MQ error")
		}
	}()

	go func() {
		if err := PublishStreamingEvents(resources.AppResources.Ctx, url, resources.ResultsCh); err != nil {
			log.Logger.WithError(err).Fatal("MQ results publisher error")
		}
	}()
}
