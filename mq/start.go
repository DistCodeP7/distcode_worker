package mq

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

func StartJobHandlers(ressources MQResources) {
	url := os.Getenv("MQ_URL")

	go func() {
		if err := StartJobConsumer(ressources.AppResources.Ctx, url, ressources.JobsCh); err != nil {
			log.Logger.WithError(err).Error("MQ error")
		}
	}()

	go func() {
		if err := StartJobCanceller(ressources.AppResources.Ctx, url, ressources.CancelJobCh); err != nil {
			log.Logger.WithError(err).Error("MQ error")
		}
	}()

	go func() {
		if err := PublishStreamingEvents(ressources.AppResources.Ctx, url, ressources.ResultsCh); err != nil {
			log.Logger.WithError(err).Fatal("MQ results publisher error")
		}
	}()
}
