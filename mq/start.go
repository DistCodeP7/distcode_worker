package mq

import (
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
	go func() {
		if err := StartJobConsumer(ressources.AppResources.Ctx, ressources.JobsCh); err != nil {
			log.Logger.WithError(err).Error("MQ error")
		}
	}()

	go func() {
		if err := StartJobCanceller(ressources.AppResources.Ctx, ressources.CancelJobCh); err != nil {
			log.Logger.WithError(err).Error("MQ error")
		}
	}()

	go func() {
		if err := PublishStreamingEvents(ressources.AppResources.Ctx, ressources.ResultsCh); err != nil {
			log.Logger.WithError(err).Fatal("MQ results publisher error")
		}
	}()
}
