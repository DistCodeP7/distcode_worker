package rabbit

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/DistCodeP7/distcode_worker/log"
	"github.com/DistCodeP7/distcode_worker/mapper"
	"github.com/DistCodeP7/distcode_worker/types"
	amqp "github.com/rabbitmq/amqp091-go"
)

const jobsQueue = "jobs"

func StartJobConsumer(
	ctx context.Context,
	url string,
	jobs chan<- types.Job,
) error {
	return ReconnectorRabbitMQ(ctx, url, jobsQueue,
		func(ch *amqp.Channel) error {
			_, err := ch.QueueDeclare(jobsQueue, true, false, false, false, nil)
			return err
		},
		func(ch *amqp.Channel) error {
			if err := ch.Qos(1, 0, false); err != nil {
				return err
			}

			msgs, err := ch.Consume(jobsQueue, "", false, false, false, false, nil)
			if err != nil {
				return err
			}

			for {
				select {
				case d, ok := <-msgs:
					if !ok {
						return fmt.Errorf("message channel closed")
					}

					processDelivery(d, jobs)
					if err := d.Ack(false); err != nil {
						log.Logger.WithError(err).Error("Failed to ack message")
					}

				case <-ctx.Done():
					close(jobs)
					return nil
				}
			}
		})
}

// Helper function to handle parsing and blocking send
func processDelivery(d amqp.Delivery, jobs chan<- types.Job) {
	if d.Body == nil {
		return
	}

	var req types.JobRequest
	if err := json.Unmarshal(d.Body, &req); err != nil {
		log.Logger.WithError(err).Error("Failed to unmarshal MQ message")
		return
	}

	job, err := mapper.ConvertToJobRequest(&req)
	if err != nil {
		log.Logger.WithError(err).Error("Failed to convert JobRequest to Job")
		return
	}

	jobs <- *job
}
