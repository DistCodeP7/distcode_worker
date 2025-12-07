package mq

import (
	"context"
	"encoding/json"

	"github.com/DistCodeP7/distcode_worker/log"
	"github.com/DistCodeP7/distcode_worker/types"
	amqp "github.com/rabbitmq/amqp091-go"
)

// PublishStreamingEvents establishes a long-lived connection and channel to RabbitMQ
// and continuously publishes events received from the 'events' channel.
// It uses ReconnectorRabbitMQ to automatically handle reconnections on failure.
func PublishStreamingEvents(ctx context.Context, events <-chan types.StreamingJobEvent) error {
	queueName := "results"
	url := "amqp://guest:guest@localhost:5672/"

	return ReconnectorRabbitMQ(ctx, url, queueName,
		// Setup function: runs once per successful connection to declare the queue.
		func(ch *amqp.Channel) error {
			_, err := ch.QueueDeclare(queueName, true, false, false, false, nil)
			return err
		},
		// Run function: runs as a continuous loop, publishing events on the established channel.
		func(ch *amqp.Channel) error {
			for {
				select {
				case event, ok := <-events:
					if !ok {
						return nil
					}

					body, err := json.Marshal(event)
					if err != nil {
						log.Logger.WithError(err).Error("Marshal error for job event")
						continue
					}

					err = ch.PublishWithContext(ctx,
						"",        // exchange
						queueName, // routing key
						false,     // mandatory
						false,     // immediate
						amqp.Publishing{
							ContentType: "application/json",
							Body:        body,
						})

					if err != nil {
						log.Logger.WithError(err).Error("Publish error, triggering reconnect")
						return err
					}

				case <-ctx.Done():
					return nil
				}
			}
		})
}
