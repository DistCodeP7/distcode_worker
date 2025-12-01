package mq

import (
	"context"
	"encoding/json"

	"github.com/DistCodeP7/distcode_worker/log"
	"github.com/DistCodeP7/distcode_worker/types"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Define the event types.
type EventType string

const (
	EventTypeResults EventType = "results"
)

// PublishStreamingEvents establishes a long-lived connection and channel to RabbitMQ
// and continuously publishes events received from the 'events' channel.
// It uses ReconnectorRabbitMQ to automatically handle reconnections on failure.
func PublishStreamingEvents(ctx context.Context, eventType EventType, events <-chan types.StreamingJobEvent) error {
	queueName := string(eventType)
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
						// The incoming events channel was closed, gracefully exit the run function.
						return nil
					}

					// 1. Marshal the event data
					body, err := json.Marshal(event)
					if err != nil {
						// This is a local error (bad struct), log it and skip the message.
						// Do not return an error to avoid unnecessary reconnecting.
						log.Logger.WithError(err).Error("Marshal error for job event")
						continue
					}

					// 2. Publish the message using the long-lived channel
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
						// This is a network error or channel error (like connection closed).
						// Return the error to trigger the ReconnectorRabbitMQ loop to reconnect.
						log.Logger.WithError(err).Error("Publish error, triggering reconnect")
						return err
					}

					log.Logger.Tracef("Published %s for job %s", eventType, event.JobUID.String())

				case <-ctx.Done():
					return nil
				}
			}
		})
}
