package mq

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/DistCodeP7/distcode_worker/types"
	amqp "github.com/rabbitmq/amqp091-go"
)

func Publish(ctx context.Context, data types.StreamingJobEvent, queueName string) error {
	return ReconnectorRabbitMQ(ctx, "amqp://guest:guest@localhost:5672/", queueName,
		func(ch *amqp.Channel) error {
			_, err := ch.QueueDeclare(queueName, true, false, false, false, nil)
			return err
		},
		func(ch *amqp.Channel) error {
			body, err := json.Marshal(data)
			if err != nil {
				return fmt.Errorf("marshal error: %v", err)
			}
			return ch.PublishWithContext(ctx,
				"", queueName, false, false,
				amqp.Publishing{
					ContentType: "application/json",
					Body:        body,
				})
		})
}

type EventType string

const (
	EventTypeResults EventType = "results"
	EventTypeMetrics EventType = "metrics"
)

func PublishStreamingEvents(ctx context.Context, eventType EventType, events <-chan types.StreamingJobEvent) error {
	queueName := string(eventType)
	return ReconnectorRabbitMQ(ctx, "amqp://guest:guest@localhost:5672/", queueName,
		func(ch *amqp.Channel) error {
			_, err := ch.QueueDeclare(queueName, true, false, false, false, nil)
			return err
		},
		func(ch *amqp.Channel) error {
			for {
				select {
				case event, ok := <-events:
					if !ok {
						return nil
					}
					if err := Publish(ctx, event, queueName); err != nil {
						log.Printf("Publish error: %v", err)
						return err // triggers reconnect
					}
					switch eventType {
					case EventTypeResults:
						log.Printf("Published result for job %d", event.ProblemId)
					case EventTypeMetrics:
						log.Printf("Published metric for job %d", event.ProblemId)
					}
				case <-ctx.Done():
					return nil
				}
			}
		})
}
