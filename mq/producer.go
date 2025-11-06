package mq

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/DistCodeP7/distcode_worker/types"
	amqp "github.com/rabbitmq/amqp091-go"
)

func Publish[T any](ctx context.Context, data T, queueName string) error {
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

func PublishJobResults(ctx context.Context, results <-chan types.StreamingJobResult) error {
	queueName := "results"
	return ReconnectorRabbitMQ(ctx, "amqp://guest:guest@localhost:5672/", queueName,
		func(ch *amqp.Channel) error {
			_, err := ch.QueueDeclare(queueName, true, false, false, false, nil)
			return err
		},
		func(ch *amqp.Channel) error {
			for {
				select {
				case result, ok := <-results:
					if !ok {
						return nil
					}
					if err := Publish(ctx, result, queueName); err != nil {
						log.Printf("Publish error: %v", err)
						return err // triggers reconnect
					}
					log.Printf("Published result for job %d", result.ProblemId)
				case <-ctx.Done():
					return nil
				}
			}
		})
}

func PublishMetrics(ctx context.Context, metric types.Metric) error {
	queueName := "metrics"
	return ReconnectorRabbitMQ(ctx, "amqp://guest:guest@localhost:5672/", queueName,
		func(ch *amqp.Channel) error {
			_, err := ch.QueueDeclare(queueName, true, false, false, false, nil)
			return err
		},
		func(ch *amqp.Channel) error {
			if err := Publish(ctx, metric, queueName); err != nil {
				log.Printf("Publish error: %v", err)
				return err
			}
			log.Printf("Published metric for job %s", metric.JobID)
			return nil
		})
}