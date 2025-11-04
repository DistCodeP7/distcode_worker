package mq

import (
	"context"
	"encoding/json"
	"log"

	"github.com/DistCodeP7/distcode_worker/types"
	amqp "github.com/rabbitmq/amqp091-go"
)

func PublishJobResults(ctx context.Context, results <-chan types.StreamingJobResult) error {
	queueName := "results"
	return reconnectorRabbitMQ(ctx, "amqp://guest:guest@localhost:5672/", queueName,
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
					body, err := json.Marshal(result)
					if err != nil {
						log.Printf("Marshal error: %v", err)
						continue
					}
					err = ch.PublishWithContext(ctx,
						"", "results", false, false,
						amqp.Publishing{
							ContentType: "application/json",
							Body:        body,
						})
					if err != nil {
						return err // triggers reconnect
					}
					log.Printf("Published result for job %d", result.ProblemId)
				case <-ctx.Done():
					return nil
				}
			}
		})
}
