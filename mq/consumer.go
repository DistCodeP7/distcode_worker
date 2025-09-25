package mq

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/DistCodeP7/distcode_worker/types"
	amqp "github.com/rabbitmq/amqp091-go"
)

func StartJobConsumer(ctx context.Context, jobs chan<- types.JobRequest) error {
	queueName := "jobs"
	return reconnectorRabbitMQ(ctx, "amqp://guest:guest@localhost:5672/", queueName,
		func(ch *amqp.Channel) error {
			_, err := ch.QueueDeclare(queueName, true, false, false, false, nil)
			return err
		},
		func(ch *amqp.Channel) error {
			msgs, err := ch.Consume(queueName, "", true, false, false, false, nil)
			if err != nil {
				return err
			}
			for {
				select {
				case d, ok := <-msgs:
					if !ok {
						return fmt.Errorf("message channel closed")
					}
					handleDelivery(d, jobs)
				case <-ctx.Done():
					close(jobs)
					return nil
				}
			}
		})
}

func handleDelivery(d amqp.Delivery, jobs chan<- types.JobRequest) {
	if d.Body == nil {
		return
	}
	var job types.JobRequest
	if err := json.Unmarshal(d.Body, &job); err != nil {
		log.Printf("Invalid job message: %s", d.Body)
		return
	}
	jobs <- job
	log.Printf("Received job %d from MQ", job.ProblemId)
}
