package mq

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/DistCodeP7/distcode_worker/log"
	"github.com/DistCodeP7/distcode_worker/types"
	amqp "github.com/rabbitmq/amqp091-go"
)

func StartJobConsumer(ctx context.Context, jobs chan<- types.JobRequest) error {
	queueName := "jobs"
	return ReconnectorRabbitMQ(ctx, "amqp://guest:guest@localhost:5672/", queueName,
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

func StartJobCanceller(ctx context.Context, jobs chan<- types.CancelJobRequest) error {
	queueName := "jobs_cancel"
	return ReconnectorRabbitMQ(ctx, "amqp://guest:guest@localhost:5672/", queueName,
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

func handleDelivery[T any](d amqp.Delivery, out chan<- T) {
	if d.Body == nil {
		return
	}

	var msg T
	if err := json.Unmarshal(d.Body, &msg); err != nil {
		log.Logger.WithError(err).Error("Failed to unmarshal MQ message")
		return
	}

	out <- msg

	switch any(msg).(type) {
	case types.JobRequest:
		log.Logger.Trace("Received job from MQ")
	case types.CancelJobRequest:
		log.Logger.Trace("Received job cancellation from MQ")
	default:
		log.Logger.Warnf("Received %T from MQ", msg)
	}
}
