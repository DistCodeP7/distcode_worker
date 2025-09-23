package reciever

import (
	"context"
	"encoding/json"
	"log"

	"github.com/DistCodeP7/distcode_worker/types"
	amqp "github.com/rabbitmq/amqp091-go"
)

func InitMessageQueue(ctx context.Context, jobs chan<- types.JobRequest) error {
	connnection, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		return err
	}
	defer connnection.Close()

	channel, err := connnection.Channel()
	if err != nil {
		return err
	}
	defer channel.Close()

	queue, err := channel.QueueDeclare("jobs", true, false, false, false, nil)
	if err != nil {
		return err
	}

	message_channel, err := channel.Consume(queue.Name, "", true, false, false, false, nil)
	if err != nil {
		return err
	}

	log.Println("Successfully connected to RabbitMQ. Waiting for messages from MQ...")

	for {
		select {
		case d := <-message_channel:
			if d.Body == nil {
				continue
			}
			var job types.JobRequest
			if err := json.Unmarshal(d.Body, &job); err != nil {
				log.Printf("Invalid job message: %s", d.Body)
				continue
			}
			jobs <- job
			log.Printf("Received job %d from MQ", job.ProblemId)
		case <-ctx.Done():
			log.Println("Shutting down MQ listener...")
			close(jobs)
			return nil
		}
	}
}
