package reciever

import (
	"context"
	"encoding/json"
	"log"

	"github.com/DistCodeP7/distcode_worker/types"
	amqp "github.com/rabbitmq/amqp091-go"
)

func InitMessageQueue(ctx context.Context, jobs chan<- types.JobRequest) error {
	connection, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		return err
	}
	defer connection.Close()

	channel, err := connection.Channel()
	if err != nil {
		return err
	}
	defer channel.Close()

	queue, err := channel.QueueDeclare("jobs", true, false, false, false, nil)
	if err != nil {
		return err
	}

	messageChannel, err := channel.Consume(
		queue.Name, // queue
		"",         // consumer
		true,       // auto-ack
		false,      // exclusive
		false,      // no-local
		false,      // no-wait
		nil,        // args
	)
	if err != nil {
		return err
	}

	log.Println("Successfully connected to RabbitMQ. Waiting for messages from MQ...")

	for {
		select {
		case d := <-messageChannel:
			handleDelivery(d, jobs)
		case <-ctx.Done():
			log.Println("Shutting down MQ listener...")
			close(jobs)
			return nil
		}
	}
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
