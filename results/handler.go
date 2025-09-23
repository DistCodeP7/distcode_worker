package results

import (
	"context"
	"encoding/json"
	"log"

	"github.com/DistCodeP7/distcode_worker/types"
	amqp "github.com/rabbitmq/amqp091-go"
)

func Handle(ctx context.Context, results <-chan types.JobResult) {
	// Connect to RabbitMQ
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open a channel: %v", err)
	}
	defer ch.Close()

	queue, err := ch.QueueDeclare(
		"results", // results queue
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("Failed to declare results queue: %v", err)
	}

	log.Println("Results publisher ready. Waiting for worker results...")

	for {
		select {
		case result, ok := <-results:
			if !ok {
				log.Println("Results channel closed. Exiting results handler.")
				return
			}

			// Marshal result to JSON
			body, err := json.Marshal(result)
			if err != nil {
				log.Printf("Failed to marshal result: %v", err)
				continue
			}

			// Publish to MQ
			err = ch.PublishWithContext(ctx,
				"",         // default exchange
				queue.Name, // routing key
				false,      // mandatory
				false,      // immediate
				amqp.Publishing{
					ContentType: "application/json",
					Body:        body,
				})
			if err != nil {
				log.Printf("Failed to publish result: %v", err)
			} else {
				log.Printf("Published result for job %d", result.JobId)
			}
		case <-ctx.Done():
			log.Println("Context cancelled. Stopping results handler...")
			return
		}
	}
}
