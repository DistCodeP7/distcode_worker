package reciever

import (
	"context"
	"encoding/json"
	"log"

	"github.com/DistCodeP7/distcode_worker/types"
	amqp "github.com/rabbitmq/amqp091-go"
)

func MQ(ctx context.Context, jobs chan<- types.Job) error {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		return err
	}
	// Keep connection open until context is done
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	q, err := ch.QueueDeclare("jobs", true, false, false, false, nil)
	if err != nil {
		return err
	}

	msgs, err := ch.Consume(q.Name, "", true, false, false, false, nil)
	if err != nil {
		return err
	}

	log.Println("Waiting for messages from MQ...")

	for {
		select {
		case d := <-msgs:
			if d.Body == nil {
				continue
			}
			var job types.Job
			if err := json.Unmarshal(d.Body, &job); err != nil {
				log.Printf("Invalid job message: %s", d.Body)
				continue
			}
			jobs <- job
			log.Printf("Received job %d from MQ", job.ID)
		case <-ctx.Done():
			log.Println("Shutting down MQ listener...")
			return nil
		}
	}
}
