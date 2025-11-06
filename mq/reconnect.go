package mq

import (
	"context"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func ReconnectorRabbitMQ(ctx context.Context, url string, queueName string, setup func(ch *amqp.Channel) error, run func(ch *amqp.Channel) error) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		conn, err := amqp.Dial(url)
		if err != nil {
			log.Printf("Failed to connect to RabbitMQ: %v", err)
			time.Sleep(5 * time.Second)
			continue
		}

		ch, err := conn.Channel()
		if err != nil {
			log.Printf("Failed to open channel: %v", err)
			conn.Close()
			time.Sleep(5 * time.Second)
			continue
		}

		if err := setup(ch); err != nil {
			log.Printf("Setup failed: %v", err)
			ch.Close()
			conn.Close()
			time.Sleep(5 * time.Second)
			continue
		}

		log.Printf("✅ Successfully connected to RabbitMQ queue='%s', ready to run", queueName)
		err = run(ch)

		ch.Close()
		conn.Close()

		if err != nil {
			log.Printf("Queue='%s' connection attempt failed: %v, reconnecting...", queueName, err)
			time.Sleep(5 * time.Second)
			continue
		}

		return nil
	}
}
