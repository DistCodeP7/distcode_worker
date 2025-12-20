package rabbit

import (
	"context"
	"time"

	"github.com/DistCodeP7/distcode_worker/log"

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
			log.Logger.WithError(err).Error("Failed to connect to RabbitMQ instance")
			time.Sleep(5 * time.Second)
			continue
		}

		ch, err := conn.Channel()
		if err != nil {
			log.Logger.WithError(err).Error("Failed to open channel")
			conn.Close()
			time.Sleep(5 * time.Second)
			continue
		}

		if err := setup(ch); err != nil {
			log.Logger.WithError(err).Error("Setup failed")
			ch.Close()
			conn.Close()
			time.Sleep(5 * time.Second)
			continue
		}

		log.Logger.Infof("Successfully connected to RabbitMQ queue='%s', ready to run", queueName)
		err = run(ch)

		ch.Close()
		conn.Close()

		if err != nil {
			log.Logger.Warnf("Queue='%s' connection attempt failed: %v, reconnecting...", queueName, err)
			time.Sleep(5 * time.Second)
			continue
		}

		return nil
	}
}
