package rabbit

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/DistCodeP7/distcode_worker/log"
	"github.com/DistCodeP7/distcode_worker/types"
	amqp "github.com/rabbitmq/amqp091-go"
)

const cancelExchange = "jobs.cancel"

func StartJobCanceller(
	ctx context.Context,
	url string,
	cancellations chan<- types.CancelJobRequest,
) error {
	return ReconnectorRabbitMQ(
		ctx,
		url,
		cancelExchange,
		func(ch *amqp.Channel) error {
			return ch.ExchangeDeclare(
				cancelExchange,
				"fanout",
				true,
				false,
				false,
				false,
				nil,
			)
		},
		func(ch *amqp.Channel) error {
			q, err := ch.QueueDeclare(
				"",
				false,
				true,
				true,
				false,
				nil,
			)
			if err != nil {
				return err
			}

			if err := ch.QueueBind(
				q.Name,
				"",
				cancelExchange,
				false,
				nil,
			); err != nil {
				return err
			}

			messages, err := ch.Consume(
				q.Name,
				"",
				true,
				false,
				false,
				false,
				nil,
			)
			if err != nil {
				return err
			}

			log.Logger.Infof("Listening for cancellations on exchange: %s", cancelExchange)
			for {
				select {
				case d, ok := <-messages:
					if !ok {
						return fmt.Errorf("cancellation channel closed")
					}

					var cancel types.CancelJobRequest
					if err := json.Unmarshal(d.Body, &cancel); err != nil {
						log.Logger.WithError(err).
							Error("Failed to unmarshal cancellation message")
						continue
					}
					cancellations <- cancel
					log.Logger.Trace("Received job cancellation broadcast")

				case <-ctx.Done():
					close(cancellations)
					return nil
				}
			}
		},
	)
}
