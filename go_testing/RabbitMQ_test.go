package go_testing

import (
	"log"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestRabbitMQIntegration(t *testing.T) {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		t.Skip("RabbitMQ not available, skipping integration test")
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"test_queue", // queue name
		false,        // durable
		false,        // delete when unused
		false,        // exclusive
		false,        // no-wait
		nil,          // args
	)
	if err != nil {
		t.Fatal(err)
	}

	body := "integration-test-message"
	err = ch.Publish(
		"",     // exchange
		q.Name, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		})
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(2 * time.Second)

	log.Println("Message sent; manually verify worker processed it.")
}
