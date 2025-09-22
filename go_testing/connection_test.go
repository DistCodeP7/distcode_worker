package go_testing

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "github.com/lib/pq"
	amqp "github.com/rabbitmq/amqp091-go"
)

func TestConnections(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Test database connection
	t.Run("database connection", func(t *testing.T) {
		db, err := sql.Open("postgres", "postgres://username:password@localhost:5432/testdb?sslmode=disable")
		if err != nil {
			t.Fatal("Failed to create database connection:", err)
		}
		defer db.Close()

		err = db.PingContext(ctx)
		if err != nil {
			t.Skip("Database connection failed:", err)
		}
	})

	// Test RabbitMQ connection
	t.Run("rabbitmq connection", func(t *testing.T) {
		conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
		if err != nil {
			t.Skip("RabbitMQ connection failed:", err)
		}
		if conn != nil {
			defer conn.Close()

			ch, err := conn.Channel()
			if err != nil {
				t.Error("Failed to open channel:", err)
			}
			if ch != nil {
				defer ch.Close()
			}
		}
	})
}
