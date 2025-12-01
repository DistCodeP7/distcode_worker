package mq

import (
	"testing"

	"github.com/DistCodeP7/distcode_worker/types"
	amqp "github.com/rabbitmq/amqp091-go"
)

func TestHandleDelivery_InvalidJSON(t *testing.T) {
	jobs := make(chan types.Job, 1)

	d := amqp.Delivery{Body: []byte(`not json`)}
	handleDelivery(d, jobs)

	select {
	case <-jobs:
		t.Error("expected no job for invalid JSON, but got one")
	default:
		// pass
	}
}

func TestHandleDelivery_NilBody(t *testing.T) {
	jobs := make(chan types.Job, 1)

	d := amqp.Delivery{Body: nil}
	handleDelivery(d, jobs)

	select {
	case <-jobs:
		t.Error("expected no job for nil body, but got one")
	default:
		// pass
	}
}

func TestHandleDelivery_EmptyBody(t *testing.T) {
	jobs := make(chan types.Job, 1)

	d := amqp.Delivery{Body: []byte(``)}
	handleDelivery(d, jobs)

	select {
	case <-jobs:
		t.Error("expected no job for empty body, but got one")
	default:
		// pass
	}
}
