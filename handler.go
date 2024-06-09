package kefka

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// Handler is a type that handles and processes messages consumed from Kafka.
type Handler interface {
	Handle(msg *kafka.Message) error
}

type HandlerFunc func(msg *kafka.Message) error

func (f HandlerFunc) Handle(msg *kafka.Message) error {
	return f(msg)
}
