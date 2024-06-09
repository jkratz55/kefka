package kefka

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type DeadLetterOpts struct {
	Producer *Producer
	Topic    string
}

type DeadLetterHandler struct {
	next Handler
}

func (d *DeadLetterHandler) Handle(message *kafka.Message) error {
	err := d.next.Handle(message)
	if err != nil {
		// todo: add headers and write to dead letter topic
	}

	return nil
}
