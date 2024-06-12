package kefka

import (
	"log/slog"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type DeadLetterOpts struct {
	Producer *Producer
	Topic    string
	Logger   *slog.Logger
}

func DeadLetter(opts DeadLetterOpts) HandlerMiddleware {
	return func(next Handler) Handler {
		return &DeadLetterHandler{
			next: next,
			opts: opts,
		}
	}
}

type DeadLetterHandler struct {
	next Handler
	opts DeadLetterOpts
}

func (d *DeadLetterHandler) Handle(message *kafka.Message) error {
	err := d.next.Handle(message)
	if err != nil {
		// todo: add headers and write to dead letter topic
	}

	return nil
}
