package kefka

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// Handler is a type that handles processing messages consumed from Kafka.
//
// Implementations of Handler are expected to encapsulate all the logic required
// to process a message from Kafka. If the handler fails to process the message
// it should return a non-nil error to indicate that the message was not successfully
// processed. A nil error value should be interpreted as a handler successfully
// processed the message.
type Handler interface {
	Handle(msg *kafka.Message) error
}

type HandlerFunc func(msg *kafka.Message) error

func (f HandlerFunc) Handle(msg *kafka.Message) error {
	return f(msg)
}

type HandlerMiddleware func(h Handler) Handler

func ChainHandlers(hs ...HandlerMiddleware) HandlerMiddleware {
	return func(next Handler) Handler {
		for i := len(hs) - 1; i >= 0; i-- {
			next = hs[i](next)
		}
		return next
	}
}
