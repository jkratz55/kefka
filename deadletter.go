package kefka

import (
	"fmt"
	"log/slog"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type DeadLetterOpts struct {
	Producer       *Producer
	Topic          string
	Logger         *slog.Logger
	OnPublishError func(msg *kafka.Message, err error)
}

func DeadLetter(opts DeadLetterOpts) HandlerMiddleware {
	if opts.Producer == nil {
		panic("DeadLetterOpts.Producer is required")
	}
	if opts.Topic == "" {
		panic("DeadLetterOpts.Topic is required")
	}
	if opts.Logger == nil {
		opts.Logger = DefaultLogger()
	}

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
		d.opts.Logger.Info("Failed to process Kafka message :: DeadLetterHandler will attempt to publish message to configured dead letter topic",
			slog.String("err", err.Error()),
			slog.Group("kafkaMessage",
				slog.String("topic", *message.TopicPartition.Topic),
				slog.Int("partition", int(message.TopicPartition.Partition)),
				slog.Int64("offset", int64(message.TopicPartition.Offset)),
				slog.String("key", string(message.Key)),
				slog.Any("headers", message.Headers)))

		dlMessage := d.opts.Producer.M().
			Topic(d.opts.Topic).
			Key(string(message.Key)).
			Value(message.Value)

		for _, header := range message.Headers {
			dlMessage.Header(header.Key, header.Value)
		}

		// Add headers containing metadata about the original message
		dlMessage.Header("kefka-original-topic", []byte(*message.TopicPartition.Topic))
		dlMessage.Header("kefka-original-partition", []byte(fmt.Sprintf("%d", message.TopicPartition.Partition)))
		dlMessage.Header("kefka-original-offset", []byte(fmt.Sprintf("%d", message.TopicPartition.Offset)))
		dlMessage.Header("kefka-original-timestamp", []byte(fmt.Sprintf("%d", message.Timestamp.Unix())))
		dlMessage.Header("kefka-err", []byte(err.Error()))

		err = dlMessage.SendAndWait()
		if err != nil {
			d.opts.Logger.Error("Failed to publish message to dead letter topic",
				slog.String("err", err.Error()),
				slog.Group("originalMessage",
					slog.String("topic", *message.TopicPartition.Topic),
					slog.Int("partition", int(message.TopicPartition.Partition)),
					slog.Int64("offset", int64(message.TopicPartition.Offset)),
					slog.String("key", string(message.Key)),
					slog.Any("headers", message.Headers)),
				slog.Group("deadLetterMessage",
					slog.String("topic", dlMessage.topic),
					slog.String("key", string(message.Key)),
					slog.Any("headers", message.Headers)))

			// If publishing to the dead letter topic fails, the OnPublishError
			// callback will be invoked if it is set to give the application a
			// chance to store the message to disk, db, or something else.
			if d.opts.OnPublishError != nil {
				d.opts.OnPublishError(message, err)
			}
			return fmt.Errorf("failed to publish message to dead letter topic: %w", err)
		} else {
			d.opts.Logger.Info("Successfully published message to dead letter topic",
				slog.Group("deadLetterMessage",
					slog.String("topic", dlMessage.topic),
					slog.String("key", string(message.Key)),
					slog.Any("headers", message.Headers)))
		}
	}

	return nil
}
