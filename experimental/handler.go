package experimental

import (
	"log/slog"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/pkg/errors"
)

type Handler interface {
	ProcessMessage(msg *kafka.Message) error
}

type HandlerFunc func(msg *kafka.Message) error

func (f HandlerFunc) ProcessMessage(msg *kafka.Message) error {
	return f(msg)
}

type HandlerMiddleware func(Handler) Handler

func ChainHandlerMiddlewares(hs ...HandlerMiddleware) HandlerMiddleware {
	return func(next Handler) Handler {
		for i := len(hs) - 1; i >= 0; i-- {
			next = hs[i](next)
		}
		return next
	}
}

// RetryHandler returns a HandlerMiddleware that retries processing a message from
// Kafka up to the specified max attempts. RetryHandler uses an exponential backoff
// between retries starting with the initial delay and increasing up to the max delay.
//
// Take note that while the message processing is being retried no other messages will
// be processed. This is because the message is not acknowledged until the handler
// returns to the Consumer or Reader and ordering continuing to process messages can
// impact ordering guarantees. It's recommended to chose sane values for maxAttempts,
// initDelay, and maxDelay to avoid a situation where messages are being consumed slowly.
//
// Providing a maxAttempts value less than 1 will panic. Providing an initDelay value
// greater than maxDelay will panic.
func RetryHandler(maxAttempts int, initDelay, maxDelay time.Duration) HandlerMiddleware {
	if maxAttempts < 1 {
		panic("maxAttempts must be greater than zero")
	}
	if initDelay <= 0 {
		panic("initDelay must be greater than zero")
	}
	if initDelay > maxDelay {
		panic("initDelay must be less than or equal to maxDelay")
	}
	return func(next Handler) Handler {
		return HandlerFunc(func(msg *kafka.Message) error {
			err := next.ProcessMessage(msg)
			if err == nil {
				return nil
			}
			attempts := 1
			backoff := ExponentialBackoff(initDelay, maxDelay)
			time.Sleep(backoff.Next())

			for err != nil && attempts <= maxAttempts {
				err = next.ProcessMessage(msg)
				attempts++
				time.Sleep(backoff.Next())
			}

			if attempts >= maxAttempts {
				err = errors.Wrap(err, "failed to process Kafka message: max reties/attempts reached")
			}

			return err
		})
	}
}

type HandlerInstrumenter interface {
	Observe(topic string, dur time.Duration, err error)
}

func InstrumentHandler(is HandlerInstrumenter) HandlerMiddleware {
	if is == nil {
		panic("valid non-nil HandlerInstrumenter required")
	}
	return func(next Handler) Handler {
		return HandlerFunc(func(msg *kafka.Message) error {
			start := time.Now()
			err := next.ProcessMessage(msg)
			is.Observe(*msg.TopicPartition.Topic, time.Since(start), err)
			return err
		})
	}
}

type LoggingHandlerConfig struct {
	Logger         *slog.Logger
	IncludeKey     bool
	IncludeValue   bool
	IncludeHeaders bool
}

func LoggingHandler(conf LoggingHandlerConfig) HandlerMiddleware {
	if conf.Logger == nil {
		conf.Logger = defaultLogger()
	}
	return func(next Handler) Handler {
		return HandlerFunc(func(msg *kafka.Message) error {
			attrs := []any{
				slog.String("topic", *msg.TopicPartition.Topic),
				slog.Int("partition", int(msg.TopicPartition.Partition)),
				slog.Int64("offset", int64(msg.TopicPartition.Offset)),
				slog.Time("timestamp", msg.Timestamp),
			}
			if conf.IncludeKey {
				attrs = append(attrs, slog.String("key", string(msg.Key)))
			}
			if conf.IncludeValue {
				attrs = append(attrs, slog.String("value", string(msg.Value)))
			}
			if conf.IncludeHeaders {
				attrs = append(attrs, slog.Any("headers", msg.Headers))
			}

			conf.Logger.Info("processing Kafka message", slog.Group("kafkaMessage", attrs...))
			return next.ProcessMessage(msg)
		})
	}
}

// DeadLetterErrorHandler is a function type that handles failures to produce a
// dead/unprocessed message to a dead letter topic. The DeadLetterErrorHandler is
// invoked by the DeadLetterHandler when a message fails to be produced to the
// dead letter topic.
//
// This allows the application to handle the error according to its own requirements.
// This could include logging, instrumentation, persisting the message to a database,
// or even panicking the application.
type DeadLetterErrorHandler func(msg *kafka.Message, err error)

type DeadLetterConfig struct {
	Producer     *Producer
	Topic        string
	ErrorHandler DeadLetterErrorHandler
}

// todo: implement dead letter publisher to publish messages into a retry topic when they fail
func DeadLetterHandler(conf DeadLetterConfig) HandlerMiddleware {
	return func(next Handler) Handler {
		return HandlerFunc(func(msg *kafka.Message) error {
			err := next.ProcessMessage(msg)
			if err != nil {
				retryMessage := retryableMessage(msg, conf.Topic, err)
				if err := conf.Producer.Produce(retryMessage); err != nil && conf.ErrorHandler != nil {
					conf.ErrorHandler(retryMessage, err)
				}
			}

			return err
		})
	}
}
