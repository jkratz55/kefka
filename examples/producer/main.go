package main

import (
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"

	"github.com/jkratz55/kefka/v2"
)

func main() {

	leveler := new(slog.LevelVar)
	leveler.Set(slog.LevelDebug)
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		AddSource: true,
		Level:     leveler,
	}))

	conf := kefka.Config{
		BootstrapServers: []string{"localhost:9092"},
		SecurityProtocol: kefka.Plaintext,
		RequiredAcks:     kefka.AckAll,
		Idempotence:      false,
		Logger:           logger,
		OnError: func(err error) {
			logger.Error(err.Error())
		},
	}

	producer, err := kefka.NewProducer(conf)
	if err != nil {
		logger.Error("Failed to initialize Kafka Producer",
			slog.String("err", err.Error()))
		os.Exit(1)
	}

	for i := 0; i < 1000; i++ {
		key := uuid.New().String()
		err := producer.M().
			Topic("test").
			Key(key).
			JSON(map[string]interface{}{
				"event": "CREATE_USER",
			}).
			SendAndWait()
		if err != nil {
			logger.Error("Failed to send message",
				slog.String("err", err.Error()))
		}
	}

	for i := 0; i < 100000; i++ {
		key := uuid.New().String()
		msg, _ := producer.M().
			Topic("test").
			Key(key).
			JSON(map[string]interface{}{
				"event": "CREATE_USER",
			}).Message()
		if err := produceMessage(producer, msg); err != nil {
			logger.Error("Failed to send message",
				slog.String("err", err.Error()))
		}
	}

	for i := 0; i < 10000000; i++ {
		key := uuid.New().String()
		err := producer.M().
			Topic("test").
			Key(key).
			JSON(map[string]interface{}{
				"event": "CREATE_USER",
			}).
			Send(nil)
		if err != nil {
			logger.Error("Failed to send message",
				slog.String("err", err.Error()))
		}
	}

	for x := producer.Flush(10 * time.Second); x > 0; x = producer.Flush(10 * time.Second) {
		logger.Debug("Waiting for messages to be sent",
			slog.Int("count", x))
	}
	producer.Close()
	time.Sleep(5 * time.Second)
}

// produceMessage is a helper function to produce a message to Kafka with retries
// when the error returned is retryable. When the error returned is retryable that
// indicates the internal producer queue is full and the message could not be
// enqueued. The producer will attempt to flush the queue to make room and try again.
func produceMessage(producer *kefka.Producer, msg *kafka.Message) error {
	for i := 3; i > 0; i-- {
		err := producer.Produce(msg, nil)
		if err == nil {
			return nil
		}

		if kefka.IsRetryable(err) {
			producer.Flush(time.Second * 1)
		}
	}

	return fmt.Errorf("kafka: failed to enqueue message")
}
