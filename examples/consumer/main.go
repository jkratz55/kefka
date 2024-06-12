package main

import (
	"log/slog"
	"os"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"

	"github.com/jkratz55/kefka/v2"
)

func main() {

	leveler := new(slog.LevelVar)
	leveler.Set(slog.LevelDebug)
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		AddSource: true,
		Level:     leveler,
	}))

	conf, err := kefka.LoadConfigFromEnv()
	if err != nil {
		logger.Error("Failed to load config from environment",
			slog.String("err", err.Error()))
		os.Exit(1)
	}
	conf.Logger = logger

	handler := kefka.HandlerFunc(func(msg *kafka.Message) error {
		// logger.Debug("Received message",
		// 	slog.Any("message", msg))
		return nil
	})

	consumer, err := kefka.NewConsumer(conf, handler, "test")
	if err != nil {
		logger.Error("Failed to initialize Kafka Consumer",
			slog.String("err", err.Error()))
		os.Exit(1)
	}

	if err := consumer.Run(); err != nil {
		logger.Error("Failed to run consumer",
			slog.String("err", err.Error()))
		os.Exit(1)
	}
}
