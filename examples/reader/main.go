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

	readerOpts := kefka.ReaderOpts{
		OnEndOfPartition: func(topic string, partition int, offset int64) {
			logger.Info("End of partition reached",
				slog.String("topic", topic),
				slog.Int("partition", partition),
				slog.Int64("offset", offset))
		},
		StopOnEndOfPartition: false,
		Limit:                0,
	}
	topicPartitions := []kafka.TopicPartition{
		{
			Topic:     kefka.StringPtr("test"),
			Partition: 0,
		},
	}
	reader, err := kefka.NewReader(conf, handler, topicPartitions, readerOpts)
	if err != nil {
		logger.Error("Failed to initialize Kafka Reader",
			slog.String("err", err.Error()))
		os.Exit(1)
	}

	if err := reader.Run(); err != nil {
		logger.Error("Failed to run reader",
			slog.String("err", err.Error()))
		os.Exit(1)
	}
}
