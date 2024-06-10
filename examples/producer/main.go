package main

import (
	"log/slog"
	"os"
	"time"

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

	for i := 0; i < 1000000; i++ {
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

	for i := 0; i < 10000; i++ {
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

	producer.Flush(time.Second * 10)
	time.Sleep(5 * time.Second)
	producer.Close()
	time.Sleep(5 * time.Second)
}
