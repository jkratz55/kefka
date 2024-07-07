package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/prometheus/client_golang/prometheus/promhttp"

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

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	producer, err := kefka.NewProducer(conf)
	if err != nil {
		logger.Error("Failed to initialize Kafka Producer",
			slog.String("err", err.Error()))
		os.Exit(1)
	}

	go func() {
		for {
			err := producer.M().
				Topic("test").
				Key("hello").
				Value([]byte("world")).
				SendAndWait()
			if err != nil {
				logger.Error("Failed to send message",
					slog.String("err", err.Error()))
			}
			time.Sleep(time.Millisecond * 100) // Write a steady rate but not too fast
		}
	}()

	var handler kefka.Handler
	handler = kefka.HandlerFunc(func(msg *kafka.Message) error {
		// logger.Debug("Received message",
		// 	slog.Any("message", msg))
		return nil
	})
	handler = kefka.Retry(kefka.RetryOpts{
		MaxAttempts: 3,
		Backoff:     kefka.ExponentialBackoff(time.Millisecond*100, time.Second*1),
	})(handler)

	consumer, err := kefka.NewConsumer(conf, handler, "test")
	if err != nil {
		logger.Error("Failed to initialize Kafka Consumer",
			slog.String("err", err.Error()))
		os.Exit(1)
	}

	go func() {
		if err := consumer.Run(); err != nil {
			logger.Error("Consumer exited unexpectedly with error",
				slog.String("err", err.Error()))
		}
	}()

	go func() {
		promServer := http.Server{
			Addr:    ":8082",
			Handler: promhttp.Handler(),
		}
		_ = promServer.ListenAndServe()
	}()

	<-ctx.Done()
}
