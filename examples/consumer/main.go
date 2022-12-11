package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/kafka"

	"github.com/jkratz55/kefka"
)

type sampleMessageHandler struct {
	counter int
}

func (s *sampleMessageHandler) Handle(msg *kafka.Message, ack kefka.Commit) {
	s.counter++
	fmt.Println(msg.TopicPartition.Offset)

	// If we configured Kafka to disable automatic commits in favor of manual
	// commits, we'd need to call ack. Depending on your requirements and use
	// cases you may need to call ack on every successful processing of a message.
	// However, that is not performant and will lower throughput. Or you may decide
	// to commit on a time interval, or every X messages, or a combo of both.
	// IMPORTANT: DON'T CALL ack if you are using automatic commits, let the client
	// handle it for you automatically.
}

func main() {

	brokers := os.Getenv("KAFKA_BROKERS")
	if strings.TrimSpace(brokers) == "" {
		panic("KAFKA_BROKERS environment variable not set or blank")
	}

	config := &kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "kefka-test",
		"auto.offset.reset": "smallest",
	}

	handler := new(sampleMessageHandler)
	opts := kefka.ConsumerOptions{
		KafkaConfig: config,
		Handler:     handler,
		Topic:       "test",
		ErrorHandler: func(err error) {
			fmt.Println("Aahhhhh snap something went wrong!", err)
		},
	}

	consumer, err := kefka.NewConsumer(opts)
	if err != nil {
		panic(err)
	}

	go consumer.Consume()

	appCtx := context.Background()
	appCtx, stop := signal.NotifyContext(appCtx, syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	select {
	case <-appCtx.Done():
		fmt.Println("Consumed", handler.counter, "messages")
		return
	}
}
