package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type order struct {
	ID        string
	FirstName string
	LastName  string
	Total     float64
}

func main() {

	brokers := os.Getenv("KAFKA_BROKERS")
	if strings.TrimSpace(brokers) == "" {
		panic("KAFKA_BROKERS environment variable not set or blank")
	}

	// Setup configuration for Kafka
	kafkaConfig := &kafka.ConfigMap{
		"bootstrap.servers": brokers,
		"client.id":         uuid.New().String(),
		"acks":              "all",
		"retries":           5,
	}

	// Zap is used here to provide an example of how to use a third party logger
	logger, err := zap.NewDevelopment(zap.IncreaseLevel(zap.DebugLevel))
	if err != nil {
		panic(err)
	}

	// Create new producer with the required configuration
	producer, err := kefka.NewProducer(kefka.ProducerOptions{
		KafkaConfig:     kafkaConfig,
		KeyMarshaller:   kefka.StringMarshaller(),
		ValueMarshaller: kefka.JsonMarshaller(),
		Logger: kefka.LoggerFunc(func(level kefka.LogLevel, s string, a ...any) {
			zapLevel := mapKefkaLogLevelToZap(level)
			logger.Log(zapLevel, fmt.Sprintf(s, a...))
		}),
	})
	if err != nil {
		panic(err)
	}

	// Since we want the delivery reports we create a channel to receive the
	// reports. In the example below a single channel is being used to get
	// the report for all messages produced. In the real world you likely would
	// not do this, and instead use a goroutine and channel per write or set of
	// writes as it relates to whatever is an "operation" or "transaction" in your
	// system.
	deliveryChan := make(chan kafka.Event, 1000)
	go func() {
		for e := range deliveryChan {
			switch ev := e.(type) {
			case *kafka.Message:
				// This is the message delivery report indicating if the message
				// was successfully delivery or encountered a permanent failure
				// after librdkafka exhausted all retries. Application level retries
				// are not recommended here since the client is already configured
				// to retry.
				if ev.TopicPartition.Error == nil {
					logger.Info(fmt.Sprintf("Successfully delivered message to Topic %s, Partition %d Offset %d",
						*ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset))
				} else {
					logger.Error("Failed to deliver message",
						zap.Error(err))
				}
			}
		}
	}()

	// Produces 1000 messages to Kafka, delivery reports will be fed into the
	// provided delivery channel
	logger.Info("Testing produce asynchronously")
	for i := 0; i < 1000; i++ {
		id := uuid.New()
		data := order{
			ID:        id.String(),
			FirstName: "Billy",
			LastName:  "Bob",
			Total:     9.99,
		}
		if err := producer.Produce("test", id, data, deliveryChan); err != nil {
			logger.Error("failed to produce message",
				zap.Error(err))
		}
	}

	// Confluent Kafka Producer doesn't technically support synchronously producing
	// messages, but Kefka emulates it. There is a caveat, this operation takes a
	// context so that it may be canceled or timeout so we don't block indefinitely.
	// However, the message may or may not be delivered if the context is done before
	// receiving the delivery report.
	logger.Info("Testing produce synchronously")
	for i := 0; i < 10; i++ {
		id := uuid.New()
		data := order{
			ID:        id.String(),
			FirstName: "Billy",
			LastName:  "Bob",
			Total:     9.99,
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
		if err := producer.SyncProduce(ctx, "errTest", id, data); err != nil {
			logger.Error("failed to produce message",
				zap.Error(err))
		}
		cancel()
	}

	for remaining := producer.Flush(10000); remaining > 0; {
		// keep flushing until the toilet is unclogged
	}
	producer.Close()
}

// mapKefkaLogLevelToZap is an example of using zap with Kefka that maps the
// log level from Kefka to Zap
func mapKefkaLogLevelToZap(level kefka.LogLevel) zapcore.Level {
	switch level {
	case kefka.DebugLevel:
		return zap.DebugLevel
	case kefka.InfoLevel:
		return zap.InfoLevel
	case kefka.WarnLevel:
		return zap.WarnLevel
	case kefka.ErrorLevel:
		return zap.ErrorLevel
	case kefka.FatalLevel:
		return zap.FatalLevel
	default:
		// this should never happen because we've exhausted all the levels returned
		// be Kefka, so we'll just return Zap panic level
		return zap.PanicLevel
	}
}
