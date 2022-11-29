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

	"github.com/jkratz55/kefka"
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

	kafkaConfig := &kafka.ConfigMap{
		"bootstrap.servers": brokers,
		"client.id":         uuid.New().String(),
		"acks":              "all",
		"retries":           5,
	}

	logger, err := zap.NewProduction()
	if err != nil {
		panic(err)
	}

	reportChan := make(chan kefka.DeliveryReport, 1000)
	go func() {
		for report := range reportChan {
			if report.Error != nil {
				logger.Error("error delivery message",
					zap.Error(err))
			} else {
				logger.Info(fmt.Sprintf("Delivered message topic: %s, partition: %d, offset: %d", report.Topic, report.Partition, report.Offset))
			}
		}
	}()

	producer, err := kefka.NewProducer(kefka.ProducerOptions{
		KafkaConfig:        kafkaConfig,
		KeyMarshaller:      kefka.StringMarshaller(),
		ValueMarshaller:    kefka.JsonMarshaller(),
		DeliveryReportChan: reportChan,
		Logger: kefka.LoggerFunc(func(level kefka.LogLevel, s string, a ...any) {
			zapLevel := mapKefkaLogLevelToZap(level)
			logger.Log(zapLevel, fmt.Sprintf(s, a...))
		}),
	})
	if err != nil {
		panic(err)
	}

	// logger.Info("Testing produce asynchronously")
	// for i := 0; i < 1000; i++ {
	// 	id := uuid.New()
	// 	data := order{
	// 		ID:        id.String(),
	// 		FirstName: "Billy",
	// 		LastName:  "Bob",
	// 		Total:     9.99,
	// 	}
	// 	if err := producer.Produce("test", id, data); err != nil {
	// 		logger.Error("failed to produce message",
	// 			zap.Error(err))
	// 	}
	// }

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

	for {
		time.Sleep(time.Second * 1)
	}

	producer.Flush(time.Second * 10)
	producer.Close()
}

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
