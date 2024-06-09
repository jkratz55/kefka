package experimental

import (
	"log/slog"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type Consumer struct {
	base    *kafka.Consumer
	logger  *slog.Logger
	running bool
}

func (c *Consumer) Run() error {
	return nil
}

func (c *Consumer) Assignment() (kafka.TopicPartitions, error) {
	return c.base.Assignment()
}

func (c *Consumer) Subscription() (topics []string, err error) {
	return c.base.Subscription()
}

func (c *Consumer) Close() error {
	return c.base.Close()
}
