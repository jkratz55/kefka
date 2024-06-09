package kefka

import (
	"strconv"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/pkg/errors"
)

// baseConsumer is an interface type that defines the behavior and functionality of
// Kafka client Consumer. This interface exists to allow for mocking and testing.
type baseConsumer interface {
	Assignment() ([]kafka.TopicPartition, error)
	Subscription() (topics []string, err error)
	Committed(partitions []kafka.TopicPartition, timeoutMs int) (offsets []kafka.TopicPartition, err error)
	QueryWatermarkOffsets(topic string, partition int32, timeoutMs int) (low, high int64, err error)
	Subscribe(topics string, rebalanceCb kafka.RebalanceCb) error
	Poll(timeoutMs int) (event kafka.Event)
	CommitMessage(msg *kafka.Message) ([]kafka.TopicPartition, error)
	StoreMessage(msg *kafka.Message) (storedOffsets []kafka.TopicPartition, err error)
	Commit() ([]kafka.TopicPartition, error)
	Position(partitions []kafka.TopicPartition) (offsets []kafka.TopicPartition, err error)
}

// Ensures that the kafka.Consumer implements all methods defined by the baseConsumer interface.
var _ baseConsumer = &kafka.Consumer{}

type Consumer struct {
	base    baseConsumer
	running bool
	mu      sync.Mutex
}

func NewConsumer(conf Config, handler Handler, topic string) (*Consumer, error) {
	return nil, nil
}

func (c *Consumer) Run() error {
	return nil
}

func (c *Consumer) Assignment() (kafka.TopicPartitions, error) {
	return c.base.Assignment()
}

func (c *Consumer) Subscription() ([]string, error) {
	return c.base.Subscription()
}

func (c *Consumer) Position() ([]kafka.TopicPartition, error) {

	// Get the current assigned partitions for the Consumer
	topicPartitions, err := c.base.Assignment()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get assignments for Consumer")
	}
	return c.base.Position(topicPartitions)
}

func (c *Consumer) Lag() (map[string]int64, error) {
	lags := make(map[string]int64)

	// Get the current assigned partitions for the Consumer
	topicPartitions, err := c.base.Assignment()
	if err != nil {
		return lags, errors.Wrap(err, "failed to get assignments for Consumer")
	}

	// Get the current offset for each partition assigned to the consumer group
	topicPartitions, err = c.base.Committed(topicPartitions, 5000)
	if err != nil {
		return lags, errors.Wrap(err, "failed to get committed offsets for Consumer")
	}

	// Iterate over each partition and query for the watermarks. The lag is determined by
	// the difference between the high watermark and the current offset.
	for _, tp := range topicPartitions {
		low, high, err := c.base.QueryWatermarkOffsets(*tp.Topic, tp.Partition, 5000)
		if err != nil {
			return lags, errors.Errorf("failed to get watermark offsets for partition %d of topic %s", tp.Partition, *tp.Topic)
		}

		offset := int64(tp.Offset)
		if tp.Offset == kafka.OffsetInvalid {
			offset = low
		}

		key := *tp.Topic + "|" + strconv.Itoa(int(tp.Partition))
		lags[key] = high - offset
	}

	return lags, nil
}

func (c *Consumer) Commit() error {
	_, err := c.base.Commit()
	return err
}

func (c *Consumer) Close() {

}
