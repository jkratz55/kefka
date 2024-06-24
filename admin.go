package kefka

import (
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// todo: implement funcs to fetch partitions and offsets by topic

func TopicPartitionsForTopic(topic string) ([]kafka.TopicPartition, error) {
	return nil, nil
}

func TopicPartitionsForTopicAndTimestamp(topic string, time time.Time) ([]kafka.TopicPartition, error) {
	return nil, nil
}
