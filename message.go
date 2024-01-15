package kefka

import (
	"strconv"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func retryableMessage(msg *kafka.Message, retryTopic string, err error) *kafka.Message {
	newMsg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic: &retryTopic,
		},
		Value: msg.Value,
		Key:   msg.Key,
	}

	headers := make([]kafka.Header, 0, len(msg.Headers))
	for _, header := range msg.Headers {
		headers = append(headers, kafka.Header{
			Key:   header.Key,
			Value: header.Value,
		})
	}

	originalPartition := strconv.Itoa(int(msg.TopicPartition.Partition))
	originalOffset := strconv.FormatInt(int64(msg.TopicPartition.Offset), 10)

	retryHeaders := []kafka.Header{
		{
			Key:   "kefka.dlt.original-topic",
			Value: []byte(*msg.TopicPartition.Topic),
		},
		{
			Key:   "kefka.dlt.original-partition",
			Value: []byte(originalPartition),
		},
		{
			Key:   "kefka.dlt.original-offset",
			Value: []byte(originalOffset),
		},
		{
			Key:   "kefka.dlt.retry-count",
			Value: []byte("0"),
		},
	}
	if err != nil {
		retryHeaders = append(retryHeaders, kafka.Header{
			Key:   "kefka.dlt.reason",
			Value: []byte(err.Error()),
		})
	}

	headers = append(headers, retryHeaders...)

	newMsg.Headers = headers
	return newMsg
}
