package main

import (
	"context"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"

	"github.com/jkratz55/kefka"
)

func main() {
	config := &kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
	}

	topic := "test"

	opts := kefka.ReaderOptions{
		KafkaConfig: config,
		MessageHandler: kefka.MessageHandlerFunc(func(msg *kafka.Message, ack kefka.Commit) {
			fmt.Println(msg.TopicPartition.Offset)
		}),
		ErrorCallback: func(err error) {
			fmt.Println("ERROR:", err)
		},
		PartitionEOFCallback: func(topic string, partition int, offset int64) {
			fmt.Printf("Reached EOF for Topic %s, Partition %d, Offset %d\n", topic, partition, offset)
		},
		TopicPartitions: []kafka.TopicPartition{
			{
				Topic:     &topic,
				Partition: 0,
				Offset:    0,
			},
		},
	}

	err := kefka.ReadTopicPartitions(context.Background(), opts)
	fmt.Println(err)
}
