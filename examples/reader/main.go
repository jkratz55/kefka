package main

import (
	"context"
	"fmt"
	"time"

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
				Offset:    kefka.FirstOffset,
			},
		},
	}

	reader, err := kefka.NewReader(opts)
	if err != nil {
		panic(err)
	}

	go reader.Read()

	time.Sleep(10 * time.Second)
	reader.Close()
	time.Sleep(10 * time.Second)

	fmt.Println("Use ReadTopicPartitions function")
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		err := kefka.ReadTopicPartitions(ctx, opts)
		if err != nil {
			panic(err)
		}
	}()

	time.Sleep(10 * time.Second)
	cancel()
}
