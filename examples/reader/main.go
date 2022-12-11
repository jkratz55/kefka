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
			// Here we are just printing the offset of the message but this is
			// where the logic would go to handle the message... save to database,
			// trigger some action, etc.
			// Note: here we are using MessageHandlerFunc for convince but it most
			// cases you'll likely want to implement your own type and include retries
			// and more robust error handling
			fmt.Println(msg.TopicPartition.Offset)
		}),
		ErrorCallback: func(err error) {
			// This callback gives the caller the oppertunity to log errors or
			// increment metrics/counters. For demo purposes we are just printing
			// errors out to stdout.
			fmt.Println("ERROR:", err)
		},
		PartitionEOFCallback: func(topic string, partition int, offset int64) {
			// Gets called to inform the caller the end of the topic has been reached
			fmt.Printf("Reached EOF for Topic %s, Partition %d, Offset %d\n", topic, partition, offset)
		},
		// Defines the topics/partitions you want to read, and where you want to start.
		// NOTE: If the offset is invalid the default behavior will be to start at the
		// last offset
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

	offsets := reader.QueryWatermarkOffsets()
	fmt.Println(offsets)

	// You will almost always want to call read in a separate goroutine unless
	// you truely want to block forever.
	go reader.Read()

	// Lazy example to let the reader run in the background and then close it...
	// Don't do this as home
	time.Sleep(10 * time.Second)
	reader.Close()
	time.Sleep(10 * time.Second)

	// Demonstrates the use of the ReadTopicPartitions helper function which can be
	// used instead of the Reader type.
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
