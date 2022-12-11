package kefka

import (
	"context"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// SyncProduce is a convenient function for producing messages synchronously.
// Technically the Confluent Kafka Producer doesn't support producing events
// synchronously. Instead, this function creates a delivery channel and wait on it
// for the delivery report/acknowledgement.
//
// SyncProduce accepts a context so that this operation can be cancelled or
// timeout. However, it is very important to note this does not cancel producing
// the message. It simply cancels waiting on the delivery report. The message still
// may be delivered.
func SyncProduce(ctx context.Context, p kafkaProducer, topic string, key, value []byte) error {
	ch := make(chan kafka.Event, 1)
	err := p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Value: value,
		Key:   key,
	}, ch)
	if err != nil {
		return fmt.Errorf("message could not be enqued: %w", err)
	}

	select {
	case <-ctx.Done():
		return fmt.Errorf("aborted waiting for message delivery report: %w", ctx.Err())
	case e := <-ch:
		m := e.(*kafka.Message)
		if m.TopicPartition.Error != nil {
			return fmt.Errorf("error publishing/producing message: %w", m.TopicPartition.Error)
		}
		return nil
	}
}

// SyncProduceMessage is a convenient function for producing messages synchronously.
// Technically the Confluent Kafka Producer doesn't support producing events
// synchronously. Instead, this function creates a delivery channel and wait on it
// for the delivery report/acknowledgement.
//
// SyncProduceMessage accepts a context so that this operation can be cancelled or
// timeout. However, it is very important to note this does not cancel producing
// the message. It simply cancels waiting on the delivery report. The message still
// may be delivered.
func SyncProduceMessage(ctx context.Context, p kafkaProducer, msg *kafka.Message) error {
	ch := make(chan kafka.Event, 1)
	err := p.Produce(msg, ch)
	if err != nil {
		return fmt.Errorf("message could not be enqued: %w", err)
	}

	select {
	case <-ctx.Done():
		return fmt.Errorf("aborted waiting for message delivery report: %w", ctx.Err())
	case e := <-ch:
		m := e.(*kafka.Message)
		if m.TopicPartition.Error != nil {
			return fmt.Errorf("error publishing/producing message: %w", m.TopicPartition.Error)
		}
		return nil
	}
}

// FlushAll will continuously call Flush on the Kafka Producer until there are
// zero messages awaiting delivery.
//
// This function is blocking and should really only be called if you need to
// force the internal queue empty. An example might be an application exiting.
func FlushAll(p kafkaProducer) {
	for remaining := p.Flush(5000); remaining > 0; {
		// keep calling flush until it returns 0 elements remaining
	}
}
