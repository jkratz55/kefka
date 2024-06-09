package kefka

import (
	"context"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type Producer struct {
	producer *kafka.Producer
}

func NewProducer() (*Producer, error) {
	return nil, nil
}

func (p *Producer) M() *MessageBuilder {
	return &MessageBuilder{producer: p}
}

func (p *Producer) Produce(m *kafka.Message, deliveryChan chan kafka.Event) error {
	return p.producer.Produce(m, deliveryChan)
}

func (p *Producer) ProduceAndWait(m *kafka.Message) error {
	// todo: implement
	return p.producer.Produce(m, nil)
}

func (p *Producer) Transactional(ctx context.Context, msgs []*kafka.Message) error {
	if err := p.producer.InitTransactions(ctx); err != nil {
		return fmt.Errorf("kafka: failed to initialize transactions: %w", err)
	}

	err := p.producer.BeginTransaction()
	deliveryChan := make(chan kafka.Event, len(msgs))

	for i := 0; i < len(msgs); i++ {
		err = p.producer.Produce(msgs[i], deliveryChan)
		if err != nil {
			abortErr := p.producer.AbortTransaction(ctx)
			if abortErr != nil {
				return fmt.Errorf("kafka: failed to abort transaction: %w: failed to produce message: %w", abortErr, err)
			}
			return err
		}
	}
	close(deliveryChan)

	for event := range deliveryChan {
		switch ev := event.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				abortErr := p.producer.AbortTransaction(ctx)
				if abortErr != nil {
					return fmt.Errorf("kafka: failed to abort transaction: %w: message delivery failed: %w", abortErr, ev.TopicPartition.Error)
				}
				return fmt.Errorf("kafka: transaction aborted: delivery failure: %w", ev.TopicPartition.Error)
			}
		}
	}

	err = p.producer.CommitTransaction(ctx)
	if err != nil {
		return fmt.Errorf("kafka: failed to commit transaction: %w", err)
	}

	return nil
}

func (p *Producer) Flush(timeout time.Duration) int {
	return p.producer.Flush(int(timeout.Milliseconds()))
}

func (p *Producer) Close() {
	p.producer.Close()
}
