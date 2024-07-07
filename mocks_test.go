package kefka

import (
	"context"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/mock"
)

type mockHandler struct {
	mock.Mock

	attempts int
}

func (m *mockHandler) Handle(msg *kafka.Message) error {
	m.attempts++
	args := m.Called(msg)
	if args.Error(0) != nil {
		return args.Error(0)
	}
	return nil
}

type mockBaseProducer struct {
	mock.Mock
}

func (m *mockBaseProducer) Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error {
	args := m.Called(msg, deliveryChan)
	if args.Error(0) != nil {
		return args.Error(0)
	}

	// The delivery report has to be sent of the producer can hang waiting for
	// the report. Oh also, this has to be done in a goroutine because the otherwise
	// this will just hang forever ...
	go func() {
		deliveryChan <- &kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:       msg.TopicPartition.Topic,
				Partition:   0,
				Offset:      0,
				Metadata:    nil,
				Error:       nil,
				LeaderEpoch: nil,
			},
			Value:   msg.Value,
			Key:     msg.Key,
			Headers: msg.Headers,
		}
	}()

	return nil
}

func (m *mockBaseProducer) Events() chan kafka.Event {
	args := m.Called()
	return args.Get(0).(chan kafka.Event)
}

func (m *mockBaseProducer) Flush(timeoutMs int) int {
	args := m.Called(timeoutMs)
	return args.Int(0)
}

func (m *mockBaseProducer) Close() {
	m.Called()
}

func (m *mockBaseProducer) IsClosed() bool {
	args := m.Called()
	return args.Bool(0)
}

func (m *mockBaseProducer) InitTransactions(ctx context.Context) error {
	args := m.Called(ctx)
	if args.Error(0) != nil {
		return args.Error(0)
	}
	return nil
}

func (m *mockBaseProducer) BeginTransaction() error {
	args := m.Called()
	if args.Error(0) != nil {
		return args.Error(0)
	}
	return nil
}

func (m *mockBaseProducer) AbortTransaction(ctx context.Context) error {
	args := m.Called(ctx)
	if args.Error(0) != nil {
		return args.Error(0)
	}
	return nil
}

func (m *mockBaseProducer) CommitTransaction(ctx context.Context) error {
	args := m.Called(ctx)
	if args.Error(0) != nil {
		return args.Error(0)
	}
	return nil
}

func (m *mockBaseProducer) Len() int {
	args := m.Called()
	return args.Int(0)
}

type mockBaseConsumer struct {
	mock.Mock
}

func (mc *mockBaseConsumer) Assignment() ([]kafka.TopicPartition, error) {
	args := mc.Called()
	if args.Error(1) != nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]kafka.TopicPartition), nil
}

func (mc *mockBaseConsumer) Subscription() (topics []string, err error) {
	args := mc.Called()
	if args.Error(1) != nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]string), nil
}

func (mc *mockBaseConsumer) Committed(partitions []kafka.TopicPartition, timeoutMs int) (offsets []kafka.TopicPartition, err error) {
	args := mc.Called(partitions, timeoutMs)
	if args.Error(1) != nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]kafka.TopicPartition), nil
}

func (mc *mockBaseConsumer) QueryWatermarkOffsets(topic string, partition int32, timeoutMs int) (low, high int64, err error) {
	args := mc.Called(topic, partition, timeoutMs)
	if args.Error(2) != nil {
		return 0, 0, args.Error(2)
	}
	return args.Get(0).(int64), args.Get(1).(int64), nil
}

func (mc *mockBaseConsumer) Subscribe(topics string, rebalanceCb kafka.RebalanceCb) error {
	args := mc.Called(topics, rebalanceCb)
	if args.Error(0) != nil {
		return args.Error(0)
	}
	return nil
}

func (mc *mockBaseConsumer) Poll(timeoutMs int) (event kafka.Event) {
	args := mc.Called(timeoutMs)
	return args.Get(0).(kafka.Event)
}

func (mc *mockBaseConsumer) CommitMessage(msg *kafka.Message) ([]kafka.TopicPartition, error) {
	args := mc.Called(msg)
	if args.Error(1) != nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]kafka.TopicPartition), nil
}

func (mc *mockBaseConsumer) StoreMessage(msg *kafka.Message) (storedOffsets []kafka.TopicPartition, err error) {
	args := mc.Called(msg)
	if args.Error(1) != nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]kafka.TopicPartition), nil
}

func (mc *mockBaseConsumer) Commit() ([]kafka.TopicPartition, error) {
	args := mc.Called()
	if args.Error(1) != nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]kafka.TopicPartition), nil
}

func (mc *mockBaseConsumer) Position(partitions []kafka.TopicPartition) (offsets []kafka.TopicPartition, err error) {
	args := mc.Called(partitions)
	if args.Error(1) != nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]kafka.TopicPartition), nil
}

func (mc *mockBaseConsumer) IsClosed() bool {
	args := mc.Called()
	return args.Bool(0)
}

func (mc *mockBaseConsumer) Close() error {
	args := mc.Called()
	if args.Error(0) != nil {
		return args.Error(0)
	}
	return nil
}
