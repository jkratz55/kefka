package kefka

import (
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type consumerMock struct {
	mock.Mock
}

func (c *consumerMock) Assignment() (partitions []kafka.TopicPartition, err error) {
	args := c.Called()
	partitions = args.Get(0).([]kafka.TopicPartition)
	if args.Get(1) != nil {
		err = args.Get(1).(error)
	}
	return
}

func (c *consumerMock) Committed(partitions []kafka.TopicPartition, timeoutMs int) (offsets []kafka.TopicPartition, err error) {
	args := c.Called(partitions, timeoutMs)
	offsets = args.Get(0).([]kafka.TopicPartition)
	if args.Get(1) != nil {
		err = args.Get(1).(error)
	}
	return
}

func (c *consumerMock) QueryWatermarkOffsets(topic string, partition int32, timeoutMs int) (low int64, high int64, err error) {
	args := c.Called(topic, partition, timeoutMs)
	low = args.Get(0).(int64)
	high = args.Get(1).(int64)
	if args.Get(2) != nil {
		err = args.Get(2).(error)
	}
	return
}

func (c *consumerMock) Close() error {
	args := c.Called()
	if args.Get(0) != nil {
		return args.Get(0).(error)
	}
	return nil
}

func (c *consumerMock) Commit() ([]kafka.TopicPartition, error) {
	args := c.Called()
	var err error
	topicPartitions := args.Get(0).([]kafka.TopicPartition)
	if args.Get(1) != nil {
		err = args.Get(1).(error)
	}
	return topicPartitions, err
}

func (c *consumerMock) Poll(timeoutMs int) (event kafka.Event) {
	args := c.Called(timeoutMs)
	return args.Get(0).(kafka.Event)
}

func (c *consumerMock) ReadMessage(timeout time.Duration) (*kafka.Message, error) {
	args := c.Called(timeout)
	var err error
	if args.Get(1) != nil {
		err = args.Get(1).(error)
	}
	return args.Get(0).(*kafka.Message), err
}

func (c *consumerMock) Subscribe(topic string, rebalanceCb kafka.RebalanceCb) error {
	args := c.Called(topic, rebalanceCb)
	if args.Get(0) != nil {
		return args.Get(0).(error)
	}
	return nil
}

type mockHandler struct {
	mock.Mock
	counter int
}

func (m *mockHandler) Handle(msg *kafka.Message, ack Commit) error {
	m.counter++
	args := m.Called(msg, ack)
	if args.Get(0) != nil {
		return args.Get(0).(error)
	}
	return nil
}

var (
	testConsumerKafkaConfig = &kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "kefka-test",
		"auto.offset.reset": "smallest",
	}
)

func TestNewConsumer(t *testing.T) {
	hander := new(mockHandler)
	opts := ConsumerOptions{
		KafkaConfig: &kafka.ConfigMap{
			"bootstrap.servers": "localhost:9092",
			"group.id":          "kefka-test",
			"auto.offset.reset": "smallest",
		},
		Handler: hander,
		Topic:   "test",
		Logger:  nopLogger{},
	}

	var consumer *Consumer
	var err error
	assert.NotPanics(t, func() {
		consumer, err = NewConsumer(opts)
		assert.NoError(t, err)
		assert.NotNil(t, consumer)
	})

	opts.KafkaConfig = nil
	assert.Panics(t, func() {
		consumer, err = NewConsumer(opts)
	})

	opts.KafkaConfig = &kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "kefka-test",
		"auto.offset.reset": "smallest",
	}
	opts.Handler = nil
	assert.Panics(t, func() {
		consumer, err = NewConsumer(opts)
	})

	opts.Handler = hander
	opts.Topic = ""
	assert.Panics(t, func() {
		consumer, err = NewConsumer(opts)
	})
}

func TestConsumer_Assignments(t *testing.T) {
	topic := "test"
	hander := new(mockHandler)
	opts := ConsumerOptions{
		KafkaConfig: testConsumerKafkaConfig,
		Handler:     hander,
		Topic:       topic,
		Logger:      nopLogger{},
	}
	consumer, err := NewConsumer(opts)
	assert.NoError(t, err)

	expected := []kafka.TopicPartition{
		{
			Topic:     &topic,
			Partition: 0,
			Offset:    1000,
		},
		{
			Topic:     &topic,
			Partition: 1,
			Offset:    343,
		},
	}
	baseConsumer := new(consumerMock)
	baseConsumer.On("Assignment").Return(expected, nil)
	consumer.baseConsumer = baseConsumer

	topicPartitions, err := consumer.Assignments()
	assert.NoError(t, err)
	assert.Equal(t, expected, topicPartitions)
}

func TestConsumer_Close(t *testing.T) {
	topic := "test"
	hander := new(mockHandler)
	opts := ConsumerOptions{
		KafkaConfig: testConsumerKafkaConfig,
		Handler:     hander,
		Topic:       topic,
		Logger:      nopLogger{},
	}
	consumer, err := NewConsumer(opts)
	assert.NoError(t, err)

	baseConsumer := new(consumerMock)
	baseConsumer.On("Close").Return(nil)
	consumer.baseConsumer = baseConsumer

	err = consumer.Close()
	assert.NoError(t, err)
}

func TestConsumer_Consume(t *testing.T) {
	topic := "test"
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: 0,
			Offset:    100,
		},
		Value:     []byte("world"),
		Key:       []byte("hello"),
		Timestamp: time.Now(),
	}
	hander := new(mockHandler)
	hander.On("Handle", mock.Anything, mock.Anything).
		Return(nil)

	opts := ConsumerOptions{
		KafkaConfig: testConsumerKafkaConfig,
		Handler:     hander,
		Topic:       topic,
		Logger:      nopLogger{},
	}
	consumer, err := NewConsumer(opts)
	assert.NoError(t, err)

	baseConsumer := new(consumerMock)
	baseConsumer.On("ReadMessage", mock.Anything).
		Return(msg, nil)
	consumer.baseConsumer = baseConsumer

	go consumer.Consume()
	time.Sleep(time.Millisecond * 100)
	assert.Greater(t, hander.counter, 0)
}

func TestConsumer_Lag(t *testing.T) {
	topic := "test"
	hander := new(mockHandler)
	hander.On("Handle", mock.Anything, mock.Anything).
		Return(nil)

	opts := ConsumerOptions{
		KafkaConfig: testConsumerKafkaConfig,
		Handler:     hander,
		Topic:       topic,
		Logger:      nopLogger{},
	}
	consumer, err := NewConsumer(opts)
	assert.NoError(t, err)

	assignments := []kafka.TopicPartition{
		{
			Topic:     &topic,
			Partition: 0,
			Offset:    1000,
		},
		{
			Topic:     &topic,
			Partition: 1,
			Offset:    343,
		},
	}
	low := int64(0)
	high := int64(10000)
	mockConsumer := new(consumerMock)
	mockConsumer.On("Assignment").Return(assignments, nil)
	mockConsumer.On("Committed", mock.Anything, mock.Anything).
		Return(assignments, nil)
	mockConsumer.On("QueryWatermarkOffsets", mock.Anything, mock.Anything, mock.Anything).
		Return(low, high, err)
	consumer.baseConsumer = mockConsumer

	lag, err := consumer.Lag()
	assert.NoError(t, err)

	expected := map[string]int64{
		"test|0": 9000,
		"test|1": 9657,
	}
	assert.Equal(t, expected, lag)
}

func TestConsumer_LagForTopicPartition(t *testing.T) {
	topic := "test"
	hander := new(mockHandler)
	hander.On("Handle", mock.Anything, mock.Anything).
		Return(nil)

	opts := ConsumerOptions{
		KafkaConfig: testConsumerKafkaConfig,
		Handler:     hander,
		Topic:       topic,
		Logger:      nopLogger{},
	}
	consumer, err := NewConsumer(opts)
	assert.NoError(t, err)

	assignments := []kafka.TopicPartition{
		{
			Topic:     &topic,
			Partition: 0,
			Offset:    1000,
		},
		{
			Topic:     &topic,
			Partition: 1,
			Offset:    343,
		},
	}
	low := int64(0)
	high := int64(10000)
	mockConsumer := new(consumerMock)
	mockConsumer.On("Assignment").Return(assignments, nil)
	mockConsumer.On("Committed", mock.Anything, mock.Anything).
		Return(assignments, nil)
	mockConsumer.On("QueryWatermarkOffsets", mock.Anything, mock.Anything, mock.Anything).
		Return(low, high, err)
	consumer.baseConsumer = mockConsumer

	lag, err := consumer.LagForTopicPartition("test", 0)
	assert.NoError(t, err)
	assert.Equal(t, int64(9000), lag)
}
