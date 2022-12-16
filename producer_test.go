package kefka

import (
	"context"
	"sync"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type nopLogger struct{}

func (n nopLogger) Printf(lvl LogLevel, format string, args ...any) {
	return
}

type producerMock struct {
	mock.Mock
}

func (p *producerMock) Close() {}

func (p *producerMock) Events() chan kafka.Event {
	args := p.Called()
	return args.Get(0).(chan kafka.Event)
}

func (p *producerMock) Flush(timeoutMs int) int {
	args := p.Called(timeoutMs)
	return args.Get(0).(int)
}

func (p *producerMock) Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error {
	args := p.Called(msg, deliveryChan)

	if deliveryChan != nil {
		deliveryChan <- &kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     msg.TopicPartition.Topic,
				Partition: msg.TopicPartition.Partition,
				Offset:    1,
				Error:     nil,
			},
		}
	}

	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).(error)
}

func TestNewProducer(t *testing.T) {

	// todo: This is testing only the happy path and isn't testing if the function
	// 	panic from misuse or if creating the base confluent kafka producer fails.
	//  These are edge case but should still be tested.

	opts := ProducerOptions{
		KafkaConfig: &kafka.ConfigMap{
			"bootstrap.servers": "localhost:9092",
			"client.id":         uuid.New().String(),
			"acks":              "all",
			"retries":           5,
		},
		KeyMarshaller:   StringMarshaller(),
		ValueMarshaller: JsonMarshaller(),
		Logger:          nopLogger{},
	}

	var prodcuer *Producer
	var err error
	assert.NotPanics(t, func() {
		prodcuer, err = NewProducer(opts)
		assert.NoError(t, err)
		assert.NotNil(t, prodcuer)
	})
}

func TestProducer_Produce(t *testing.T) {

	opts := ProducerOptions{
		KafkaConfig: &kafka.ConfigMap{
			"bootstrap.servers": "localhost:9092",
			"client.id":         uuid.New().String(),
			"acks":              "all",
			"retries":           5,
		},
		KeyMarshaller:   StringMarshaller(),
		ValueMarshaller: JsonMarshaller(),
		Logger:          nopLogger{},
	}
	producer, err := NewProducer(opts)
	assert.NoError(t, err)

	eventChan := make(chan kafka.Event, 1)
	baseProducer := new(producerMock)
	baseProducer.On("Events").Return(eventChan)
	baseProducer.On("Produce", mock.Anything, mock.Anything).Return(nil)
	producer.baseProducer = baseProducer // replace internal Kafka Producer with mock

	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		report := <-eventChan
		m := report.(*kafka.Message)
		assert.NoError(t, m.TopicPartition.Error)
	}()

	err = producer.Produce("test", "billy", "bob", eventChan)
	assert.NoError(t, err)

	wg.Wait()
}

func TestProducer_ProduceMessage(t *testing.T) {
	opts := ProducerOptions{
		KafkaConfig: &kafka.ConfigMap{
			"bootstrap.servers": "localhost:9092",
			"client.id":         uuid.New().String(),
			"acks":              "all",
			"retries":           5,
		},
		KeyMarshaller:   StringMarshaller(),
		ValueMarshaller: JsonMarshaller(),
		Logger:          nopLogger{},
	}
	producer, err := NewProducer(opts)
	assert.NoError(t, err)

	eventChan := make(chan kafka.Event, 1)
	baseProducer := new(producerMock)
	baseProducer.On("Events").Return(eventChan)
	baseProducer.On("Produce", mock.Anything, mock.Anything).Return(nil)
	producer.baseProducer = baseProducer // replace internal Kafka Producer with mock

	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		report := <-eventChan
		m := report.(*kafka.Message)
		assert.NoError(t, m.TopicPartition.Error)
	}()

	topic := "test"
	err = producer.ProduceMessage(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Value: []byte("Hello World!"),
		Key:   []byte("KEY"),
	}, eventChan)
	assert.NoError(t, err)

	wg.Wait()
}

func TestProducer_SyncProduce(t *testing.T) {
	opts := ProducerOptions{
		KafkaConfig: &kafka.ConfigMap{
			"bootstrap.servers": "localhost:9092",
			"client.id":         uuid.New().String(),
			"acks":              "all",
			"retries":           5,
		},
		KeyMarshaller:   StringMarshaller(),
		ValueMarshaller: JsonMarshaller(),
		Logger:          nopLogger{},
	}
	producer, err := NewProducer(opts)
	assert.NoError(t, err)

	eventChan := make(chan kafka.Event, 1)
	baseProducer := new(producerMock)
	baseProducer.On("Events").Return(eventChan)
	baseProducer.On("Produce", mock.Anything, mock.Anything).Return(nil)
	producer.baseProducer = baseProducer // replace internal Kafka Producer with mock

	err = producer.SyncProduce(context.Background(), "test", "hello", "world")
	assert.NoError(t, err)
}

func TestProducer_SyncProduceMessage(t *testing.T) {
	opts := ProducerOptions{
		KafkaConfig: &kafka.ConfigMap{
			"bootstrap.servers": "localhost:9092",
			"client.id":         uuid.New().String(),
			"acks":              "all",
			"retries":           5,
		},
		KeyMarshaller:   StringMarshaller(),
		ValueMarshaller: JsonMarshaller(),
		Logger:          nopLogger{},
	}
	producer, err := NewProducer(opts)
	assert.NoError(t, err)

	eventChan := make(chan kafka.Event, 1)
	baseProducer := new(producerMock)
	baseProducer.On("Events").Return(eventChan)
	baseProducer.On("Produce", mock.Anything, mock.Anything).Return(nil)
	producer.baseProducer = baseProducer // replace internal Kafka Producer with mock

	topic := "test"
	err = producer.SyncProduceMessage(context.Background(), &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Value: []byte("Hello World!"),
		Key:   []byte("KEY"),
	})
	assert.NoError(t, err)
}

func TestProducer_Close(t *testing.T) {
	opts := ProducerOptions{
		KafkaConfig: &kafka.ConfigMap{
			"bootstrap.servers": "localhost:9092",
			"client.id":         uuid.New().String(),
			"acks":              "all",
			"retries":           5,
		},
		KeyMarshaller:   StringMarshaller(),
		ValueMarshaller: JsonMarshaller(),
		Logger:          nopLogger{},
	}
	producer, err := NewProducer(opts)
	assert.NoError(t, err)

	baseProducer := new(producerMock)
	baseProducer.On("Close").Return()

	producer.baseProducer = baseProducer
	producer.Close()
}

func TestProducer_Flush(t *testing.T) {
	opts := ProducerOptions{
		KafkaConfig: &kafka.ConfigMap{
			"bootstrap.servers": "localhost:9092",
			"client.id":         uuid.New().String(),
			"acks":              "all",
			"retries":           5,
		},
		KeyMarshaller:   StringMarshaller(),
		ValueMarshaller: JsonMarshaller(),
		Logger:          nopLogger{},
	}
	producer, err := NewProducer(opts)
	assert.NoError(t, err)

	baseProducer := new(producerMock)
	baseProducer.On("Flush", mock.Anything).Return(0)

	producer.baseProducer = baseProducer
	producer.Flush(10000)
}
