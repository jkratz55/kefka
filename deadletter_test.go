package kefka

import (
	"context"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type mockBaseProducer struct {
	mock.Mock
}

func (m *mockBaseProducer) Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error {
	args := m.Called(msg, deliveryChan)
	if args.Error(0) != nil {
		return args.Error(0)
	}

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
	// TODO implement me
	panic("implement me")
}

func (m *mockBaseProducer) Flush(timeoutMs int) int {
	// TODO implement me
	panic("implement me")
}

func (m *mockBaseProducer) Close() {
	m.Called()
}

func (m *mockBaseProducer) IsClosed() bool {
	// TODO implement me
	panic("implement me")
}

func (m *mockBaseProducer) InitTransactions(ctx context.Context) error {
	// TODO implement me
	panic("implement me")
}

func (m *mockBaseProducer) BeginTransaction() error {
	// TODO implement me
	panic("implement me")
}

func (m *mockBaseProducer) AbortTransaction(ctx context.Context) error {
	// TODO implement me
	panic("implement me")
}

func (m *mockBaseProducer) CommitTransaction(ctx context.Context) error {
	// TODO implement me
	panic("implement me")
}

func (m *mockBaseProducer) Len() int {
	// TODO implement me
	panic("implement me")
}

func TestDeadLetterHandler_Handle(t *testing.T) {
	topic := "test"
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: 0,
			Offset:    kafka.Offset(1001),
		},
		Value:     []byte("world"),
		Key:       []byte("hello"),
		Timestamp: time.Now(),
	}

	tests := []struct {
		name         string
		opts         DeadLetterOpts
		initProcuder func() *Producer
		initHandler  func() Handler
		message      *kafka.Message
		expectErr    bool
	}{
		{
			name: "Handler Processes Message Successfully",
			opts: DeadLetterOpts{
				Topic:  "test.dlt",
				Logger: NopLogger(),
			},
			initHandler: func() Handler {
				h := new(mockHandler)
				h.On("Handle", msg).Return(nil)
				return h
			},
			initProcuder: func() *Producer {
				p := new(mockBaseProducer)
				p.On("Produce", msg, mock.Anything).Return(nil)
				return &Producer{
					base:           p,
					loggerStopChan: make(chan struct{}),
					eventStopChan:  make(chan struct{}),
				}
			},
			message:   msg,
			expectErr: false,
		},
		{
			name: "Handler Fails DLT Produce Succeeds",
			opts: DeadLetterOpts{
				Topic:  "test.dlt",
				Logger: NopLogger(),
			},
			initHandler: func() Handler {
				h := new(mockHandler)
				h.On("Handle", msg).Return(assert.AnError)
				return h
			},
			initProcuder: func() *Producer {
				p := new(mockBaseProducer)
				p.On("Produce", mock.Anything, mock.Anything).Return(nil)
				return &Producer{
					base:           p,
					loggerStopChan: make(chan struct{}),
					eventStopChan:  make(chan struct{}),
				}
			},
			message:   msg,
			expectErr: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			producer := test.initProcuder()

			test.opts.Producer = producer
			handler := DeadLetter(test.opts)(test.initHandler())

			err := handler.Handle(test.message)
			assert.Equal(t, test.expectErr, err != nil)
		})
	}
}
