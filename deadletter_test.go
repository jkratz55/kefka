package kefka

import (
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestDeadLetterHandler_Handle(t *testing.T) {

	onErrorInvokeCount := 0
	onErrorCb := func(msg *kafka.Message, err error) {
		onErrorInvokeCount++
	}

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
		onErrorCount int
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
				p.On("Flush", mock.Anything).Return(0)
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
			name: "Handler Fails And DLT Produce Fails",
			opts: DeadLetterOpts{
				Topic:          "test.dlt",
				Logger:         NopLogger(),
				OnPublishError: onErrorCb,
			},
			initHandler: func() Handler {
				h := new(mockHandler)
				h.On("Handle", msg).Return(assert.AnError)
				return h
			},
			initProcuder: func() *Producer {
				p := new(mockBaseProducer)
				p.On("Produce", mock.Anything, mock.Anything).Return(assert.AnError)
				p.On("Flush", mock.Anything).Return(0)
				return &Producer{
					base:           p,
					loggerStopChan: make(chan struct{}),
					eventStopChan:  make(chan struct{}),
				}
			},
			message:      msg,
			expectErr:    true,
			onErrorCount: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			onErrorInvokeCount = 0
			producer := test.initProcuder()

			test.opts.Producer = producer
			handler := DeadLetter(test.opts)(test.initHandler())

			err := handler.Handle(test.message)
			assert.Equal(t, test.expectErr, err != nil)
			assert.Equal(t, test.onErrorCount, onErrorInvokeCount)
		})
	}
}
