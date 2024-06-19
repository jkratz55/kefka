package kefka

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/assert"
)

func TestRetryHandler_Handle(t *testing.T) {
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
		name             string
		opts             RetryOpts
		initHandler      func() Handler
		message          *kafka.Message
		expectErr        bool
		expectedAttempts int
	}{
		{
			name: "First Attempt Succeeds",
			opts: RetryOpts{
				MaxAttempts: 3,
				Backoff:     ConstantBackoff(time.Millisecond * 100),
				OnError: func(err error) bool {
					return true
				},
			},
			initHandler: func() Handler {
				h := new(mockHandler)
				h.On("Handle", msg).Return(nil)
				return h
			},
			message:          msg,
			expectErr:        false,
			expectedAttempts: 1,
		},
		{
			name: "First Attempt Fails, Second Attempts Succeeds",
			opts: RetryOpts{
				MaxAttempts: 3,
				Backoff:     ConstantBackoff(time.Millisecond * 100),
				OnError: func(err error) bool {
					return true
				},
			},
			initHandler: func() Handler {
				h := new(mockHandler)
				h.On("Handle", msg).Return(assert.AnError).Once()
				h.On("Handle", msg).Return(nil).Once()
				return h
			},
			message:          msg,
			expectErr:        false,
			expectedAttempts: 2,
		},
		{
			name: "Fail All Three Attempts",
			opts: RetryOpts{
				MaxAttempts: 3,
				Backoff:     ConstantBackoff(time.Millisecond * 100),
				OnError: func(err error) bool {
					return true
				},
			},
			initHandler: func() Handler {
				h := new(mockHandler)
				h.On("Handle", msg).Return(assert.AnError)
				return h
			},
			message:          msg,
			expectErr:        true,
			expectedAttempts: 3,
		},
		{
			name: "Fail First Two Attempts, Succeed on Last Attempt",
			opts: RetryOpts{
				MaxAttempts: 3,
				Backoff:     ConstantBackoff(time.Millisecond * 100),
				OnError: func(err error) bool {
					return true
				},
			},
			initHandler: func() Handler {
				h := new(mockHandler)
				h.On("Handle", msg).Return(assert.AnError).Times(2)
				h.On("Handle", msg).Return(nil).Once()
				return h
			},
			message:          msg,
			expectErr:        false,
			expectedAttempts: 3,
		},
		{
			name: "OnErrorFunc Stop Early",
			opts: RetryOpts{
				MaxAttempts: 3,
				Backoff:     ConstantBackoff(time.Millisecond * 100),
				OnError: func(err error) bool {
					if errors.Is(err, context.DeadlineExceeded) {
						return false
					}
					return true
				},
			},
			initHandler: func() Handler {
				h := new(mockHandler)
				h.On("Handle", msg).Return(assert.AnError).Once()
				h.On("Handle", msg).Return(context.DeadlineExceeded)
				return h
			},
			message:          msg,
			expectErr:        true,
			expectedAttempts: 2,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			innerHandler := test.initHandler()
			h := Retry(test.opts)(innerHandler)
			err := h.Handle(test.message)
			assert.Equal(t, test.expectErr, err != nil)
			assert.Equal(t, test.expectedAttempts, innerHandler.(*mockHandler).attempts)
		})
	}
}
