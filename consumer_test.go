package kefka

import (
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestNewConsumer(t *testing.T) {
	tests := []struct {
		name      string
		conf      Config
		handler   Handler
		topic     string
		expectErr bool
	}{
		{
			name: "Valid Configuration",
			conf: Config{
				BootstrapServers: []string{"localhost:9092"},
				GroupID:          "kefka-test",
				AutoOffsetReset:  Earliest,
			},
			handler: HandlerFunc(func(msg *kafka.Message) error {
				return nil
			}),
			topic:     "test",
			expectErr: false,
		},
		{
			name: "Invalid Configuration - Missing Handler",
			conf: Config{
				BootstrapServers: []string{"localhost:9092"},
				GroupID:          "kefka-test",
				AutoOffsetReset:  Earliest,
			},
			handler:   nil,
			topic:     "test",
			expectErr: true,
		},
		{
			name: "Invalid Configuration - Missing Topic",
			conf: Config{
				BootstrapServers: []string{"localhost:9092"},
				GroupID:          "kefka-test",
				AutoOffsetReset:  Earliest,
			},
			handler: HandlerFunc(func(msg *kafka.Message) error {
				return nil
			}),
			topic:     "",
			expectErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			consumer, err := NewConsumer(test.conf, test.handler, test.topic)
			assert.Equal(t, test.expectErr, err != nil)
			if !test.expectErr {
				assert.NotNil(t, consumer)
			}
		})
	}
}

// todo: Consumer needs a lot more unit testing

func TestConsumer_Run(t *testing.T) {

	base := new(mockBaseConsumer)
	base.On("Poll", mock.Anything).Return()
}
