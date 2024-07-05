package kefka

import (
	"context"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/jkratz55/kefka/v2/internal"
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

type mockIntegrationHandler struct {
	keys []string
}

func (m *mockIntegrationHandler) Handle(msg *kafka.Message) error {
	m.keys = append(m.keys, string(msg.Key))
	return nil
}

func TestConsumer_Integration(t *testing.T) {
	if !internal.IsTestContainersEnabled() {
		t.Skip("testcontainers not enabled")
	}

	ctx := context.Background()
	kafkaContainer, err := initKafkaTestContainer(ctx)
	assert.NoError(t, err)

	brokers, err := kafkaContainer.Brokers(ctx)
	assert.NoError(t, err)

	producer, err := NewProducer(Config{
		BootstrapServers: brokers,
		RequiredAcks:     AckLeader,
		Logger:           NopLogger(),
	})
	assert.NoError(t, err)

	keys := make([]string, 1000)

	// Produce messages for the consumer to consume
	for i := 0; i < 1000; i++ {
		key := uuid.New().String()
		keys[i] = key

		err := producer.M().
			Topic("test").
			Key(key).
			Value([]byte(key)).
			SendAndWait()
		assert.NoError(t, err)
	}

	handler := &mockIntegrationHandler{}

	consumer, err := NewConsumer(Config{
		BootstrapServers: brokers,
		GroupID:          "kefka-test",
		AutoOffsetReset:  Earliest,
		SecurityProtocol: Plaintext,
		Logger:           NopLogger(),
	}, handler, "test")
	assert.NoError(t, err)

	// Run the consumer as one normally would
	go func() {
		err := consumer.Run()
		assert.NoError(t, err)
	}()

	// Once the consumer has read all 1000 messages we published close it so it
	// stops consuming messages.
	ticker := time.NewTicker(time.Second * 1)

Loop:
	for {
		select {
		case <-ticker.C:
			if len(handler.keys) == 1000 {
				consumer.Close()
				break Loop
			}
		}
	}

	assert.ElementsMatch(t, keys, handler.keys)
}
