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

func TestConsumer_handleError(t *testing.T) {
	type test struct {
		name      string
		arg       kafka.Error
		expectErr bool
	}

	tests := []test{
		{
			name:      "Non Fatal Error",
			arg:       kafka.NewError(kafka.ErrAllBrokersDown, "all brokers down", false),
			expectErr: false,
		},
		{
			name:      "Fatal Error",
			arg:       kafka.NewError(kafka.ErrCritSysResource, "all hope is lost", true),
			expectErr: true,
		},
	}

	cbCalled := 0
	cb := func(err error) {
		cbCalled++
	}

	consumer := &Consumer{
		logger: NopLogger(),
		conf: Config{
			OnError: cb,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := consumer.handleError(test.arg)
			assert.Equal(t, test.expectErr, err != nil)
		})
	}

	assert.Equal(t, 2, cbCalled)
}

func TestConsumer_handleOffsetsCommitted(t *testing.T) {
	type test struct {
		name string
		args kafka.OffsetsCommitted
	}

	tests := []test{
		{
			name: "Offset Commit Error",
			args: kafka.OffsetsCommitted{
				Error: kafka.NewError(kafka.ErrAllBrokersDown, "all brokers down", false),
				Offsets: []kafka.TopicPartition{
					{
						Topic:     StringPtr("test"),
						Partition: 0,
						Offset:    0,
						Error:     kafka.NewError(kafka.ErrAllBrokersDown, "all brokers down", false),
					},
				},
			},
		},
		{
			name: "Offset Commit Success",
			args: kafka.OffsetsCommitted{
				Error: nil,
				Offsets: []kafka.TopicPartition{
					{
						Topic:     StringPtr("test"),
						Partition: 0,
						Offset:    kafka.Offset(1001),
						Error:     nil,
					},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			consumer := &Consumer{
				logger: NopLogger(),
			}
			consumer.handleOffsetsCommitted(test.args)
		})
	}
}

func TestConsumer_Lag(t *testing.T) {
	type test struct {
		name         string
		consumerInit func() baseConsumer
		expected     map[string]int64
	}

	tests := []test{
		{
			name: "With Assigned Partitions",
			consumerInit: func() baseConsumer {
				consumer := new(mockBaseConsumer)

				assignmentReturn := []kafka.TopicPartition{
					{
						Topic:     StringPtr("test"),
						Partition: 0,
						Offset:    0,
					},
					{
						Topic:     StringPtr("test"),
						Partition: 1,
						Offset:    0,
					},
				}
				consumer.On("Assignment").Return(assignmentReturn, nil)

				committedReturn := []kafka.TopicPartition{
					{
						Topic:     StringPtr("test"),
						Partition: 0,
						Offset:    1001,
					},
					{
						Topic:     StringPtr("test"),
						Partition: 1,
						Offset:    2312,
					},
				}
				consumer.On("Committed", mock.Anything, mock.Anything).Return(committedReturn, nil)

				consumer.On("QueryWatermarkOffsets", mock.Anything, mock.Anything, mock.Anything).Return(int64(141), int64(3000), nil).Once()
				consumer.On("QueryWatermarkOffsets", mock.Anything, mock.Anything, mock.Anything).Return(int64(163), int64(3000), nil).Once()

				return consumer
			},
			expected: map[string]int64{
				"test|0": 1999,
				"test|1": 688,
			},
		},
		{
			name: "Without Assigned Partitions",
			consumerInit: func() baseConsumer {
				consumer := new(mockBaseConsumer)

				assignmentReturn := []kafka.TopicPartition{}
				consumer.On("Assignment").Return(assignmentReturn, nil)

				return consumer
			},
			expected: map[string]int64{},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			consumer := &Consumer{
				base: test.consumerInit(),
			}

			lag, err := consumer.Lag()
			assert.NoError(t, err)
			assert.Equal(t, test.expected, lag)
		})
	}
}
