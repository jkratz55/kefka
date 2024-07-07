package kefka

import (
	"context"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/jkratz55/kefka/v2/internal"
)

func TestNewReader(t *testing.T) {

	type test struct {
		name            string
		conf            Config
		handler         Handler
		topicPartitions []kafka.TopicPartition
		opts            ReaderOpts
		expectErr       bool
	}

	handler := HandlerFunc(func(msg *kafka.Message) error {
		return nil
	})

	tests := []test{
		{
			name: "Valid Configuration",
			conf: Config{
				BootstrapServers: []string{"localhost:9092"},
				GroupID:          "kefka-test",
				AutoOffsetReset:  Earliest,
			},
			handler: handler,
			topicPartitions: []kafka.TopicPartition{
				{
					Topic:     StringPtr("test"),
					Partition: 0,
				},
				{
					Topic:     StringPtr("test"),
					Partition: 1,
				},
			},
			opts:      ReaderOpts{},
			expectErr: false,
		},
		{
			name: "Invalid Configuration - Missing Handler",
			conf: Config{
				BootstrapServers: []string{"localhost:9092"},
				GroupID:          "kefka-test",
				AutoOffsetReset:  Earliest,
			},
			handler: nil,
			topicPartitions: []kafka.TopicPartition{
				{
					Topic:     StringPtr("test"),
					Partition: 0,
				},
				{
					Topic:     StringPtr("test"),
					Partition: 1,
				},
			},
			opts:      ReaderOpts{},
			expectErr: true,
		},
		{
			name: "Invalid Configuration - Empty TopicPartitions",
			conf: Config{
				BootstrapServers: []string{"localhost:9092"},
				GroupID:          "kefka-test",
				AutoOffsetReset:  Earliest,
			},
			handler:         nil,
			topicPartitions: []kafka.TopicPartition{},
			opts:            ReaderOpts{},
			expectErr:       true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reader, err := NewReader(test.conf, test.handler, test.topicPartitions, test.opts)
			if test.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, reader)
			}
		})
	}
}

func TestReader(t *testing.T) {
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

	conf := Config{
		BootstrapServers: brokers,
		Logger:           NopLogger(),
	}

	eofCalled := 0
	processedKeys := make([]string, 0)
	handler := HandlerFunc(func(msg *kafka.Message) error {
		processedKeys = append(processedKeys, string(msg.Key))
		return nil
	})
	tps := []kafka.TopicPartition{
		{
			Topic:     StringPtr("test"),
			Partition: 0,
		},
	}
	opts := ReaderOpts{
		OnEndOfPartition: func(topic string, partition int, offset int64) {
			eofCalled++
		},
		StopOnEndOfPartition: true,
	}

	reader, err := NewReader(conf, handler, tps, opts)
	assert.NoError(t, err)

	go func() {
		err := reader.Run()
		assert.NoError(t, err)
	}()

	for {
		time.Sleep(time.Millisecond * 100)
		if reader.IsClosed() {
			assert.ElementsMatch(t, keys, processedKeys)
			assert.Equal(t, 1, eofCalled)
			break
		}
	}
}
