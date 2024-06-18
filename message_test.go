package kefka

import (
	"context"
	"encoding/json"
	"log"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/assert"

	"github.com/jkratz55/kefka/v2/internal"
)

func TestMessageBuilder_Message(t *testing.T) {

	topic := "test"
	payload, _ := json.Marshal(map[string]interface{}{
		"event":  "CREATE_USER",
		"userId": 1111,
	})

	tests := []struct {
		name     string
		setup    func() (*kafka.Message, error)
		expected *kafka.Message
	}{
		{
			name: "Valid Message",
			setup: func() (*kafka.Message, error) {
				mb := &MessageBuilder{}
				return mb.Topic("test").
					Key("hello").
					Value([]byte("world")).
					Header("foo", []byte("bar")).
					Opaque(42).
					Message()
			},
			expected: &kafka.Message{
				TopicPartition: kafka.TopicPartition{
					Topic:     &topic,
					Partition: kafka.PartitionAny,
				},
				Value:  []byte("world"),
				Key:    []byte("hello"),
				Opaque: 42,
				Headers: []kafka.Header{
					{
						Key:   "foo",
						Value: []byte("bar"),
					},
				},
			},
		},
		{
			name: "Valid Message - JSON",
			setup: func() (*kafka.Message, error) {
				payload := map[string]interface{}{
					"event":  "CREATE_USER",
					"userId": 1111,
				}
				mb := &MessageBuilder{}
				return mb.Topic("test").
					Key("hello").
					JSON(payload).
					Header("foo", []byte("bar")).
					Opaque(42).
					Message()
			},
			expected: &kafka.Message{
				TopicPartition: kafka.TopicPartition{
					Topic:     &topic,
					Partition: kafka.PartitionAny,
				},
				Value:  payload,
				Key:    []byte("hello"),
				Opaque: 42,
				Headers: []kafka.Header{
					{
						Key:   "foo",
						Value: []byte("bar"),
					},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual, err := test.setup()
			assert.NoError(t, err)
			assert.Equal(t, test.expected, actual)
		})
	}
}

func TestMessageBuilder_Send(t *testing.T) {

	// If TestContainers is not enabled, skip this test
	if !internal.IsTestContainersEnabled() {
		t.Skip("testcontainers not enabled")
	}

	ctx := context.Background()

	kafkaContainer, err := initKafkaTestContainer(ctx)
	if err != nil {
		log.Fatalf("failed to start container: %s", err)
	}

	// Clean up the container after
	defer func() {
		if err := kafkaContainer.Terminate(ctx); err != nil {
			log.Fatalf("failed to terminate container: %s", err)
		}
	}()

	brokers, err := kafkaContainer.Brokers(ctx)
	if err != nil {
		t.Fatalf("failed to get brokers: %s", err)
	}

	conf := Config{
		BootstrapServers: brokers,
		RequiredAcks:     AckLeader,
	}

	producer, err := NewProducer(conf)
	if err != nil {
		t.Fatalf("failed to create producer: %s", err)
	}

	ch := make(chan kafka.Event)
	err = producer.M().
		Topic("test").
		Key("hello").
		Value([]byte("world")).
		Header("foo", []byte("bar")).
		Opaque(42).
		Send(ch)
	assert.NoError(t, err)

	e := <-ch
	switch ev := e.(type) {
	case *kafka.Message:
		assert.NoError(t, ev.TopicPartition.Error)
		assert.Equal(t, "test", *ev.TopicPartition.Topic)
		assert.Equal(t, kafka.Offset(0), ev.TopicPartition.Offset)
		assert.Equal(t, 42, ev.Opaque)
		assert.Equal(t, []byte("hello"), ev.Key)
		assert.Equal(t, []byte("world"), ev.Value)
	}

	producer.Flush(time.Second * 3)
	producer.Close()
}

func TestMessageBuilder_SendAndWait(t *testing.T) {
	// If TestContainers is not enabled, skip this test
	if !internal.IsTestContainersEnabled() {
		t.Skip("testcontainers not enabled")
	}

	ctx := context.Background()

	kafkaContainer, err := initKafkaTestContainer(ctx)
	if err != nil {
		log.Fatalf("failed to start container: %s", err)
	}

	// Clean up the container after
	defer func() {
		if err := kafkaContainer.Terminate(ctx); err != nil {
			log.Fatalf("failed to terminate container: %s", err)
		}
	}()

	brokers, err := kafkaContainer.Brokers(ctx)
	if err != nil {
		t.Fatalf("failed to get brokers: %s", err)
	}

	conf := Config{
		BootstrapServers: brokers,
		RequiredAcks:     AckLeader,
	}

	producer, err := NewProducer(conf)
	if err != nil {
		t.Fatalf("failed to create producer: %s", err)
	}

	err = producer.M().
		Topic("test").
		Key("hello").
		Value([]byte("world")).
		Header("foo", []byte("bar")).
		Opaque(42).
		SendAndWait()
	assert.NoError(t, err)

	producer.Flush(time.Second * 3)
	producer.Close()
}
