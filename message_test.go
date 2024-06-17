package kefka

import (
	"encoding/json"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/assert"
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
