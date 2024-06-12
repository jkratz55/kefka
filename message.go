package kefka

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type MessageBuilder struct {
	producer *Producer
	topic    string
	key      string
	value    []byte
	headers  []Header
	opaque   interface{}
	err      error
}

func (m *MessageBuilder) Topic(t string) *MessageBuilder {
	m.topic = t
	return m
}

func (m *MessageBuilder) Key(k string) *MessageBuilder {
	m.key = k
	return m
}

func (m *MessageBuilder) Value(v []byte) *MessageBuilder {
	m.value = v
	return m
}

func (m *MessageBuilder) JSON(v any) *MessageBuilder {
	data, err := json.Marshal(v)
	m.err = err
	m.value = data
	return m
}

func (m *MessageBuilder) Header(key string, value []byte) *MessageBuilder {
	m.headers = append(m.headers, Header{Key: key, Value: value})
	return m
}

func (m *MessageBuilder) Opaque(o interface{}) *MessageBuilder {
	m.opaque = o
	return m
}

func (m *MessageBuilder) Send(deliveryChan chan kafka.Event) error {
	if m.err != nil {
		return m.err
	}
	if strings.TrimSpace(m.topic) == "" {
		return errors.New("invalid message: no topic")
	}

	msg := m.Message()
	err := m.producer.producer.Produce(msg, deliveryChan)
	if err != nil {
		return fmt.Errorf("kafka: enqueue message: %w", err)
	}
	return nil
}

// SendAndWait produces a message to Kafka and waits for the delivery report.
//
// This method is blocking and will wait until the delivery report is received
// from Kafka.
func (m *MessageBuilder) SendAndWait() error {
	deliveryChan := make(chan kafka.Event)
	defer close(deliveryChan)

	err := m.Send(deliveryChan)
	if err != nil {
		return fmt.Errorf("kafka: enqueue message: %w", err)
	}

	e := <-deliveryChan
	switch ev := e.(type) {
	case *kafka.Message:
		if ev.TopicPartition.Error != nil {
			return fmt.Errorf("kafka delivery failure: %w", ev.TopicPartition.Error)
		}
	case kafka.Error:
		return fmt.Errorf("kafka error: %w", ev)
	default:
		return fmt.Errorf("unexpected kafka event: %T", e)
	}

	return nil
}

// Message builds and returns a *kafka.Message instance from the Confluent Kafka
// library.
func (m *MessageBuilder) Message() *kafka.Message {
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &m.topic,
			Partition: kafka.PartitionAny,
		},
		Value:  m.value,
		Opaque: m.opaque,
	}
	if m.key != "" {
		msg.Key = []byte(m.key)
	}

	if len(m.headers) > 0 {
		headers := make([]kafka.Header, len(m.headers))
		for i, h := range m.headers {
			headers[i] = kafka.Header{
				Key:   h.Key,
				Value: h.Value,
			}
		}
		msg.Headers = headers
	}

	return msg
}

type Header struct {
	Key   string
	Value []byte
}
