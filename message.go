package kefka

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// MessageBuilder represents a Kafka message and provides a fluent API for
// constructing a message to be produced to Kafka.
type MessageBuilder struct {
	producer *Producer
	topic    string
	key      string
	value    []byte
	headers  []Header
	opaque   interface{}
	err      error
}

// Topic sets the topic for the message.
func (m *MessageBuilder) Topic(t string) *MessageBuilder {
	m.topic = t
	return m
}

// Key sets the key for the message.
func (m *MessageBuilder) Key(k string) *MessageBuilder {
	m.key = k
	return m
}

// Value sets the value for the message.
func (m *MessageBuilder) Value(v []byte) *MessageBuilder {
	m.value = v
	return m
}

// JSON serializes the provided value to JSON and sets it as the value for the
// message.
func (m *MessageBuilder) JSON(v any) *MessageBuilder {
	data, err := json.Marshal(v)
	m.err = err
	m.value = data
	return m
}

// Header adds a header to the message.
func (m *MessageBuilder) Header(key string, value []byte) *MessageBuilder {
	m.headers = append(m.headers, Header{Key: key, Value: value})
	return m
}

// Opaque sets the opaque value for the message.
func (m *MessageBuilder) Opaque(o interface{}) *MessageBuilder {
	m.opaque = o
	return m
}

// Send produces a message to Kafka asynchronously and returns immediately if
// the message was enqueued successfully, otherwise returns an error. The delivery
// report is delivered on the provided channel. If the channel is nil than Send
// operates as fire-and-forget.
func (m *MessageBuilder) Send(deliveryChan chan kafka.Event) error {
	if m.err != nil {
		return m.err
	}
	if strings.TrimSpace(m.topic) == "" {
		return errors.New("invalid message: no topic")
	}

	msg, err := m.Message()
	if err != nil {
		return fmt.Errorf("kafka: build message: %w", err)
	}

	err = m.producer.base.Produce(msg, deliveryChan)
	if err != nil {
		producerMessagesEnqueueFailures.WithLabelValues(m.topic).Inc()
		return RetryableError(fmt.Errorf("kafka: enqueue message: %w", err))
	}
	producerMessagesEnqueued.WithLabelValues(m.topic).Inc()
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
		return err
	}

	e := <-deliveryChan
	switch ev := e.(type) {
	case *kafka.Message:
		if ev.TopicPartition.Error != nil {
			producerMessageDeliveryFailures.WithLabelValues(m.topic).Inc()
			return fmt.Errorf("kafka delivery failure: %w", ev.TopicPartition.Error)
		}
		producerMessagesDelivered.WithLabelValues(m.topic).Inc()
	case kafka.Error:
		producerKafkaErrors.WithLabelValues(ev.Code().String()).Inc()
		return fmt.Errorf("kafka error: %w", ev)
	default:
		return fmt.Errorf("unexpected kafka event: %T", e)
	}

	return nil
}

// Message builds and returns a *kafka.Message instance from the Confluent Kafka
// library.
func (m *MessageBuilder) Message() (*kafka.Message, error) {
	if m.err != nil {
		return nil, m.err
	}
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

	return msg, nil
}

// Err returns the last error that occurred while building the message or nil
// if there were no errors.
func (m *MessageBuilder) Err() error {
	return m.err
}

// Header represents a Kafka message header.
type Header struct {
	Key   string
	Value []byte
}
