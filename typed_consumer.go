package kefka

import (
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type TypedMessage[K, V any] struct {
	Key       K
	Value     V
	Topic     string
	Partition int
	Offset    int64
	Timestamp time.Time
	Headers   []Header
}

type Header struct {
	Key   string
	Value []byte
}

type TypedMessageHandler[K, V any] interface {
	Handle(msg TypedMessage[K, V]) error
}

type TypedConsumer[K, V any] struct {
	baseConsumer      *kafka.Consumer
	keyUnmarshaller   UnmarshallFunc
	valueUnmarshaller UnmarshallFunc
	handler           TypedMessageHandler[K, V]
	errorCallback     ErrorCallback
	quit              chan struct{}
}

func (c *TypedConsumer[K, V]) Start() {
	for {
		select {
		case <-c.quit:
			return
		default:
			msg, err := c.baseConsumer.ReadMessage(time.Second * 10)
			if err != nil {
				c.errorCallback(err)
				continue
			}
			typedMessage, err := c.mapMessage(msg)
			if err != nil {
				c.errorCallback(err)
			}
			_ = c.handler.Handle(typedMessage)
		}
	}
}

func (c *TypedConsumer[K, V]) Close() {
	c.quit <- struct{}{}
	_ = c.baseConsumer.Close()
}

func (c *TypedConsumer[K, V]) mapMessage(msg *kafka.Message) (TypedMessage[K, V], error) {
	headers := make([]Header, 0)
	for _, header := range msg.Headers {
		headers = append(headers, Header{
			header.Key,
			header.Value,
		})
	}
	var key K
	err := c.keyUnmarshaller(msg.Key, &key)
	if err != nil {
		return TypedMessage[K, V]{}, fmt.Errorf("error unmarshalling key: %w", err)
	}
	var val V
	err = c.valueUnmarshaller(msg.Value, &val)
	if err != nil {
		return TypedMessage[K, V]{}, fmt.Errorf("error unmarshalling value: %w", err)
	}
	typedMessage := TypedMessage[K, V]{
		Key:       key,
		Value:     val,
		Topic:     *msg.TopicPartition.Topic,
		Partition: int(msg.TopicPartition.Partition),
		Offset:    int64(msg.TopicPartition.Offset),
		Timestamp: msg.Timestamp,
		Headers:   headers,
	}
	return typedMessage, nil
}
