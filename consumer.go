package kefka

import (
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/jkratz55/slices"
)

type ErrorCallback func(err error)

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

// MessageHandler is a type that handles/processes messages from Kafka.
//
// Implementations of MessageHandler are expected to perform all processing and
// business logic around the received message. Implementations of MessageHandler
// should handle any and all retry logic, error handling, etc.
//
// Implementations of MessageHandler should call the Commit function when using
// manual/synchronous commits with Kafka. Otherwise, it should not be called as
// it can have negative impacts on throughput/performance.
type MessageHandler interface {
	Handle(msg *kafka.Message, ack Commit)
}

// MessageHandlerFunc is a convenient way to satisfy the MessageHandler interface
// without creating a type.
//
// See MessageHandler for more details.
type MessageHandlerFunc func(msg *kafka.Message, ack Commit)

func (m MessageHandlerFunc) Handle(msg *kafka.Message, ack Commit) {
	m(msg, ack)
}

// CancelFunc is a function used to cancel Consumer operations
type CancelFunc func()

// Commit is a function that commits the offsets to Kafka synchronously. Using
// synchronous writes can significantly impact throughput and should be used
// sparingly.
//
// If using auto commit there is no need to invoke this function.
type Commit func()

// Consume uses the provided Consumer and reads messages from Kafka in a separate
// goroutine. A CancelFunc is returned to cancel/stop consumption of messages from
// Kafka.
//
// The consumer and handler parameters are mandatory, while the ErrorCallback is
// optionally. Providing an ErrorCallback is highly recommended, otherwise error
// will end up in the void.
//
// Calling Close on kafka.Consumer will cause all hell to break loose. Ensure
// the CancelFunc is called first, and then the kafka.Consumer can be safely
// closed.
func Consume(consumer *kafka.Consumer, handler MessageHandler, errCb ErrorCallback) CancelFunc {
	termChan := make(chan struct{}, 1)
	termed := false
	cancel := CancelFunc(func() {
		if !termed {
			termChan <- struct{}{}
			termed = true
		}
	})

	go func() {
		for {
			select {
			case <-termChan:
				return
			default:
				msg, err := consumer.ReadMessage(time.Second * 10)
				if err != nil {
					if errCb != nil {
						errCb(err)
					}
					continue
				}
				ack := func() {
					if _, err := consumer.Commit(); err != nil && errCb != nil {
						errCb(err)
					}
				}
				handler.Handle(msg, ack)
			}
		}
	}()

	return cancel
}

// Lag fetches the current lag for a given consumer, topic and partition.
//
// Lag is meant to be used when working in a consumer group. To fetch how
// many messages are in a given topic/partition use MessageCount instead.
func Lag(client *kafka.Consumer, topic string, partition int) (int64, error) {
	partitions, err := client.Assignment()
	if err != nil {
		return 0, fmt.Errorf("error querying assignments: %w", err)
	}

	partitions, err = client.Committed(partitions, 10000)
	if err != nil {
		return 0, fmt.Errorf("error querying committed offsets: %w", err)
	}

	low, high, err := client.QueryWatermarkOffsets(topic, int32(partition), 10000)
	if err != nil {
		return 0, fmt.Errorf("error querying watermark offsets: %w", err)
	}

	p, ok := slices.FindFirst(partitions, func(p kafka.TopicPartition) bool {
		if *p.Topic == topic && p.Partition == int32(partition) {
			return true
		}
		return false
	})
	if !ok {
		return high - low, nil
	}
	return high - int64(p.Offset), nil
}

// MessageCount returns the count of messages for a given topic/partition.
func MessageCount(client *kafka.Consumer, topic string, partition int) (int64, error) {
	low, high, err := client.QueryWatermarkOffsets(topic, int32(partition), 10000)
	if err != nil {
		return 0, fmt.Errorf("error querying watermark offsets: %w", err)
	}
	return high - low, nil
}
