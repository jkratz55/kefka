package kefka

import (
	"fmt"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/jkratz55/slices"
)

// ErrorCallback is a function that is invoked with an error value when an error
// occurs processing messages from Kafka.
type ErrorCallback func(err error)

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

type ConsumerOptions struct {
	KafkaConfig  *kafka.ConfigMap
	Handler      MessageHandler
	Topic        string
	PollTimeout  time.Duration
	ErrorHandler ErrorCallback
	Logger       Logger
}

type Consumer struct {
	baseConsumer *kafka.Consumer
	handler      MessageHandler
	pollTimeout  time.Duration
	errorHandler ErrorCallback
	logger       Logger
	topic        string

	close   sync.Once
	running bool
	termCh  chan struct{}
}

func NewConsumer(opts ConsumerOptions) (*Consumer, error) {
	// Sanity check on API usage, these are required fields and documented as such.
	// If the API is being misused or not provided the documented required properties
	// just panic.
	if opts.KafkaConfig == nil {
		panic("")
	}
	if opts.Handler == nil {
		panic("")
	}
	if opts.Topic == "" {
		panic("")
	}

	// Use sane defaults if properties/fields not provided.
	if opts.PollTimeout == 0 {
		opts.PollTimeout = time.Second * 10
	}

	baseConsumer, err := kafka.NewConsumer(opts.KafkaConfig)
	if err != nil {
		return nil, fmt.Errorf("unable to create Confluent Kafka Consumer with provided config: %w", err)
	}
	err = baseConsumer.Subscribe(opts.Topic, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to subscribe to topic %s: %w", opts.Topic, err)
	}

	consumer := &Consumer{
		baseConsumer: baseConsumer,
		handler:      opts.Handler,
		pollTimeout:  opts.PollTimeout,
		errorHandler: opts.ErrorHandler,
		logger:       opts.Logger,
		topic:        opts.Topic,
		close:        sync.Once{},
		running:      false,
		termCh:       make(chan struct{}),
	}
	return consumer, nil
}

func (c *Consumer) Start() {
	if c.running {
		panic("consumer is already running")
	}
	for {
		select {
		case <-c.termCh:
			return
		default:
			c.readMessage()
		}
	}
}

// Lag returns the current lag for the consumer as a map where the key is the
// topic|partition and the value is the current lag.
func (c *Consumer) Lag() (map[string]int64, error) {

}

func (c *Consumer) Close() {
	if !c.running {
		panic("cannot close a Consumer that isn't running, illegal use of API")
	}
	c.close.Do(func() {
		c.running = false
		c.termCh <- struct{}{}
		c.baseConsumer.Close()
	})
}

func (c *Consumer) readMessage() {
	msg, err := c.baseConsumer.ReadMessage(c.pollTimeout)
	if err != nil {
		c.errorHandler(fmt.Errorf("error reading message from Kafka: %w", err))
		return
	}
	ack := func() {
		if _, err := c.baseConsumer.Commit(); err != nil && c.errorHandler != nil {
			c.errorHandler(fmt.Errorf("error committing offsets to Kafka for offset %d: %w", msg.TopicPartition.Offset, err))
		}
	}
	c.handler.Handle(msg, ack)
}

type Reader struct {
	base    *kafka.Consumer
	handler MessageHandler
}

func (r *Reader) Read(topicParitions []kafka.TopicPartition) error {
	err := r.base.Assign(topicParitions)
	if err != nil {
		return fmt.Errorf("error assigning topics/patitions: %w", err)
	}

	for {
		// r.base.
	}
}

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

// LagForTopicPartition fetches the current lag for a given consumer, topic
// and partition.
//
// Lag is meant to be used when working in a consumer group. To fetch how
// many messages are in a given topic/partition use MessageCount instead.
func LagForTopicPartition(client *kafka.Consumer, topic string, partition int) (int64, error) {
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
