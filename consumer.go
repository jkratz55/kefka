package kefka

import (
	"context"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/jkratz55/slices"
)

const (
	// DefaultPollTimeout is the default timeout when polling events from Kafka.
	DefaultPollTimeout = time.Second * 10
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

// ConsumerOptions contains the configuration options to instantiate and initialize
// a Consumer.
//
// Note: Some of the fields are required and will cause a panic. Take note of the GoDoc
// comments for each of the fields in the struct.
type ConsumerOptions struct {

	// The Kafka configuration that is used to create the underlying Confluent Kafka
	// Consumer type. This is a required field. A zero value (nil) will cause a panic.
	KafkaConfig *kafka.ConfigMap

	// The handler that will be handed the message from Kafka and process it. This is
	// a required field. A zero value (nil) will cause a panic.
	Handler MessageHandler

	// The topic to consume. This is a required field. A zero value ("") will cause
	// a panic.
	Topic string

	// Configures the timeout polling messages from Kafka. This field is optional.
	// If the zero-value is provided DefaultPollTimeout will be used.
	PollTimeout time.Duration

	// An optional callback that is invoked when an error occurs polling/reading
	// messages from Kafka.
	//
	// While optional, it is highly recommended to provide an ErrorCallback.
	// Otherwise, errors from the underlying Confluent Kafka Consumer are discarded.
	// Ideally, these errors should be logged and/or capture metrics.
	ErrorHandler ErrorCallback
}

// Consumer is a type for consumer messages from Kafka.
//
// Under the hood Consumer uses Confluent Kafka consumer type. Consumer is in
// essence a wrapper around the Confluent Kafka Go library.
//
// The zero value of Consumer is not usable. Instances of Consumer should be
// created using the NewConsumer function.
//
// The Consumer type is only meant to be used when using Kafka consumer groups.
// If not using Consumer groups use the Reader type instead. The `group.id`
// property must be set in the Kafka ConfigMap.
//
// The Kafka configuration provided is very important to the behavior of the
// Consumer. The Consumer type does not support all the features of the underlying
// Confluent Kafka client. It does however, support manual commits if that behavior
// is required. The MessageHandler accepts a Kafka message and a Commit func. If
// auto commits are disabled the caller must invoke the Commit function when they
// want to commit offsets back to Kafka. If auto commits are on the Commit function
// can be ignored. In fact, it should not be called at all if using auto commits.
type Consumer struct {
	baseConsumer *kafka.Consumer
	handler      MessageHandler
	pollTimeout  time.Duration
	errorHandler ErrorCallback
	topic        string

	running bool
	termCh  chan struct{}
}

// NewConsumer creates and initializes a new instance of the Consumer type.
//
// If the API is misused (missing required fields, not setting group.id in
// the Kafka config, etc.) this function will panic. If the Consumer cannot
// be created with the provided configuration or subscribing to the provided
// topic fails a non-nil error value will be returned.
func NewConsumer(opts ConsumerOptions) (*Consumer, error) {
	// Sanity check on API usage, these are required fields and documented as such.
	// If the API is being misused or not provided the documented required properties
	// just panic.
	if opts.KafkaConfig == nil {
		panic("Kafka ConfigMap is required, illegal use of API")
	}
	if opts.Handler == nil {
		panic("MessageHandler is required, illegal use of API")
	}
	if opts.Topic == "" {
		panic("Topic is required, illegal use of API")
	}
	if groupId, _ := opts.KafkaConfig.Get("group.id", ""); groupId == "" {
		panic("group.id is a required property in the Kafka configuration")
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
		topic:        opts.Topic,
		running:      false,
		termCh:       make(chan struct{}),
	}
	return consumer, nil
}

// Consume begins polling Kafka for messages/events passing the read messages off
// to the provided MessageHandler.
//
// This function is blocking and in most use cases it should be called in a
// separate goroutine. It will continue to run until Close is called or the
// program exits.
func (c *Consumer) Consume() {
	if c.running {
		return
	}
	c.running = true
	for {
		select {
		case <-c.termCh:
			return
		default:
			c.readMessage()
		}
	}
}

// Assignments fetches and returns the currently assigned topics and partitions
// for the Consumer.
func (c *Consumer) Assignments() ([]kafka.TopicPartition, error) {
	return c.baseConsumer.Assignment()
}

// Close stops polling messages/events from Kafka and cleans up resources including
// calling Close on the underlying Confluent Kafka consumer. Once Close has been
// called the instance of Consumer is no longer usable.
//
// This function should only be called once.
func (c *Consumer) Close() error {
	if c.running {
		c.running = false
		c.termCh <- struct{}{}
	}
	return c.baseConsumer.Close()
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

var nopCommit = func() {}

type PartitionEOFCallback func(topic string, partition int, offset int64)

type ReaderOptions struct {
	KafkaConfig          *kafka.ConfigMap
	MessageHandler       MessageHandler
	ErrorCallback        ErrorCallback
	PartitionEOFCallback PartitionEOFCallback
	TopicPartitions      kafka.TopicPartitions
	PollTimeout          time.Duration
}

// ReadTopicPartitions consumes messages from Kafka outside a consumer group. A
// new Confluent Kafka consumer is created from the configuration provided but
// ReadTopicPartitions automatically will add or overwrite specific values to
// ensure it guarantees certain behaviors. The following Kafka configuration
// cannot be overridden.
//
//	enable.partition.eof -> true
//	enable.auto.commit -> false
//	group.id -> kefkareader
//
// ReadTopicPartitions is blocking and will run forever unless the context is
// either cancelled or exceeds a deadline. In most use cases you'll want to
// call ReadTopicPartitions from a new goroutine.
//
// ReadTopicPartitions is capable or consuming multiple topics/partitions. But,
// the through put will likely be higher if this function is used with one topic
// and one partition.
func ReadTopicPartitions(ctx context.Context, opts ReaderOptions) error {

	// Overrides configurations required for this function to work as designed.
	_ = opts.KafkaConfig.SetKey("enable.partition.eof", true)
	_ = opts.KafkaConfig.SetKey("enable.auto.commit", false)
	_ = opts.KafkaConfig.SetKey("group.id", "kefkareader")

	// Use default poll timeout if one wasn't provided
	if opts.PollTimeout == 0 {
		opts.PollTimeout = time.Second * 10
	}

	consumer, err := kafka.NewConsumer(opts.KafkaConfig)
	if err != nil {
		return fmt.Errorf("unable to create Confluent Kafka Consumer with provided config: %w", err)
	}

	err = consumer.Assign(opts.TopicPartitions)
	if err != nil {
		return fmt.Errorf("error assigning topics/patitions: %w", err)
	}
	defer consumer.Unassign()
	defer consumer.Close()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			e := consumer.Poll(int(opts.PollTimeout.Milliseconds()))
			switch e.(type) {
			case *kafka.Message:
				// Passes the message to the MessageHandler with a no-op Commit func.
				// Even if the handler some reason calls commit it won't have any
				// effect.
				msg := e.(*kafka.Message)
				opts.MessageHandler.Handle(msg, nopCommit)
			case kafka.Error:
				// If an error callback was registered it will be called with the
				// error. Otherwise, we drop the error on the floor and move on.
				if opts.ErrorCallback != nil {
					err := e.(kafka.Error)
					opts.ErrorCallback(err)
				}
			case kafka.PartitionEOF:
				// If the partition EOF was registered it will be called with the
				// topic, partition, and offset notifying the callback the end of
				// the partition has been reached.
				if opts.PartitionEOFCallback != nil {
					tp := e.(kafka.PartitionEOF)
					opts.PartitionEOFCallback(*tp.Topic, int(tp.Partition), int64(tp.Offset))
				}
			}
		}
	}
}

type Reader struct {
	base           *kafka.Consumer
	handler        MessageHandler
	errorCb        ErrorCallback
	partitionEOFCb PartitionEOFCallback
}

func (r *Reader) Read(ctx context.Context, topicParitions []kafka.TopicPartition) error {
	err := r.base.Assign(topicParitions)
	if err != nil {
		return fmt.Errorf("error assigning topics/patitions: %w", err)
	}
	defer r.base.Unassign()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			e := r.base.Poll(10000)
			switch e.(type) {
			case *kafka.Message:
				// Passes the message to the MessageHandler with a no-op Commit func.
				// Even if the handler some reason calls commit it won't have any
				// effect.
				msg := e.(*kafka.Message)
				r.handler.Handle(msg, nopCommit)
			case *kafka.Error:
				// If an error callback was registered it will be called with the
				// error. Otherwise, we drop the error on the floor and move on.
				if r.errorCb != nil {
					err := e.(*kafka.Error)
					r.errorCb(err)
				}
			case *kafka.PartitionEOF:
				// If the partition EOF was registered it will be called with the
				// topic, partition, and offset notifying the callback the end of
				// the partition has been reached.
				if r.partitionEOFCb != nil {
					tp := e.(*kafka.TopicPartition)
					r.partitionEOFCb(*tp.Topic, int(tp.Partition), int64(tp.Offset))
				}
			}
		}
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
