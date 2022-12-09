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

	// Allows plugging in a third party Logger. By default, the Logger from the
	// standard library will be used if one is not provided at INFO level.
	Logger Logger
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
	logger       Logger

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
		opts.PollTimeout = DefaultPollTimeout
	}
	// Create a default logger if one was not provided
	if opts.Logger == nil {
		opts.Logger = defaultLogger()
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
		logger:       opts.Logger,
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
		c.logger.Printf(WarnLevel, "Consumer already running, call to Consumer is a no-op")
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
		switch e := err.(type) {
		case kafka.Error:
			c.logger.Printf(ErrorLevel, "error polling from Kafka: %s Code: %d", e.Error(), e.Code())
		default:
			c.logger.Printf(ErrorLevel, "error polling from Kafka: %s", err)
		}
		c.errorHandler(err)
		return
	}
	ack := func() {
		if _, err := c.baseConsumer.Commit(); err != nil && c.errorHandler != nil {
			c.logger.Printf(ErrorLevel, "error committing offset to Kafka, Topic %s Partition %d Offset %d Err %s",
				*msg.TopicPartition.Topic, msg.TopicPartition.Partition, msg.TopicPartition.Offset, err)
			c.errorHandler(err)
		} else {
			c.logger.Printf(DebugLevel, "Successfully committed offset %d for topic %s partition %d",
				msg.TopicPartition.Offset, *msg.TopicPartition.Topic, msg.TopicPartition.Partition)
		}
	}
	start := time.Now()
	c.handler.Handle(msg, ack)
	c.logger.Printf(DebugLevel, "Executed MessageHandler for message with offset %d, topic %s partition %d in %d seconds",
		msg.TopicPartition.Offset, *msg.TopicPartition.Topic, msg.TopicPartition.Partition, time.Since(start).Seconds())
}

var nopCommit = func() {}

// PartitionEOFCallback is a function type that is invoked when the end of the
// partition is reached.
type PartitionEOFCallback func(topic string, partition int, offset int64)

// ReaderOptions is a type representing the configuration options to instantiate
// and initialize a Reader.
//
// Note: There are fields that are mandatory and if not provided will result in
// a panic.
type ReaderOptions struct {
	// The Kafka configuration that is used to create the underlying Confluent Kafka
	// Consumer type. This is a required field. A zero value (nil) will cause a panic.
	KafkaConfig *kafka.ConfigMap
	// The handler that will be handed the message from Kafka and process it. This is
	// a required field. A zero value (nil) will cause a panic.
	MessageHandler MessageHandler
	// An optional callback that is invoked when an error occurs polling/reading
	// messages from Kafka.
	//
	// While optional, it is highly recommended to provide an ErrorCallback.
	// Otherwise, errors from the underlying Confluent Kafka Consumer are discarded.
	// Ideally, these errors should be logged and/or capture metrics.
	ErrorCallback ErrorCallback
	// An optional callback that is invoked when the Reader reaches the end of a
	// topic/partition.
	PartitionEOFCallback PartitionEOFCallback
	// The topics and partitions to be read. This is a required field and at least
	// one TopicPartition must be supplied. Each TopicPartition should provide the
	// topic, partition, and starting offset. It is important to note the starting
	// offset of 0 will default to the latest offset if 0 is not a valid offset.
	// Optionally the values FirstOffset and LastOffset can be passed to start at
	// the beginning or end of the partition respectively.
	//
	// Example:
	//	topic := "test"
	//	TopicPartitions: []kafka.TopicPartition{
	//		{
	//			Topic:     &topic,
	//			Partition: 0,
	//			Offset:    kefka.FirstOffset,
	//		},
	//	},
	TopicPartitions kafka.TopicPartitions
	// Configures the timeout polling messages from Kafka. This field is optional.
	// If the zero-value is provided DefaultPollTimeout will be used.
	PollTimeout time.Duration
	// Allows plugging in a third party Logger. By default, the Logger from the
	// standard library will be used if one is not provided at INFO level.
	Logger Logger
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
		opts.PollTimeout = DefaultPollTimeout
	}
	if opts.Logger == nil {
		opts.Logger = defaultLogger()
	}

	consumer, err := kafka.NewConsumer(opts.KafkaConfig)
	if err != nil {
		return fmt.Errorf("unable to create Confluent Kafka Consumer with provided config: %w", err)
	}

	err = consumer.Assign(opts.TopicPartitions)
	if err != nil {
		return fmt.Errorf("error assigning topics/patitions: %w", err)
	}
	defer func(consumer *kafka.Consumer) {
		err := consumer.Unassign()
		if err != nil {
			opts.Logger.Printf(WarnLevel, "Error unassigning topic/partitions: %s", err)
		}
	}(consumer)
	defer func(consumer *kafka.Consumer) {
		err := consumer.Close()
		if err != nil {
			opts.Logger.Printf(WarnLevel, "Error closing base Kafka Consumer: %s", err)
		}
	}(consumer)

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

// Reader is a type for reading messages from Kafka topics/partitions.
//
// Under the hood Reader uses Confluent Kafka consumer type. Reader is in
// essence a wrapper around the Confluent Kafka Go library for reading messages
// outside a consumer group.
//
// The zero value of Reader is not usable. Instances of Reader should be
// created using the NewReader function
//
// In contrast to Consumer, Reader is meant to serve use cases where messages
// are read from Kafka but not consumed. In other words, offsets are not committed
// back to Kafka, and it is safe for multiple instances to read the same messages
// possibly over and over. As messages are read from Kafka they are passed to a
// provided MessageHandler to handle/process the message. A noop Commit is passed
// to the MessageHandler, so even if its called it has no effect.
//
// In order to guarantee the behavior of Reader certain Kafka configuration properties
// are overridden and cannot be altered.
//
//	enable.partition.eof -> true
//	enable.auto.commit -> false
//	group.id -> kefkareader
type Reader struct {
	base           *kafka.Consumer
	handler        MessageHandler
	errorCb        ErrorCallback
	partitionEOFCb PartitionEOFCallback
	pollTimeout    int
	logger         Logger

	term chan struct{}
}

// NewReader creates and initializes a new ready to use instance of Reader.
//
// If the API is misused (missing required fields, not setting group.id in
// the Kafka config, etc.) this function will panic. If the Reader cannot
// be created with the provided configuration or assigning the topics/partitions
// fails a non-nil error value will be returned.
func NewReader(opts ReaderOptions) (*Reader, error) {

	// Sanity check on API usage, these are required fields and documented as such.
	// If the API is being misused or not provided the documented required properties
	// just panic.
	if opts.KafkaConfig == nil {
		panic("Kafka ConfigMap is required, illegal use of API")
	}
	if opts.MessageHandler == nil {
		panic("MessageHandler is required, illegal use of API")
	}
	if len(opts.TopicPartitions) == 0 {
		panic("Topics and partitions to read must be specified, illegal use of API")
	}

	// Overrides configurations required for this function to work as designed.
	_ = opts.KafkaConfig.SetKey("enable.partition.eof", true)
	_ = opts.KafkaConfig.SetKey("enable.auto.commit", false)
	_ = opts.KafkaConfig.SetKey("group.id", "kefkareader")

	// Use default poll timeout if one wasn't provided
	if opts.PollTimeout == 0 {
		opts.PollTimeout = DefaultPollTimeout
	}
	if opts.Logger == nil {
		opts.Logger = defaultLogger()
	}

	consumer, err := kafka.NewConsumer(opts.KafkaConfig)
	if err != nil {
		return nil, fmt.Errorf("unable to create Confluent Kafka Consumer with provided config: %w", err)
	}

	err = consumer.Assign(opts.TopicPartitions)
	if err != nil {
		return nil, fmt.Errorf("error assigning topics/patitions: %w", err)
	}

	return &Reader{
		base:           consumer,
		handler:        opts.MessageHandler,
		errorCb:        opts.ErrorCallback,
		partitionEOFCb: opts.PartitionEOFCallback,
		pollTimeout:    int(opts.PollTimeout.Milliseconds()),
		logger:         opts.Logger,
		term:           make(chan struct{}),
	}, nil
}

// Read begins polling Kafka for messages/events and passing the messages to the
// configured MessageHandler.
//
// This method is blocking and will run until Close is called. In most cases this
// method should be called on a new goroutine.
//
// This method should never be called more than once.
func (r *Reader) Read() {
	for {
		select {
		case <-r.term:
			return
		default:
			e := r.base.Poll(r.pollTimeout)
			switch e.(type) {
			case *kafka.Message:
				// Passes the message to the MessageHandler with a no-op Commit func.
				// Even if the handler some reason calls commit it won't have any
				// effect.
				msg := e.(*kafka.Message)
				r.handler.Handle(msg, nopCommit)
			case kafka.Error:
				// If an error callback was registered it will be called with the
				// error. Otherwise, we drop the error on the floor and move on.
				if r.errorCb != nil {
					err := e.(kafka.Error)
					r.errorCb(err)
				}
			case kafka.PartitionEOF:
				// If the partition EOF was registered it will be called with the
				// topic, partition, and offset notifying the callback the end of
				// the partition has been reached.
				if r.partitionEOFCb != nil {
					tp := e.(kafka.PartitionEOF)
					r.partitionEOFCb(*tp.Topic, int(tp.Partition), int64(tp.Offset))
				}
			default:
				// other events are ignored
				r.logger.Printf(DebugLevel, "Event %s ignored", e)
			}
		}
	}
}

// Close stops the Consumer. After calling Close the Consumer is no longer usable.
func (r *Reader) Close() error {
	r.term <- struct{}{}
	return r.base.Close()
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
				msg, err := consumer.ReadMessage(DefaultPollTimeout)
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
