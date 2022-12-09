package kefka

import (
	"context"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// ProducerOptions is a type containing all the configuration options to instantiate
// and initialize a Producer.
type ProducerOptions struct {
	// The Kafka configuration that is used to create the underlying Confluent Kafka
	// Producer type. This is a required field. A zero value (nil) will cause a panic.
	KafkaConfig *kafka.ConfigMap
	// Configures how the Producer will marshall the key into []byte. This is a
	// required field. A zero value (nil) will cause a panic.
	KeyMarshaller MarshallFunc
	// Configures how the Producer will marshall the value into []byte. This is
	// a required field. A zero value (nil) will cause a panic.
	ValueMarshaller MarshallFunc
	// Allows plugging in a third party Logger. By default, the Logger from the
	// standard library will be used if one is not provided at INFO level.
	Logger Logger
}

// Producer is a type that supports producing messages to a Kafka cluster. Producer
// wraps and uses Confluent Kafka producer type under the hood. Producing messages
// asynchronously and synchronously are supported.
//
// Producer can automatically handle marshalling of keys and values.
//
// The zero-value is not usable and a Producer should be instantiated with the
// NewProducer function.
type Producer struct {
	baseProducer    *kafka.Producer
	keyMarshaller   MarshallFunc
	valueMarshaller MarshallFunc
	logger          Logger
}

// NewProducer creates and initializes a new Producer.
//
// If an error occurs while creating the Confluent Kafka producer from the
// provided configuration a non-nil error value will be returned. If the
// ProducerOptions is missing required properties (misusing the API) this
// function will panic.
func NewProducer(opts ProducerOptions) (*Producer, error) {
	// If the API is being misused, IE not providing the documented required
	// fields just panic because this can never succeed.
	if opts.KafkaConfig == nil {
		panic("must provide a valid reference to Kafka ConfigMap")
	}
	if opts.KeyMarshaller == nil || opts.ValueMarshaller == nil {
		panic("must provide a MarshallFunc for key and value")
	}

	// Create the underlying Confluent Kafka producer.
	base, err := kafka.NewProducer(opts.KafkaConfig)
	if err != nil {
		return nil, fmt.Errorf("unable to create Producer with provided configuration: %w", err)
	}

	// Create a default logger if one was not provided
	if opts.Logger == nil {
		opts.Logger = defaultLogger()
	}

	producer := &Producer{
		baseProducer:    base,
		keyMarshaller:   opts.KeyMarshaller,
		valueMarshaller: opts.ValueMarshaller,
		logger:          opts.Logger,
	}

	// Start a goroutine to poll events from the producer
	go func() {
		for e := range base.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					producer.logger.Printf(ErrorLevel, "Delivery failed for message: %s", ev.TopicPartition.Error)
				} else {
					producer.logger.Printf(DebugLevel, "Delivered message to topic %s partition %d at offset %d",
						*ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset)
				}
			case kafka.Error:
				// These are generic client errors such as broker connection failures,
				// authentication issues, etc. As such, these errors are generally
				// informational as the librdkafka will automatically try to recover
				// from any errors encountered. The application does not need to take
				// action on these.
				producer.logger.Printf(ErrorLevel, "Kafka Error: %s, Code: %d", ev.Error(), ev.Code())
			default:
				producer.logger.Printf(DebugLevel, "Ignored event from Kafka: %s", ev)
			}
		}
	}()

	return producer, nil
}

// Produce produces a single message asynchronously. Messages are enqueued into
// and internal transmit queue, thus returning immediately. If the message could
// not be enqueued a non-nil error value is returned.
//
// Delivery reports are sent on the provided deliveryChan if provided. If a nil
// delivery channel is provided delivery reports are polled from the producers
// events but will not be available to the caller. In essence, if a delivery chan
// is not provided this method is fire and forget. It will ensure the message was
// enqueued but will not verify it was delivered. If the caller needs to verify
// messages were successfully delivered to the Kafka brokers a delivery chan should
// be provided listen for the delivery reports to confirm delivery.
//
// Produce automatically handles marshalling the key and value to binary using the
// provided MarshallFunc.
func (p *Producer) Produce(topic string, key any, val any, deliveryChan chan kafka.Event) error {
	keyData, err := p.keyMarshaller(key)
	if err != nil {
		return fmt.Errorf("error marshalling key: %w", err)
	}
	valData, err := p.valueMarshaller(val)
	if err != nil {
		return fmt.Errorf("error marshalling value: %w", err)
	}
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Value: valData,
		Key:   keyData,
	}
	err = p.baseProducer.Produce(msg, deliveryChan)
	if err != nil {
		return fmt.Errorf("message could not be enqued: %w", err)
	}
	return nil
}

// ProduceMessage produces a single message asynchronously. Messages are enqueued into
// and internal transmit queue, thus returning immediately. If the message could
// not be enqueued a non-nil error value is returned.
//
// Delivery reports are sent on the provided deliveryChan if provided. If a nil
// delivery channel is provided delivery reports are polled from the producers
// events but will not be available to the caller. In essence, if a delivery chan
// is not provided this method is fire and forget. It will ensure the message was
// enqueued but will not verify it was delivered. If the caller needs to verify
// messages were successfully delivered to the Kafka brokers a delivery chan should
// be provided listen for the delivery reports to confirm delivery.
//
// ProduceMessage in contrast to Produce takes a kafka.Message and doesn't handle
// marshalling the key and values. However, it provides more control over the message
// if you need to produce to a specific partition, set headers, etc.
func (p *Producer) ProduceMessage(msg *kafka.Message, deliveryChan chan kafka.Event) error {
	err := p.baseProducer.Produce(msg, deliveryChan)
	if err != nil {
		return fmt.Errorf("message could not be enqued: %w", err)
	}
	return nil
}

// SyncProduce produces a single message synchronously.
//
// Technically the underlying Confluent Kafka library doesn't directly support
// producing events synchronously. Instead, SyncProduce produces the message
// asynchronously and awaits notification of delivery on a channel. If the context
// is done before receiving on the delivery chan this method will return with an
// error but there is a possibility the message may still be delivered.
func (p *Producer) SyncProduce(ctx context.Context, topic string, key any, val any) error {
	keyData, err := p.keyMarshaller(key)
	if err != nil {
		return fmt.Errorf("error marshalling key: %w", err)
	}
	valData, err := p.valueMarshaller(val)
	if err != nil {
		return fmt.Errorf("error marshalling value: %w", err)
	}
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Value: valData,
		Key:   keyData,
	}

	ch := make(chan kafka.Event, 1)
	err = p.baseProducer.Produce(msg, ch)
	if err != nil {
		return fmt.Errorf("message could not be enqued: %w", err)
	}

	select {
	case <-ctx.Done():
		return fmt.Errorf("aborted waiting for message delivery report: %w", ctx.Err())
	case e := <-ch:
		m := e.(*kafka.Message)
		if m.TopicPartition.Error != nil {
			return fmt.Errorf("error publishing/producing message: %w", m.TopicPartition.Error)
		}
		return nil
	}
}

// SyncProduceMessage produces a single message asynchronously. SyncProduceMessage accepts
// a raw kafka.Message. SyncProduceMessage is meant to compliment SyncProduce when the caller
// needs more control over the message.
//
// Technically the underlying Confluent Kafka library doesn't directly support
// producing events synchronously. Instead, SyncProduceMessage produces the message
// asynchronously and awaits notification of delivery on a channel. If the context
// is done before receiving on the delivery chan this method will return with an
// error but there is a possibility the message may still be delivered.
func (p *Producer) SyncProduceMessage(ctx context.Context, msg *kafka.Message) error {
	ch := make(chan kafka.Event, 1)
	err := p.baseProducer.Produce(msg, ch)
	if err != nil {
		return fmt.Errorf("message could not be enqued: %w", err)
	}

	select {
	case <-ctx.Done():
		return fmt.Errorf("aborted waiting for message delivery report: %w", ctx.Err())
	case e := <-ch:
		m := e.(*kafka.Message)
		if m.TopicPartition.Error != nil {
			return fmt.Errorf("error publishing/producing message: %w", m.TopicPartition.Error)
		}
		return nil
	}
}

// Flush and wait for outstanding messages and requests to complete delivery.
// Includes messages on ProduceChannel. Runs until value reaches zero or on
// timeoutMs. Returns the number of outstanding events still un-flushed.
func (p *Producer) Flush(timeout time.Duration) int {
	return p.baseProducer.Flush(int(timeout.Milliseconds()))
}

// Close delegates to the close method on the underlying Confluent Kafka consumer
// and closes all channels. After Close has been called the instance of Producer
// is no longer usable.
//
// As a best practice Flush or FlushAll should be called before calling close to
// ensure all produce messages/events have been transmitted to the Kafka brokers
// before closing.
func (p *Producer) Close() {
	p.baseProducer.Close()
}
