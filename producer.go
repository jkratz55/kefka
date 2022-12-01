package kefka

import (
	"context"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// DefaultDeliveryChannelCapacity is the default size/capacity of the delivery
// channel used by the Producer type for handling asynchronous produces.
//
// This should not be confused with the DeliveryReport channel which is used
// for delivering delivery reports.
const DefaultDeliveryChannelCapacity = 10000

// DeliveryReport represents the results of producing a message asynchronously
// to Kafka.
type DeliveryReport struct {
	Topic     string
	Partition int32
	Offset    int64
	Error     error
}

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
	// An optional channel for receiving reports for asynchronous produced
	// messages/events. When producing messages asynchronously this channel
	// will notify the caller when messages have been delivered, or when an
	// error occurs.
	//
	// It is important to use a buffered channel of appropriate capacity. The
	// Producer will NOT block if the channel is applying back pressure and
	// will instead drop the report on the floor and continue.
	//
	// This channel must not be closed while the producer is in use and only
	// the Producer should ever write to this channel.
	DeliveryReportChan chan DeliveryReport
	// A Logger to log internals of the Producer. The Logger is optional but
	// recommended. In particular, it will log when delivery reports get dropped
	// due to backpressure from the DeliveryReportChan
	Logger Logger
	// Configures the capacity/size of Producer internal channel for aysnc
	// deliveries. If not provided the default DefaultDeliveryChannelCapacity
	// will be used.
	//
	// Generally the default value should suffice, but is configurable if
	// needed for throughput tuning.
	InternalDeliveryChanCapacity int
}

// Producer is a type that supports producing messages to a Kafka cluster. Producer
// wraps and uses Confluent Kafka producer type under the hood. Producing messages
// asynchronously and synchronously are supported.
//
// Producer automatically handles marshalling of keys and values.
//
// The zero-value is not usable and a Producer should be instantiated with the
// NewProducer function.
type Producer struct {
	baseProducer    *kafka.Producer
	keyMarshaller   MarshallFunc
	valueMarshaller MarshallFunc
	deliveryChan    chan kafka.Event

	deliveryReportChan chan DeliveryReport
	logger             Logger
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

	internalChanCapacity := DefaultDeliveryChannelCapacity
	if opts.InternalDeliveryChanCapacity > 0 {
		internalChanCapacity = opts.InternalDeliveryChanCapacity
	}

	producer := &Producer{
		baseProducer:       base,
		keyMarshaller:      opts.KeyMarshaller,
		valueMarshaller:    opts.ValueMarshaller,
		deliveryChan:       make(chan kafka.Event, internalChanCapacity),
		deliveryReportChan: opts.DeliveryReportChan,
		logger:             opts.Logger,
	}

	// Starts a goroutine to handle delivery reports for async produce events
	go func() {
		for e := range producer.deliveryChan {
			if producer.deliveryReportChan == nil {
				// If a delivery report channel was not provided skip to the next
				// iteration. Regardless we still have to read from the delivery
				// channel, or it can begin to put backpressure on the underlying
				// Confluent Kafka consumer.
				continue
			}
			switch ev := e.(type) {
			case *kafka.Message:
				// To prevent holding up processing of the delivery channel
				// delivery reports will be dropped on the floor if channel
				// being written to is full. Users of this package are
				// expected to choose proper sizing options according to
				// their workload/use case
				report := DeliveryReport{
					Topic:     *ev.TopicPartition.Topic,
					Partition: ev.TopicPartition.Partition,
					Offset:    int64(ev.TopicPartition.Offset),
					Error:     ev.TopicPartition.Error,
				}
				select {
				case producer.deliveryReportChan <- report:
					// wrote report to channel
				default:
					if producer.logger != nil {
						producer.logger.Printf(WarnLevel, "Delivery report channel backpressure! DeliveryReport will be dropped. Topic: %s, Partition: %d, Offset: %d, Error: %s",
							report.Topic, report.Partition, report.Offset, report.Error)
					}
				}
			case *kafka.Error:
				// To prevent holding up processing of the delivery channel
				// delivery reports will be dropped on the floor if channel
				// being written to is full. Users of this package are
				// expected to choose proper sizing options according to
				// their workload/use case
				report := DeliveryReport{Error: ev}
				select {
				case producer.deliveryReportChan <- report:
					// wrote report to channel
				default:
					if producer.logger != nil {
						producer.logger.Printf(ErrorLevel, "Delivery report channel backpressure! DeliveryReport will be dropped. Failed to deliver message: %s", report.Error)
					}
				}
			}
		}
	}()

	return producer, nil
}

// Produce single message. This is an asynchronous call that enqueues the message
// on the internal transmit queue, thus returning immediately. Delivery reports
// are delivered on the channel provided in ProducerOptions when initializes the
// Producer. If the zero-value (nil) channel was used, this is fire and forget
// with no notification mechanism.
func (p *Producer) Produce(topic string, key any, val any) error {
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
	err = p.baseProducer.Produce(msg, p.deliveryChan)
	if err != nil {
		return fmt.Errorf("message could not be enqued: %w", err)
	}
	return nil
}

// ProduceMessage produces a single message asynchronously. ProduceMessage accepts
// a raw kafka.Message. ProduceMessage is meant to compliment Produce when the caller
// needs more control over the message.
//
// Delivery reports are delivered on the channel provided in ProducerOptions when
// initializes the Producer. If the zero-value (nil) channel was used, this is fire
// and forget with no notification mechanism.
func (p *Producer) ProduceMessage(msg *kafka.Message) error {
	err := p.baseProducer.Produce(msg, p.deliveryChan)
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
	close(p.deliveryChan)
	if p.deliveryReportChan != nil {
		close(p.deliveryReportChan)
	}
}

// SyncProduce is a convenient function for producing messages synchronously.
// Technically the Confluent Kafka Producer doesn't support producing events
// synchronously. Instead, this function creates a delivery channel and wait on it
// for the delivery report/acknowledgement.
//
// SyncProduce accepts a context so that this operation can be cancelled or
// timeout. However, it is very important to note this does not cancel producing
// the message. It simply cancels waiting on the delivery report. The message still
// may be delivered.
func SyncProduce(ctx context.Context, p *kafka.Producer, topic string, key, value []byte) error {
	ch := make(chan kafka.Event, 1)
	err := p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Value: value,
		Key:   key,
	}, ch)
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

// SyncProduceMessage is a convenient function for producing messages synchronously.
// Technically the Confluent Kafka Producer doesn't support producing events
// synchronously. Instead, this function creates a delivery channel and wait on it
// for the delivery report/acknowledgement.
//
// SyncProduceMessage accepts a context so that this operation can be cancelled or
// timeout. However, it is very important to note this does not cancel producing
// the message. It simply cancels waiting on the delivery report. The message still
// may be delivered.
func SyncProduceMessage(ctx context.Context, p *kafka.Producer, msg *kafka.Message) error {
	ch := make(chan kafka.Event, 1)
	err := p.Produce(msg, ch)
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

// FlushAll will continuously call Flush on the Kafka Producer until there are
// zero messages awaiting delivery.
//
// This function is blocking and should really only be called if you need to
// force the internal queue empty. An example might be an application exiting.
func FlushAll(p *kafka.Producer) {
	for remaining := p.Flush(5000); remaining > 0; {
		// keep calling flush until it returns 0 elements remaining
	}
}
