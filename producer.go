package kefka

import (
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// DeliveryReport represents the results of producing a message asynchronously
// to Kafka.
type DeliveryReport struct {
	Topic     string
	Partition int32
	Offset    int64
	Error     error
}

type ProducerOptions struct {
	BaseProducer       *kafka.Producer
	KeyMarshaller      MarshallFunc
	ValueMarshaller    MarshallFunc
	DeliveryReportChan chan DeliveryReport
	Logger             Logger
}

type Producer struct {
	baseProducer    *kafka.Producer
	keyMarshaller   MarshallFunc
	valueMarshaller MarshallFunc
	deliveryChan    chan kafka.Event

	deliveryReportChan chan DeliveryReport
	logger             Logger
}

func NewProducer(opts ProducerOptions) *Producer {
	if opts.BaseProducer == nil {
		panic("must provide a valid reference to Kafka Producer")
	}
	if opts.KeyMarshaller == nil || opts.ValueMarshaller == nil {
		panic("must provide a MarshallFunc for key and value")
	}
	producer := &Producer{
		baseProducer:       opts.BaseProducer,
		keyMarshaller:      opts.KeyMarshaller,
		valueMarshaller:    opts.ValueMarshaller,
		deliveryChan:       make(chan kafka.Event, 10000),
		deliveryReportChan: opts.DeliveryReportChan,
		logger:             opts.Logger,
	}

	// Starts a goroutine to handle delivery reports for async produce events
	go func() {
		for e := range producer.deliveryChan {
			switch ev := e.(type) {
			case *kafka.Message:
				if producer.deliveryReportChan != nil {
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
							producer.logger.Printf(WarnLevel, "Delivery report channel backpressure! DeliveryReport will be dropped. DeliveryReport: %v", report)
						}
					}

				}
			case *kafka.Error:
				if producer.deliveryChan != nil {
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
							producer.logger.Printf(WarnLevel, "Delivery report channel backpressure! DeliveryReport containing an error will be dropped. DeliveryReport: %v", report)
						}
					}
				}
			}
		}
	}()

	return producer
}

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

func (p *Producer) ProduceAndFlush(topic string, key any, val any) error {
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
	defer close(ch)
	err = p.baseProducer.Produce(msg, ch)
	if err != nil {
		return fmt.Errorf("message could not be enqued: %w", err)
	}

	e := <-ch
	m := e.(*kafka.Message)
	if m.TopicPartition.Error != nil {
		return fmt.Errorf("error publishing/producing message: %w", m.TopicPartition.Error)
	}
	return nil
}

func (p *Producer) Flush(timeout time.Duration) {
	p.baseProducer.Flush(int(timeout.Milliseconds()))
}

func (p *Producer) Close() {
	p.baseProducer.Close()
	close(p.deliveryChan)
	if p.deliveryReportChan != nil {
		close(p.deliveryReportChan)
	}
}
