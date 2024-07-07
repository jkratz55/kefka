package kefka

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type baseProducer interface {
	Produce(m *kafka.Message, deliveryChan chan kafka.Event) error
	Events() chan kafka.Event
	Flush(timeoutMs int) int
	Close()
	IsClosed() bool
	InitTransactions(ctx context.Context) error
	BeginTransaction() error
	AbortTransaction(ctx context.Context) error
	CommitTransaction(ctx context.Context) error
	Len() int
}

var _ baseProducer = &kafka.Producer{}

// Producer is a high-level type for producing messages to Kafka.
type Producer struct {
	base           baseProducer
	loggerStopChan chan struct{}
	eventStopChan  chan struct{}
}

// NewProducer creates and initializes a new Kafka Producer.
func NewProducer(conf Config) (*Producer, error) {

	// Set any default configuration values not present
	conf.init()

	// Build ConfigMap Confluent Kafka client expects
	configMap := producerConfigMap(conf)

	loggerStopChan := make(chan struct{})
	eventStopChan := make(chan struct{})
	logChan := make(chan kafka.LogEvent, 1000)

	// Start goroutine to read logs from librdkafka and uses slog.Logger to log
	// them rather than dumping them to stdout
	go func(logger *slog.Logger) {
		for {
			select {
			case logEvent, ok := <-logChan:
				if !ok {
					return
				}
				logger.Debug(logEvent.Message,
					slog.Group("librdkafka",
						slog.String("name", logEvent.Name),
						slog.String("tag", logEvent.Tag),
						slog.Int("level", logEvent.Level)))
			case <-loggerStopChan:
				return
			}
		}
	}(conf.Logger)

	// Configure logs from librdkafka to be sent to our logger rather than stdout
	_ = configMap.SetKey("go.logs.channel.enable", true)
	_ = configMap.SetKey("go.logs.channel", logChan)

	// If KEFKA_DEBUG is enabled print the Kafka configuration to stdout for
	// debugging/troubleshooting purposes.
	if ok, _ := strconv.ParseBool(os.Getenv("KEFKA_DEBUG")); ok {
		printConfigMap(configMap)
	}
	conf.Logger.Info("Initializing Kafka Producer",
		slog.Any("config", obfuscateConfig(configMap)))

	producer, err := kafka.NewProducer(configMap)
	if err != nil {
		return nil, fmt.Errorf("kafka: failed to initialize Confluent Kafka Producer: %w", err)
	}

	// Start goroutine to read events from the producer and log them if necessary.
	// If the users of this library are not providing a delivery channel the events
	// will be sent on this chanel.
	go func(logger *slog.Logger) {
		events := producer.Events()
		for {
			select {
			case rawEvent, ok := <-events:
				if !ok {
					return
				}
				switch event := rawEvent.(type) {
				case *kafka.Message:
					if event.TopicPartition.Error != nil {
						producerMessageDeliveryFailures.WithLabelValues(*event.TopicPartition.Topic).Inc()
						logger.Error("kafka delivery failure",
							slog.String("err", event.TopicPartition.Error.Error()),
							slog.Group("kafkaMessage",
								slog.String("key", string(event.Key)),
								slog.String("topic", *event.TopicPartition.Topic),
								slog.Int("partition", int(event.TopicPartition.Partition)),
								slog.Any("headers", event.Headers)))
						if conf.OnError != nil {
							conf.OnError(event.TopicPartition.Error)
						}
					} else {
						producerMessagesDelivered.WithLabelValues(*event.TopicPartition.Topic).Inc()
						logger.Debug("kafka message successfully delivered",
							slog.Group("kafkaMessage",
								slog.String("key", string(event.Key)),
								slog.String("topic", *event.TopicPartition.Topic),
								slog.Int("partition", int(event.TopicPartition.Partition)),
								slog.Int64("offset", int64(event.TopicPartition.Offset)),
								slog.Any("headers", event.Headers)))
					}
				case kafka.Error:
					producerKafkaErrors.WithLabelValues(event.Code().String()).Inc()
					logger.Error("kafka producer error",
						slog.String("err", event.Error()),
						slog.Int("code", int(event.Code())),
						slog.Bool("fatal", event.IsFatal()),
						slog.Bool("retryable", event.IsRetriable()),
						slog.Bool("timeout", event.IsTimeout()),
						slog.Bool("txnRequiresAbort", event.TxnRequiresAbort()))
					if conf.OnError != nil {
						conf.OnError(event)
					}
				}
			case <-eventStopChan:
				return
			}
		}
	}(conf.Logger)

	return &Producer{
		base:           producer,
		eventStopChan:  eventStopChan,
		loggerStopChan: loggerStopChan,
	}, nil
}

// M returns a MessageBuilder that provides a fluent API for building and sending
// a message.
func (p *Producer) M() *MessageBuilder {
	return &MessageBuilder{producer: p}
}

// Produce produces a message to Kafka asynchronously and returns immediately if
// the message was enqueued successfully, otherwise returns an error. The delivery
// report is delivered on the provided channel. If the channel is nil than Produce
// operates as fire-and-forget.
func (p *Producer) Produce(m *kafka.Message, deliveryChan chan kafka.Event) error {
	m.TopicPartition.Partition = kafka.PartitionAny
	err := p.base.Produce(m, deliveryChan)
	if err != nil {
		producerMessagesEnqueueFailures.WithLabelValues(*m.TopicPartition.Topic).Inc()
		return RetryableError(fmt.Errorf("kafka: enqueue message: %w", err))
	}
	producerMessagesEnqueued.WithLabelValues(*m.TopicPartition.Topic).Inc()
	return nil
}

// ProduceAndWait produces a message to Kafka and waits for the delivery report.
//
// This method is blocking and will wait until the delivery report is received
// from Kafka. This allows for producing messages synchronously.
func (p *Producer) ProduceAndWait(m *kafka.Message) error {
	deliveryChan := make(chan kafka.Event)
	defer close(deliveryChan)

	m.TopicPartition.Partition = kafka.PartitionAny
	err := p.base.Produce(m, deliveryChan)
	if err != nil {
		return err
	}

	e := <-deliveryChan
	switch ev := e.(type) {
	case *kafka.Message:
		if ev.TopicPartition.Error != nil {
			producerMessageDeliveryFailures.WithLabelValues(*ev.TopicPartition.Topic).Inc()
			return fmt.Errorf("kafka delivery failure: %w", ev.TopicPartition.Error)
		}
		producerMessagesDelivered.WithLabelValues(*ev.TopicPartition.Topic).Inc()
	case kafka.Error:
		producerKafkaErrors.WithLabelValues(ev.Code().String()).Inc()
		return fmt.Errorf("kafka error: %w", ev)
	default:
		return fmt.Errorf("unexpected kafka event: %T", e)
	}

	return nil
}

// Transactional sends a batch of messages as a single transaction. If any messages
// fail to be produced the transaction will be aborted and an error will be returned.
func (p *Producer) Transactional(ctx context.Context, msgs []*kafka.Message) error {
	if err := p.base.InitTransactions(ctx); err != nil {
		return fmt.Errorf("kafka: failed to initialize transactions: %w", err)
	}

	err := p.base.BeginTransaction()
	deliveryChan := make(chan kafka.Event, len(msgs))

	for i := 0; i < len(msgs); i++ {
		err = p.base.Produce(msgs[i], deliveryChan)
		if err != nil {
			producerMessagesEnqueueFailures.WithLabelValues(*msgs[i].TopicPartition.Topic).Inc()
			abortErr := p.base.AbortTransaction(ctx)
			if abortErr != nil {
				return fmt.Errorf("kafka: failed to abort transaction: %w: failed to produce message: %w", abortErr, err)
			}
			return err
		}
		producerMessagesEnqueued.WithLabelValues(*msgs[i].TopicPartition.Topic).Inc()
	}

	for i := 0; i < len(msgs); i++ {
		event := <-deliveryChan
		switch ev := event.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				producerMessageDeliveryFailures.WithLabelValues(*ev.TopicPartition.Topic).Inc()
				abortErr := p.base.AbortTransaction(ctx)
				if abortErr != nil {
					return fmt.Errorf("kafka: failed to abort transaction: %w: message delivery failed: %w", abortErr, ev.TopicPartition.Error)
				}
				return fmt.Errorf("kafka: transaction aborted: delivery failure: %w", ev.TopicPartition.Error)
			}
			producerMessagesDelivered.WithLabelValues(*ev.TopicPartition.Topic).Inc()
		}
	}
	close(deliveryChan)

	err = p.base.CommitTransaction(ctx)
	if err != nil {
		return fmt.Errorf("kafka: failed to commit transaction: %w", err)
	}

	return nil
}

// Len returns the number of messages and requests waiting to be transmitted to
// the broker as well as delivery reports queued for the application.
func (p *Producer) Len() int {
	return p.base.Len()
}

// Flush and wait for outstanding messages and requests to complete delivery. Runs
// until value reaches zero or timeout is exceeded. Returns the number of outstanding
// events still un-flushed.
func (p *Producer) Flush(timeout time.Duration) int {
	return p.base.Flush(int(timeout.Milliseconds()))
}

// Close stops the producer and releases any resources. A Producer is not usable
// after this method is called.
func (p *Producer) Close() {
	p.eventStopChan <- struct{}{}  // Stop reading from event loop
	p.loggerStopChan <- struct{}{} // Stop reading logs from librdkafka
	p.base.Close()
}

// IsClosed returns true if the producer has been closed, otherwise false.
func (p *Producer) IsClosed() bool {
	return p.base.IsClosed()
}

func producerConfigMap(conf Config) *kafka.ConfigMap {

	// Configure base properties/parameters
	configMap := &kafka.ConfigMap{
		"bootstrap.servers":                  strings.Join(conf.BootstrapServers, ","),
		"security.protocol":                  conf.SecurityProtocol.String(),
		"message.max.bytes":                  conf.MessageMaxBytes,
		"enable.idempotence":                 conf.Idempotence,
		"request.required.acks":              conf.RequiredAcks.value(),
		"topic.metadata.refresh.interval.ms": 300000,
		"connections.max.idle.ms":            600000,
	}

	if conf.TransactionID != "" {
		_ = configMap.SetKey("transactional.id", conf.TransactionID)
	}

	// If SSL is enabled any additional SSL configuration provided needs added
	// to the configmap
	if conf.SecurityProtocol == Ssl || conf.SecurityProtocol == SaslSsl {
		if conf.CertificateAuthorityLocation != "" {
			_ = configMap.SetKey("ssl.ca.location", conf.CertificateAuthorityLocation)
		}
		if conf.CertificateLocation != "" {
			_ = configMap.SetKey("ssl.certificate.location", conf.CertificateLocation)
		}
		if conf.CertificateKeyLocation != "" {
			_ = configMap.SetKey("ssl.key.location", conf.CertificateKeyLocation)
		}
		if conf.CertificateKeyPassword != "" {
			_ = configMap.SetKey("ssl.key.password", conf.CertificateKeyPassword)
		}
		if conf.SkipTlsVerification {
			_ = configMap.SetKey("enable.ssl.certificate.verification", false)
		}
	}

	// If using SASL authentication add additional SASL configuration to the
	// configmap
	if conf.SecurityProtocol == SaslPlaintext || conf.SecurityProtocol == SaslSsl {
		_ = configMap.SetKey("sasl.mechanism", conf.SASLMechanism.String())
		_ = configMap.SetKey("sasl.username", conf.SASLUsername)
		_ = configMap.SetKey("sasl.password", conf.SASLPassword)
	}

	return configMap
}
