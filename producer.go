package kefka

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type Producer struct {
	producer       *kafka.Producer
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
			case logEvent := <-logChan:
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
						logger.Debug("kafka message successfully delivered",
							slog.Group("kafkaMessage",
								slog.String("key", string(event.Key)),
								slog.String("topic", *event.TopicPartition.Topic),
								slog.Int("partition", int(event.TopicPartition.Partition)),
								slog.Int64("offset", int64(event.TopicPartition.Offset)),
								slog.Any("headers", event.Headers)))
					}
				case kafka.Error:
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
		producer:       producer,
		eventStopChan:  eventStopChan,
		loggerStopChan: loggerStopChan,
	}, nil
}

func (p *Producer) M() *MessageBuilder {
	return &MessageBuilder{producer: p}
}

func (p *Producer) Produce(m *kafka.Message, deliveryChan chan kafka.Event) error {
	m.TopicPartition.Partition = kafka.PartitionAny
	return p.producer.Produce(m, deliveryChan)
}

func (p *Producer) ProduceAndWait(m *kafka.Message) error {
	deliveryChan := make(chan kafka.Event)
	defer close(deliveryChan)

	m.TopicPartition.Partition = kafka.PartitionAny
	err := p.producer.Produce(m, deliveryChan)
	if err != nil {
		return err
	}

	e := <-deliveryChan
	switch ev := e.(type) {
	case *kafka.Message:
		if ev.TopicPartition.Error != nil {
			return fmt.Errorf("kafka delivery failure: %w", ev.TopicPartition.Error)
		}
	case kafka.Error:
		return fmt.Errorf("kafka error: %w", ev)
	default:
		return fmt.Errorf("unexpected kafka event: %T", e)
	}

	return nil
}

func (p *Producer) Transactional(ctx context.Context, msgs []*kafka.Message) error {
	if err := p.producer.InitTransactions(ctx); err != nil {
		return fmt.Errorf("kafka: failed to initialize transactions: %w", err)
	}

	err := p.producer.BeginTransaction()
	deliveryChan := make(chan kafka.Event, len(msgs))

	for i := 0; i < len(msgs); i++ {
		err = p.producer.Produce(msgs[i], deliveryChan)
		if err != nil {
			abortErr := p.producer.AbortTransaction(ctx)
			if abortErr != nil {
				return fmt.Errorf("kafka: failed to abort transaction: %w: failed to produce message: %w", abortErr, err)
			}
			return err
		}
	}
	close(deliveryChan)

	for event := range deliveryChan {
		switch ev := event.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				abortErr := p.producer.AbortTransaction(ctx)
				if abortErr != nil {
					return fmt.Errorf("kafka: failed to abort transaction: %w: message delivery failed: %w", abortErr, ev.TopicPartition.Error)
				}
				return fmt.Errorf("kafka: transaction aborted: delivery failure: %w", ev.TopicPartition.Error)
			}
		}
	}

	err = p.producer.CommitTransaction(ctx)
	if err != nil {
		return fmt.Errorf("kafka: failed to commit transaction: %w", err)
	}

	return nil
}

// Len returns the number of messages and requests waiting to be transmitted to
// the broker as well as delivery reports queued for the application.
func (p *Producer) Len() int {
	return p.producer.Len()
}

func (p *Producer) Flush(timeout time.Duration) int {
	return p.producer.Flush(int(timeout.Milliseconds()))
}

func (p *Producer) Close() {
	p.producer.Close()
	p.eventStopChan <- struct{}{}  // Stop reading from event loop
	p.loggerStopChan <- struct{}{} // Stop reading logs from librdkafka
}

func (p *Producer) IsClosed() bool {
	return p.producer.IsClosed()
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
