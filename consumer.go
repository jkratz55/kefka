package kefka

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// baseConsumer is an interface type that defines the behavior and functionality of
// Kafka client Consumer. This interface exists to allow for mocking and testing.
type baseConsumer interface {
	Assignment() ([]kafka.TopicPartition, error)
	Subscription() (topics []string, err error)
	Committed(partitions []kafka.TopicPartition, timeoutMs int) (offsets []kafka.TopicPartition, err error)
	QueryWatermarkOffsets(topic string, partition int32, timeoutMs int) (low, high int64, err error)
	Subscribe(topics string, rebalanceCb kafka.RebalanceCb) error
	Poll(timeoutMs int) (event kafka.Event)
	CommitMessage(msg *kafka.Message) ([]kafka.TopicPartition, error)
	StoreMessage(msg *kafka.Message) (storedOffsets []kafka.TopicPartition, err error)
	Commit() ([]kafka.TopicPartition, error)
	Position(partitions []kafka.TopicPartition) (offsets []kafka.TopicPartition, err error)
	IsClosed() bool
	Close() error
}

// Ensures that the kafka.Consumer implements all methods defined by the baseConsumer interface.
var _ baseConsumer = &kafka.Consumer{}

type Consumer struct {
	base               baseConsumer
	handler            Handler
	topic              string
	running            bool
	mu                 sync.Mutex
	logger             *slog.Logger
	commitEveryMessage bool
	pollTimeout        int
	conf               Config
	stopChan           chan struct{}
}

func NewConsumer(conf Config, handler Handler, topic string) (*Consumer, error) {
	if handler == nil {
		return nil, errors.New("invalid config: cannot initialize Consumer with nil Handler")
	}
	if strings.TrimSpace(topic) == "" {
		return nil, errors.New("invalid config: cannot initialize Consumer with empty topic")
	}

	conf.init()

	configMap := consumerConfigMap(conf)

	stopChan := make(chan struct{})
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
			case <-stopChan:
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
	conf.Logger.Info("Initializing Kafka Consumer",
		slog.Any("config", obfuscateConfig(configMap)))

	base, err := kafka.NewConsumer(configMap)
	if err != nil {
		return nil, fmt.Errorf("kafka: failed to initialize Confluent Kafka Consumer: %w", err)
	}

	consumer := &Consumer{
		base:               base,
		handler:            handler,
		topic:              topic,
		running:            false,
		mu:                 sync.Mutex{},
		logger:             conf.Logger,
		commitEveryMessage: conf.CommitInterval < 0,
		pollTimeout:        int(conf.PollTimeout.Milliseconds()),
		conf:               conf,
		stopChan:           stopChan,
	}
	return consumer, nil
}

func (c *Consumer) Run() error {

	// Try to acquire the lock to ensure Run cannot be invoked more than once on
	// the same Consumer instance. If the lock cannot be acquired, return an error
	// as that means the Consumer is already running.
	if ok := c.mu.TryLock(); !ok {
		return fmt.Errorf("unsupported operation: cannot invoke Run() on an already running Consumer")
	}
	defer c.mu.Unlock()

	// Once a Consumer is closed it cannot be reused.
	if c.base.IsClosed() {
		return errors.New("unsupported operation: cannot invoke Run() on a closed Consumer")
	}

	err := c.base.Subscribe(c.topic, c.rebalanceCb)
	if err != nil {
		return fmt.Errorf("failed to subscribe to topic %s: %w", c.topic, err)
	}
	c.logger.Debug(fmt.Sprintf("Subscribed to topic %s", c.topic))

	c.running = true

	// Event loop that continuously polls the brokers for new events and handles
	// them accordingly. The loop will continue to run until the Consumer is closed
	// or encounters a fatal error.
	for c.running {
		rawEvent := c.base.Poll(c.pollTimeout)

		switch event := rawEvent.(type) {

		case kafka.Error:
			// If an error callback is provided invoke it with the error
			if c.conf.OnError != nil {
				c.conf.OnError(event)
			}
			c.logger.Error("Kafka returned an error while polling for events",
				slog.String("err", event.Error()),
				slog.Int("code", int(event.Code())),
				slog.Bool("fatal", event.IsFatal()),
				slog.Bool("retryable", event.IsRetriable()),
				slog.Bool("timeout", event.IsTimeout()))

		case *kafka.Message:
			err := c.handler.Handle(event)
			if err != nil {
				c.logger.Error("Failed to process message: handler returned a error",
					slog.String("err", err.Error()),
					slog.String("topic", *event.TopicPartition.Topic),
					slog.Int("partition", int(event.TopicPartition.Partition)),
					slog.Int64("offset", int64(event.TopicPartition.Offset)),
					slog.String("key", string(event.Key)))
			}

			// If the Consumer is configured to commit offsets after every message
			// commit the offset for the message. Otherwise, the offset is stored
			// and the offsets are committed based on the configured commit interval.
			if c.commitEveryMessage {
				_, err := c.base.CommitMessage(event)
				if err != nil {
					// If an error callback is provided invoke it with the error
					if c.conf.OnError != nil {
						c.conf.OnError(err)
					}
					c.logger.Error("Failed to commit offset for message",
						slog.String("err", err.Error()),
						slog.String("topic", *event.TopicPartition.Topic),
						slog.Int("partition", int(event.TopicPartition.Partition)),
						slog.Int64("offset", int64(event.TopicPartition.Offset)),
						slog.String("key", string(event.Key)))
				}
			} else {
				_, err := c.base.StoreMessage(event)
				if err != nil {
					// If an error callback is provided invoke it with the error
					if c.conf.OnError != nil {
						c.conf.OnError(err)
					}
					c.logger.Error("Failed to acknowledge message and store offsets",
						slog.String("err", err.Error()),
						slog.String("topic", *event.TopicPartition.Topic),
						slog.Int("partition", int(event.TopicPartition.Partition)),
						slog.Int64("offset", int64(event.TopicPartition.Offset)),
						slog.String("key", string(event.Key)))
				}
			}

		case kafka.OffsetsCommitted:
			for _, tp := range event.Offsets {
				if tp.Error != nil {
					if c.conf.OnError != nil {
						c.conf.OnError(err)
					}
					c.logger.Error("Failed to commit offset to Kafka brokers",
						slog.String("err", tp.Error.Error()),
						slog.String("topic", *tp.Topic),
						slog.Int("partition", int(tp.Partition)),
						slog.Int64("offset", int64(tp.Offset)))
				} else {
					c.logger.Debug("Successfully committed offset to Kafka brokers",
						slog.String("topic", *tp.Topic),
						slog.Int("partition", int(tp.Partition)),
						slog.Int64("offset", int64(tp.Offset)))
				}
			}
		}
	}

	// The Consumer was gracefully closed using Close() method. Commit any offsets
	// that have not been committed yet, and close the underlying Confluent Kafka
	// client to release resources. Any errors that occur during the commit or close
	// will be bubbled up to the caller.
	var closeErr error
	if _, err := c.base.Commit(); err != nil {
		closeErr = err
	}
	if err := c.base.Close(); err != nil {
		closeErr = errors.Join(closeErr, err)
	}

	// Signal to the goroutine processing logs to stop
	c.stopChan <- struct{}{}
	return closeErr
}

func (c *Consumer) Assignment() (kafka.TopicPartitions, error) {
	return c.base.Assignment()
}

func (c *Consumer) Subscription() ([]string, error) {
	return c.base.Subscription()
}

// Position returns the current consume position for the currently assigned partitions.
// The consume position is the next message to read from the partition. i. e., the offset
// of the last message seen by the application + 1.
func (c *Consumer) Position() ([]kafka.TopicPartition, error) {

	// Get the current assigned partitions for the Consumer
	topicPartitions, err := c.base.Assignment()
	if err != nil {
		return nil, fmt.Errorf("failed to get assignments for Consumer: %w", err)
	}
	return c.base.Position(topicPartitions)
}

func (c *Consumer) Lag() (map[string]int64, error) {
	lags := make(map[string]int64)

	// Get the current assigned partitions for the Consumer
	topicPartitions, err := c.base.Assignment()
	if err != nil {
		return lags, fmt.Errorf("failed to get assignments for Consumer: %w", err)
	}

	// Get the current offset for each partition assigned to the consumer group
	topicPartitions, err = c.base.Committed(topicPartitions, 5000)
	if err != nil {
		return lags, fmt.Errorf("failed to get commited offsets: %w", err)
	}

	// Iterate over each partition and query for the watermarks. The lag is determined by
	// the difference between the high watermark and the current offset.
	for _, tp := range topicPartitions {
		low, high, err := c.base.QueryWatermarkOffsets(*tp.Topic, tp.Partition, 5000)
		if err != nil {
			return lags, fmt.Errorf("failed to get watermark offsets for partition %d of topic %s", tp.Partition, *tp.Topic)
		}

		offset := int64(tp.Offset)
		if tp.Offset == kafka.OffsetInvalid {
			offset = low
		}

		key := *tp.Topic + "|" + strconv.Itoa(int(tp.Partition))
		lags[key] = high - offset
	}

	return lags, nil
}

func (c *Consumer) Commit() error {
	_, err := c.base.Commit()
	return err
}

func (c *Consumer) Close() {
	c.running = false
}

func (c *Consumer) IsRunning() bool {
	return c.running
}

func (c *Consumer) IsClosed() bool {
	return c.base.IsClosed()
}

func (c *Consumer) rebalanceCb(_ *kafka.Consumer, event kafka.Event) error {
	switch e := event.(type) {
	case kafka.AssignedPartitions:
		c.logger.Info("Consumer group rebalanced: assigned partitions",
			slog.Any("assignments", e.Partitions))
	case kafka.RevokedPartitions:
		c.logger.Info("Consumer group rebalanced: revoking partitions",
			slog.Any("assignments", e.Partitions))
	}

	return nil
}

func consumerConfigMap(conf Config) *kafka.ConfigMap {
	// Configure base properties/parameters
	configMap := &kafka.ConfigMap{
		"bootstrap.servers":                  strings.Join(conf.BootstrapServers, ","),
		"group.id":                           conf.GroupID,
		"session.timeout.ms":                 int(conf.SessionTimeout.Milliseconds()),
		"heartbeat.interval.ms":              int(conf.HeartbeatInterval.Milliseconds()),
		"auto.offset.reset":                  conf.AutoOffsetReset.String(),
		"enable.auto.offset.store":           false,
		"auto.commit.interval.ms":            int(conf.CommitInterval.Milliseconds()),
		"security.protocol":                  conf.SecurityProtocol.String(),
		"message.max.bytes":                  conf.MessageMaxBytes,
		"fetch.max.bytes":                    conf.MaxFetchBytes,
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
