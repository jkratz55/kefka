package kefka

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// ReaderOpts is a type representing additional options/configuration that can
// be provided to the Reader.
type ReaderOpts struct {

	// OnEndOfPartition is a callback that is invoked when the Reader reaches the
	// end of a partition. This will be invoked for each partition that reaches
	// the end of the partition as well as each time it reaches the end of the
	// partition. Essentially, this callback can be invoked multiple times for the
	// same topic/partition.
	OnEndOfPartition func(topic string, partition int, offset int64)

	// StopOnEndOfPartition is a flag that when enabled will stop the Reader when
	// all topics/partitions the Reader is reading from have reached the end.
	StopOnEndOfPartition bool

	// Maximum messages the Reader will read. Once the limit is reached the Reader
	// will gracefully stop and close.
	//
	// The default zero is unlimited.
	Limit int
}

// Reader provides a high-level API for reading messages from Kafka. The Reader
// differs from Consumer in that it is designed to simply read/view the contents
// of a topic/partition and does not provide the ability to commit offsets as it
// does not leverage consumer groups.
type Reader struct {
	base               *kafka.Consumer
	handler            Handler
	topicPartitions    []kafka.TopicPartition
	running            bool
	mu                 sync.Mutex
	pollTimeout        int
	conf               Config
	stopChan           chan struct{}
	logger             *slog.Logger
	opts               ReaderOpts
	partitionEOFStatus map[string]bool
	counter            int64
}

// NewReader initializes a new Reader instance with the provided configuration.
func NewReader(conf Config, handler Handler, tps []kafka.TopicPartition, opts ReaderOpts) (*Reader, error) {
	if handler == nil {
		return nil, errors.New("invalid config: cannot initialize Reader with nil Handler")
	}
	if len(tps) == 0 {
		return nil, errors.New("invalid config: cannot initialize Reader with empty TopicPartitions")
	}

	partitionEOFStatus := make(map[string]bool)
	for _, tp := range tps {
		if tp.Topic == nil || strings.TrimSpace(*tp.Topic) == "" {
			return nil, errors.New("invalid config: cannot initialize Reader with empty Topic")
		}
		key := fmt.Sprintf("%s|%d", *tp.Topic, tp.Partition)
		partitionEOFStatus[key] = false
	}

	conf.init()
	configMap := readerConfigMap(conf)

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

	// If a PartitionEOF callback is set or StopOnEndOfPartition is enabled then
	// partition EOF events needs to be enabled
	if opts.OnEndOfPartition != nil || opts.StopOnEndOfPartition {
		_ = configMap.SetKey("enable.partition.eof", true)
	}

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

	reader := &Reader{
		base:               base,
		handler:            handler,
		topicPartitions:    tps,
		running:            false,
		mu:                 sync.Mutex{},
		pollTimeout:        int(conf.PollTimeout.Milliseconds()),
		conf:               conf,
		stopChan:           stopChan,
		logger:             conf.Logger,
		partitionEOFStatus: partitionEOFStatus,
		opts:               opts,
		counter:            0,
	}
	return reader, nil
}

// Run starts polling events from Kafka and reading/processing messages. Run
// is blocking and will continue to run until the Reader is closed, reached the
// end of all partitions and configured to stop, configured with a message limit
// and reaches limit, or encounters a fatal error.
//
// In almost all use cases Run should be invoked in a new goroutine. If the Reader
// is gracefully closed Run should return a nil error value.
func (r *Reader) Run() error {

	// Try to acquire the lock to ensure Run cannot be invoked more than once on
	// the same Reader instance. If the lock cannot be acquired, return an error
	// as that means the Reader is already running.
	if ok := r.mu.TryLock(); !ok {
		return fmt.Errorf("unsupported operation: cannot invoke Run() on an already running Reader")
	}
	defer r.mu.Unlock()

	// Once a Reader is closed it cannot be reused.
	if r.base.IsClosed() {
		return errors.New("unsupported operation: cannot invoke Run() on a closed Reader")
	}

	if err := r.base.Assign(r.topicPartitions); err != nil {
		return fmt.Errorf("kafka: failed to assign topic partitions: %w", err)
	}
	r.logger.Debug("Assigned topic partitions",
		slog.Any("topicPartitions", r.topicPartitions))

	r.running = true

	// Event loop that continuously polls the brokers for new events and handles
	// them accordingly. The loop will continue to run until the Reader is closed
	// or encounters a fatal error.
	for r.running {
		rawEvent := r.base.Poll(r.pollTimeout)

		switch event := rawEvent.(type) {
		case kafka.Error:

			// Handle the error event polled from Kafka. The handleError method
			// will only bubble up an error if Confluent Kafka Go/librdkafka client
			// returns a fatal error indicating the Reader cannot continue. The
			// Reader will close and the error is bubbled up to the caller to
			// indicate the Reader has unexpectedly stopped due to a fatal error
			// and cannot continue.
			if err := r.handleError(event); err != nil {
				r.running = false
				_ = r.base.Close()
				r.stopChan <- struct{}{}
				return err
			}

		case *kafka.Message:
			r.handleMessage(event)

		case kafka.PartitionEOF:
			r.handlePartitionEOF(event)
		}
	}

	// The Reader was gracefully closed, close the underlying Confluent Kafka
	// client to release resources. Any errors that occur during the commit or close
	// will be bubbled up to the caller.
	var closeErr error
	if err := r.base.Close(); err != nil {
		closeErr = err
	}

	// Signal to the goroutine processing logs to stop
	r.stopChan <- struct{}{}
	return closeErr
}

func (r *Reader) handleError(err kafka.Error) error {
	readerKafkaErrors.WithLabelValues(err.Code().String()).Inc()
	// If an error callback is provided invoke it with the error
	if r.conf.OnError != nil {
		r.conf.OnError(err)
	}
	r.logger.Error("Kafka returned an error while polling for events",
		slog.String("err", err.Error()),
		slog.Int("code", int(err.Code())),
		slog.Bool("fatal", err.IsFatal()),
		slog.Bool("retryable", err.IsRetriable()),
		slog.Bool("timeout", err.IsTimeout()))

	// If the error is fatal there is no point in continuing to run the Reader.
	// An error is bubbled up to the caller to indicate to the Reader has stopped
	// due to a fatal error.
	if err.IsFatal() {
		return fmt.Errorf("kafka client fatal error: %w", err)
	}

	return nil
}

func (r *Reader) handleMessage(msg *kafka.Message) {
	start := time.Now()
	err := r.handler.Handle(msg)
	duration := time.Since(start).Seconds()
	status := statusSuccess

	partitionStr := strconv.Itoa(int(msg.TopicPartition.Partition))
	if err != nil {
		readerMessagesFailed.WithLabelValues(*msg.TopicPartition.Topic, partitionStr).Inc()
		r.logger.Error("Failed to process message: handler returned a error",
			consumerSlogAttrs(msg, err)...)
		status = statusError
	}
	readerMessagesProcessed.WithLabelValues(*msg.TopicPartition.Topic, partitionStr).Inc()
	readerHandlerDuration.WithLabelValues(*msg.TopicPartition.Topic, partitionStr, status).Observe(duration)

	// If limit is not set no limit is enforced on messages read
	if r.opts.Limit > 0 {
		r.counter++
		if r.counter >= int64(r.opts.Limit) {
			r.running = false
		}
	}
}

func (r *Reader) handlePartitionEOF(event kafka.PartitionEOF) {
	// If a callback is configured for PartitionEOF event invoke the callback
	if r.opts.OnEndOfPartition != nil {
		r.opts.OnEndOfPartition(*event.Topic, int(event.Partition), int64(event.Offset))
	}
	// If StopOnEndOfPartition is enabled, stop the Reader when the end of each partition is reached.
	if r.opts.StopOnEndOfPartition {
		// We only bother tracking the EOF partition status if configured to stop
		// on EOF.
		key := fmt.Sprintf("%s|%d", *event.Topic, event.Partition)
		r.partitionEOFStatus[key] = true

		// If all partitions have reached EOF, stop the Reader.
		if r.reachedEOFAllPartitions() {
			r.running = false
		}
	}
}

func (r *Reader) reachedEOFAllPartitions() bool {
	for _, val := range r.partitionEOFStatus {
		if !val {
			return false
		}
	}
	return true
}

// Assignment returns the current partition assignments for the Reader.
func (r *Reader) Assignment() (kafka.TopicPartitions, error) {
	return r.base.Assignment()
}

// Position returns the current consume position for the currently assigned partitions.
// The consume position is the next message to read from the partition. i. e., the offset
// of the last message seen by the application + 1.
func (r *Reader) Position() ([]kafka.TopicPartition, error) {
	return r.base.Position(r.topicPartitions)
}

// Lag returns the current lag for each partition the Reader is assigned/configured
// to read from. The lag is represented as a map topic|partition -> lag.
func (r *Reader) Lag() (map[string]int64, error) {
	lags := make(map[string]int64)
	positions, err := r.Position()
	if err != nil {
		return lags, fmt.Errorf("kafka: failed to get current positions: %w", err)
	}

	for _, pos := range positions {
		low, high, err := r.base.QueryWatermarkOffsets(*pos.Topic, pos.Partition, 3000)
		if err != nil {
			return lags, fmt.Errorf("kafka: failed to query watermark offsets: %w", err)
		}

		key := fmt.Sprintf("%s|%d", *pos.Topic, pos.Partition)
		if pos.Offset < 0 {
			lags[key] = high - low
		} else {
			lags[key] = high - int64(pos.Offset)
		}
	}

	return lags, nil
}

// Close gracefully closes the Reader and releases any resources. After Close
// is called the Reader cannot be reused.
func (r *Reader) Close() {
	r.running = false
}

// IsRunning returns true if the Reader is currently running, otherwise false.
func (r *Reader) IsRunning() bool {
	return r.running
}

// IsClosed returns true if the Reader is closed, otherwise false.
func (r *Reader) IsClosed() bool {
	return r.base.IsClosed()
}

func readerConfigMap(conf Config) *kafka.ConfigMap {
	// Configure base properties/parameters
	configMap := &kafka.ConfigMap{
		"bootstrap.servers":                  strings.Join(conf.BootstrapServers, ","),
		"group.id":                           "KEFKA", // this is required but won't be used
		"auto.offset.reset":                  "earliest",
		"enable.auto.offset.store":           false,
		"enable.auto.commit":                 false,
		"auto.commit.interval.ms":            0,
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
