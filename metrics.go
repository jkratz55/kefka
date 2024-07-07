package kefka

import (
	"log/slog"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/multierr"
)

const (
	statusSuccess = "success"
	statusError   = "error"
)

var (
	kefkaVersion                 *prometheus.GaugeVec
	confluentKafkaLibraryVersion *prometheus.GaugeVec

	consumerMessagesProcessed  *prometheus.CounterVec
	consumerMessagesFailed     *prometheus.CounterVec
	consumerHandlerDuration    *prometheus.HistogramVec
	consumerKafkaErrors        *prometheus.CounterVec
	consumerOffsetsCommited    *prometheus.CounterVec
	consumerRebalances         *prometheus.CounterVec
	consumerCommitOffsetErrors *prometheus.CounterVec
	consumerStoreOffsetErrors  *prometheus.CounterVec

	producerMessagesEnqueued        *prometheus.CounterVec
	producerMessagesEnqueueFailures *prometheus.CounterVec
	producerMessagesDelivered       *prometheus.CounterVec
	producerMessageDeliveryFailures *prometheus.CounterVec
	producerKafkaErrors             *prometheus.CounterVec

	readerMessagesProcessed *prometheus.CounterVec
	readerMessagesFailed    *prometheus.CounterVec
	readerHandlerDuration   *prometheus.HistogramVec
	readerKafkaErrors       *prometheus.CounterVec

	dltMessagesProduced             *prometheus.CounterVec
	dltMessageFailures              *prometheus.CounterVec
	retryHandlerErrors              *prometheus.CounterVec
	retryHandlerMaxAttemptsExceeded *prometheus.CounterVec
	retryHandlerNonRetryableErrors  *prometheus.CounterVec
)

func init() {
	kefkaVersion = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "kefka",
		Name:      "version",
		Help:      "Version of github.com/jkratz55/kefka",
	}, []string{"version"})
	confluentKafkaLibraryVersion = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "confluent_kafka",
		Name:      "client_version",
		Help:      "Version of the Confluent Kafka Go client",
	}, []string{"version"})
	consumerMessagesProcessed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "kefka",
		Subsystem: "consumer",
		Name:      "messages_processed",
		Help:      "Number of messages processed by the consumer regardless of success or failure",
	}, []string{"topic"})
	consumerMessagesFailed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "kefka",
		Subsystem: "consumer",
		Name:      "messages_failed",
		Help:      "Number of messages that were not successfully processed",
	}, []string{"topic"})
	consumerHandlerDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "kefka",
		Subsystem: "consumer",
		Name:      "handler_duration_seconds",
		Help:      "Duration of time for a handler to process a message",
		Buckets:   prometheus.ExponentialBuckets(0.001, 2, 10),
	}, []string{"topic", "status"})
	consumerKafkaErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "kefka",
		Subsystem: "consumer",
		Name:      "kafka_errors",
		Help:      "Number of errors returned by the Kafka client",
	}, []string{"code"})
	consumerOffsetsCommited = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "kefka",
		Subsystem: "consumer",
		Name:      "offsets_commited",
		Help:      "Number of times offsets commited by the consumer",
	}, []string{"topic", "partition"})
	consumerRebalances = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "kefka",
		Subsystem: "consumer",
		Name:      "rebalances",
		Help:      "Number of times the consumer has rebalanced",
	}, []string{"topic"})
	consumerStoreOffsetErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "kefka",
		Subsystem: "consumer",
		Name:      "store_offset_errors",
		Help:      "Number of errors that occurred while storing offsets",
	}, []string{"topic", "partition"})
	consumerCommitOffsetErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "kefka",
		Subsystem: "consumer",
		Name:      "commit_offset_errors",
		Help:      "Number of errors that occurred while committing offsets",
	}, []string{"topic", "partition"})
	producerMessagesEnqueued = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "kefka",
		Subsystem: "producer",
		Name:      "messages_enqueued",
		Help:      "Number of messages enqueued by the producer",
	}, []string{"topic"})
	producerMessagesEnqueueFailures = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "kefka",
		Subsystem: "producer",
		Name:      "messages_enqueue_failures",
		Help:      "Number of messages that failed to be enqueued by the producer",
	}, []string{"topic"})
	producerMessagesDelivered = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "kefka",
		Subsystem: "producer",
		Name:      "messages_delivered",
		Help:      "Number of messages delivered by the producer",
	}, []string{"topic"})
	producerMessageDeliveryFailures = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "kefka",
		Subsystem: "producer",
		Name:      "message_delivery_failures",
		Help:      "Number of messages that failed to be delivered by the producer",
	}, []string{"topic"})
	producerKafkaErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "kefka",
		Subsystem: "producer",
		Name:      "kafka_errors",
		Help:      "Number of errors returned by the Kafka client",
	}, []string{"code"})
	readerMessagesProcessed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "kefka",
		Subsystem: "reader",
		Name:      "messages_processed",
		Help:      "Number of messages processed by the reader regardless of success or failure",
	}, []string{"topic", "partition"})
	readerMessagesFailed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "kefka",
		Subsystem: "reader",
		Name:      "messages_failed",
		Help:      "Number of messages that were not successfully processed",
	}, []string{"topic", "partition"})
	readerHandlerDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "kefka",
		Subsystem: "reader",
		Name:      "handler_duration_seconds",
		Help:      "Duration of time for a handler to process a message",
		Buckets:   prometheus.ExponentialBuckets(0.001, 2, 10),
	}, []string{"topic", "partition", "status"})
	readerKafkaErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "kefka",
		Subsystem: "reader",
		Name:      "kafka_errors",
		Help:      "Number of errors returned by the Kafka client",
	}, []string{"code"})
	dltMessagesProduced = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "kefka",
		Subsystem: "dead_letter",
		Name:      "messages_produced",
		Help:      "Number of messages produced to the dead letter topic",
	}, []string{"topic"})
	dltMessageFailures = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "kefka",
		Subsystem: "dead_letter",
		Name:      "message_failures",
		Help:      "Number of messages that failed to be produced to the dead letter topic",
	}, []string{"topic"})
	retryHandlerErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "kefka",
		Subsystem: "retry_handler",
		Name:      "errors",
		Help:      "Number of errors returned by handler RetryHandler wraps",
	}, []string{"topic"})
	retryHandlerMaxAttemptsExceeded = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "kefka",
		Subsystem: "retry_handler",
		Name:      "max_attempts_exceeded",
		Help:      "Number of times the maximum attempts were exceeded by the RetryHandler",
	}, []string{"topic"})
	retryHandlerNonRetryableErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "kefka",
		Subsystem: "retry_handler",
		Name:      "non_retryable_errors",
		Help:      "Number of times a non-retryable errors the RetryHandler encountered",
	}, []string{"topic"})

	err := multierr.Combine(
		prometheus.Register(kefkaVersion),
		prometheus.Register(confluentKafkaLibraryVersion),
		prometheus.Register(consumerMessagesProcessed),
		prometheus.Register(consumerMessagesFailed),
		prometheus.Register(consumerHandlerDuration),
		prometheus.Register(consumerKafkaErrors),
		prometheus.Register(consumerOffsetsCommited),
		prometheus.Register(consumerRebalances),
		prometheus.Register(consumerCommitOffsetErrors),
		prometheus.Register(consumerStoreOffsetErrors),
		prometheus.Register(producerMessagesEnqueued),
		prometheus.Register(producerMessagesEnqueueFailures),
		prometheus.Register(producerMessagesDelivered),
		prometheus.Register(producerMessageDeliveryFailures),
		prometheus.Register(producerKafkaErrors),
		prometheus.Register(readerMessagesProcessed),
		prometheus.Register(readerMessagesFailed),
		prometheus.Register(readerHandlerDuration),
		prometheus.Register(readerKafkaErrors),
		prometheus.Register(dltMessagesProduced),
		prometheus.Register(dltMessageFailures),
		prometheus.Register(retryHandlerErrors),
		prometheus.Register(retryHandlerMaxAttemptsExceeded),
		prometheus.Register(retryHandlerNonRetryableErrors),
	)
	if err != nil {
		DefaultLogger().Error("failed to register prometheus metrics: some or all metrics may not be available",
			slog.String("err", err.Error()))
	}

	kefkaVersion.WithLabelValues(version).Set(1)

	_, confluentKafkaLibVersion := kafka.LibraryVersion()
	confluentKafkaLibraryVersion.WithLabelValues(confluentKafkaLibVersion).Set(1)
}
