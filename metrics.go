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
	kefkaVersion                    *prometheus.GaugeVec
	confluentKafkaLibraryVersion    *prometheus.GaugeVec
	consumerMessagesProcessed       *prometheus.CounterVec
	consumerMessagesFailed          *prometheus.CounterVec
	consumerHandlerDuration         *prometheus.HistogramVec
	consumerKafkaErrors             *prometheus.CounterVec
	consumerOffsetsCommited         *prometheus.CounterVec
	consumerRebalances              *prometheus.CounterVec
	consumerCommitOffsetErrors      *prometheus.CounterVec
	consumerStoreOffsetErrors       *prometheus.CounterVec
	producerMessagesEnqueued        *prometheus.CounterVec
	producerMessagesDelivered       *prometheus.CounterVec
	producerMessageDeliveryFailures *prometheus.CounterVec
	producerKafkaErrors             *prometheus.CounterVec
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
		Help:      "Number of messages processed by the consumer",
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
	}, []string{"topic"})
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
	}, []string{"topic"})
	consumerCommitOffsetErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "kefka",
		Subsystem: "consumer",
		Name:      "commit_offset_errors",
		Help:      "Number of errors that occurred while committing offsets",
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
	)
	if err != nil {
		DefaultLogger().Error("failed to register prometheus metrics: some or all metrics may not be available",
			slog.String("err", err.Error()))
	}

	kefkaVersion.WithLabelValues(version).Set(1)

	_, confluentKafkaLibVersion := kafka.LibraryVersion()
	confluentKafkaLibraryVersion.WithLabelValues(confluentKafkaLibVersion).Set(1)
}
