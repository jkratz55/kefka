package kefka

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	kefkaVersion                    *prometheus.GaugeVec
	consumerMessagesProcessed       *prometheus.CounterVec
	consumerMessagesFailed          *prometheus.CounterVec
	consumerHandlerDuration         *prometheus.HistogramVec
	consumerKafkaErrors             *prometheus.CounterVec
	consumerHandlerErrors           *prometheus.CounterVec
	consumerOffsetsCommited         *prometheus.CounterVec
	consumerRebalances              *prometheus.CounterVec
	producerMessagesEnqueued        *prometheus.CounterVec
	producerMessagesDelivered       *prometheus.CounterVec
	producerMessageDeliveryFailures *prometheus.CounterVec
	producerKafkaErrors             *prometheus.CounterVec
)

func init() {
	// todo: handle initialization of prometheus collectors
}
