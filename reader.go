package kefka

import (
	"log/slog"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type OnEndOfPartition func(topic string, partition int, offset int64)

type TopicPartition struct {
	Topic     string
	Partition int
	Offset    int64
}

type ReaderConfig struct {
	Handler          Handler
	Brokers          []string
	TopicPartitions  []TopicPartition
	OnEndOfPartition OnEndOfPartition
}

type Reader struct {
	base           *kafka.Consumer
	handler        Handler
	logger         *slog.Logger
	partitionEOFCb OnEndOfPartition
	termCh         chan struct{}
}

func NewReader(config ReaderConfig) (*Reader, error) {
	return nil, nil
}

func (r *Reader) Run() error {
	for {
		select {
		case <-r.termCh:
			return nil
		default:
			e := r.base.Poll(-1)
			switch event := e.(type) {
			case *kafka.Message:
				err := r.handler.ProcessMessage(event)
				if err != nil {
					r.logger.Error("message handler returned a non-nil error value",
						errAttr(err),
						slog.String("topic", *event.TopicPartition.Topic),
						slog.Int("partition", int(event.TopicPartition.Partition)),
						slog.Int64("offset", int64(event.TopicPartition.Offset)),
						slog.String("key", string(event.Key)))
				}
			case kafka.Error:
				// todo: handle error
			case kafka.PartitionEOF:
				// It is safe to blindly call the callback here because the feature
				// on librdkafka that makes this possible is only enabled when the
				// Consumer was created with callback set to a non-nil value.
				r.partitionEOFCb(*event.Topic, int(event.Partition), int64(event.Offset))
			default:
				r.logger.Debug("ignoring event from kafka",
					slog.Any("event", e))
			}
		}
	}
}

func (r *Reader) Close() error {
	r.termCh <- struct{}{}
	return r.base.Close()
}
