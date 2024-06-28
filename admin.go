package kefka

import (
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// AdminClient is a wrapper around the confluent-kafka-go AdminClient that offers
// useful functionality when working with kefka Reader and Consumer types.
type AdminClient struct {
	admin     *kafka.AdminClient
	consumer  *kafka.Consumer
	logger    *slog.Logger
	closeChan chan struct{}
}

// NewAdminClient initializes a new AdminClient with the provided configuration.
func NewAdminClient(conf Config) (*AdminClient, error) {
	conf.init()
	configMap := adminConfigMap(conf)

	logChan := make(chan kafka.LogEvent, 100)
	closeChan := make(chan struct{})

	// Configure logs from librdkafka to be sent to our logger rather than stdout
	_ = configMap.SetKey("go.logs.channel.enable", true)
	_ = configMap.SetKey("go.logs.channel", logChan)

	adminClient, err := kafka.NewAdminClient(configMap)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize admin client: %w", err)
	}

	// A consumer is needed to perform some operations such as querying for offsets
	// by timestamp and watermarks. Since Consumer requires a group ID we need to
	// create a dummy value, even if we never use it.
	_ = configMap.SetKey("group.id", "kefka-admin")
	consumer, err := kafka.NewConsumer(configMap)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize consumer: %w", err)
	}

	ac := &AdminClient{
		admin:     adminClient,
		consumer:  consumer,
		logger:    conf.Logger,
		closeChan: closeChan,
	}
	go ac.captureLogs(logChan)

	return ac, nil
}

func (ac *AdminClient) captureLogs(logChan chan kafka.LogEvent) {
	for {
		select {
		case <-ac.closeChan:
			return
		case logEvent, ok := <-logChan:
			if !ok {
				return
			}
			ac.logger.Debug(logEvent.Message,
				slog.Group("librdkafka",
					slog.String("name", logEvent.Name),
					slog.String("tag", logEvent.Tag),
					slog.Int("level", logEvent.Level)))
		}
	}
}

// TopicPartitionsForTopic returns all the partitions for a given topic.
func (ac *AdminClient) TopicPartitionsForTopic(topic string) ([]kafka.TopicPartition, error) {
	if strings.TrimSpace(topic) == "" {
		return nil, errors.New("fetch topic partitions failed: topic name is required")
	}

	metadata, err := ac.admin.GetMetadata(&topic, false, 5000)
	if err != nil {
		return nil, fmt.Errorf("kafka: failed to get metadata: %w", err)
	}

	topicMetadata, ok := metadata.Topics[topic]
	if !ok {
		return nil, fmt.Errorf("topic %s not found", topic)
	}

	tps := make([]kafka.TopicPartition, 0, len(topicMetadata.Partitions))
	for _, pm := range topicMetadata.Partitions {
		tps = append(tps, kafka.TopicPartition{
			Topic:     &topic,
			Partition: pm.ID,
			Offset:    -2,
		})
	}

	return tps, nil
}

// TopicPartitionsForTopicAndTimestamp returns all the topic/partitions with the
// offsets set to the first offset in the partition for the given timestamp. If
// there are no offsets at or after the given tie, the offset will be set to the
// high watermark for the partition.
func (ac *AdminClient) TopicPartitionsForTopicAndTimestamp(topic string, time time.Time) ([]kafka.TopicPartition, error) {
	metadata, err := ac.admin.GetMetadata(&topic, false, 5000)
	if err != nil {
		return nil, fmt.Errorf("kafka admin client failed to get metadata: %w", err)
	}

	topicMetadata, ok := metadata.Topics[topic]
	if !ok {
		return nil, fmt.Errorf("topic %s not found", topic)
	}

	// Because the timestamp could be out of range we need to fetch the watermarks
	// for each partition to determine the offset to use in the event querying the
	// OffsetsForTimes returns an offset out of range.
	watermarks := make(map[string]Watermarks)
	for _, partition := range topicMetadata.Partitions {
		low, high, err := ac.consumer.QueryWatermarkOffsets(topic, partition.ID, 3000)
		if err != nil {
			return nil, fmt.Errorf("failed to query watermarks for partition %d: %w", partition.ID, err)
		}
		key := fmt.Sprintf("%s|%d", topic, partition.ID)
		watermarks[key] = Watermarks{Low: low, High: high}
	}

	// Build request for offsets by timestamp
	timestamp := time.UnixMilli()
	tps := make([]kafka.TopicPartition, 0, len(topicMetadata.Partitions))
	for _, partition := range topicMetadata.Partitions {
		tps = append(tps, kafka.TopicPartition{
			Topic:     &topicMetadata.Topic,
			Partition: partition.ID,
			Offset:    kafka.Offset(timestamp),
		})
	}

	offsets, err := ac.consumer.OffsetsForTimes(tps, 5000)
	if err != nil {
		return nil, fmt.Errorf("failed to query offsets for times: %w", err)
	}

	mappedTopicPartitions := make([]kafka.TopicPartition, 0, len(offsets))
	for _, tp := range offsets {
		offset := tp.Offset
		if offset < 0 {
			offset = kafka.Offset(watermarks[fmt.Sprintf("%s|%d", *tp.Topic, tp.Partition)].High)
		}
		mappedTopicPartitions = append(mappedTopicPartitions, kafka.TopicPartition{
			Topic:     tp.Topic,
			Partition: tp.Partition,
			Offset:    offset,
		})
	}

	return mappedTopicPartitions, nil
}

// WatermarksForTopic returns the low and high watermarks for each partition as
// a map of partition ID to Watermarks.
func (ac *AdminClient) WatermarksForTopic(topic string) (map[int]Watermarks, error) {
	if strings.TrimSpace(topic) == "" {
		return nil, errors.New("fetch topic partitions failed: topic name is required")
	}

	metadata, err := ac.admin.GetMetadata(&topic, false, 5000)
	if err != nil {
		return nil, fmt.Errorf("kafka: failed to get metadata: %w", err)
	}

	topicMetadata, ok := metadata.Topics[topic]
	if !ok {
		return nil, fmt.Errorf("topic %s not found", topic)
	}

	watermarks := make(map[int]Watermarks)
	for _, partition := range topicMetadata.Partitions {
		low, high, err := ac.consumer.QueryWatermarkOffsets(topic, partition.ID, 3000)
		if err != nil {
			return nil, fmt.Errorf("failed to query watermarks for partition %d: %w", partition.ID, err)
		}
		watermarks[int(partition.ID)] = Watermarks{Low: low, High: high}
	}

	return watermarks, nil
}

// WatermarksForTopicPartition returns the low and high watermarks for a given
// topic and partition.
func (ac *AdminClient) WatermarksForTopicPartition(topic string, partition int) (low int64, high int64, err error) {
	low, high, err = ac.consumer.QueryWatermarkOffsets(topic, int32(partition), 3000)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to query watermarks for partition %d: %w", partition, err)
	}

	return low, high, nil
}

// Close closes the AdminClient and any resources it holds. Once close is called
// the AdminClient is not usable.
func (ac *AdminClient) Close() {
	ac.admin.Close()
	ac.consumer.Close()
	close(ac.closeChan)
}

// IsClosed returns true if the AdminClient has been closed, otherwise false.
func (ac *AdminClient) IsClosed() bool {
	return ac.admin.IsClosed()
}

// TopicPartitionsForTopic returns all the topic/partitions for a given topic.
func TopicPartitionsForTopic(conf Config, topic string) ([]kafka.TopicPartition, error) {
	adminClient, err := NewAdminClient(conf)
	if err != nil {
		return nil, err
	}
	defer adminClient.Close()

	return adminClient.TopicPartitionsForTopic(topic)
}

// TopicPartitionsForTopicAndTimestamp returns all the topic/partitions with the
// offsets set to the first offset in the partition for the given timestamp. If
// there are no offsets at or after the given tie, the offset will be set to the
// high watermark for the partition.
func TopicPartitionsForTopicAndTimestamp(conf Config, topic string, time time.Time) ([]kafka.TopicPartition, error) {
	adminClient, err := NewAdminClient(conf)
	if err != nil {
		return nil, err
	}
	defer adminClient.Close()

	return adminClient.TopicPartitionsForTopicAndTimestamp(topic, time)
}

type Watermarks struct {
	Low  int64
	High int64
}

func adminConfigMap(conf Config) *kafka.ConfigMap {

	configMap := &kafka.ConfigMap{
		"bootstrap.servers": strings.Join(conf.BootstrapServers, ","),
		"security.protocol": conf.SecurityProtocol.String(),
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
