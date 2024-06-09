package experimental

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type KafkaFactory struct {
	BootstrapServers  []string
	GroupID           string
	AutoOffsetReset   AutoOffsetReset
	CommitInterval    time.Duration
	SessionTimeout    time.Duration
	HeartbeatInterval time.Duration
	MaxBytes          int
	SecurityProtocol  SecurityProtocol
	SSL               struct {
		CACertificateLocation string
		CertificateLocation   string
		KeyLocation           string
		KeyPassword           string
		SkipVerification      bool
	}
	SASL struct {
		Mechanism SaslMechanism
		Username  string
		Password  string
		OAuth     struct {
			Method           string
			ClientID         string
			ClientSecret     string
			TokenEndpointURL string
			Extensions       string
			Scope            string
		}
	}
	Logger *slog.Logger
}

// ConfigMap returns a kafka.ConfigMap that contains all the common configuration
// properties (meaning properties that are not specific to the consumer or producer).
func (kf *KafkaFactory) configMap() *kafka.ConfigMap {

	if kf.Logger == nil {
		kf.Logger = defaultLogger()
	}

	// Because we don't want librdkafka to log to stderr/stdout the logs are sent
	// to a channel and then processed by a goroutine that will log the messages
	// converting the levels to slog levels.
	logChan := make(chan kafka.LogEvent, 1000)
	go func() {
		logEvent := <-logChan
		lvl := mapLibrdKafkaLevel(logEvent.Level)
		kf.Logger.Log(context.Background(), lvl, logEvent.Message,
			slog.String("source", "librdkafka"),
			slog.Group("librdkafka",
				slog.String("name", logEvent.Name),
				slog.String("tag", logEvent.Tag)))
	}()

	// Follow the same default as librdkafka
	if kf.SecurityProtocol == "" {
		kf.SecurityProtocol = Plaintext
	}

	configMap := &kafka.ConfigMap{
		"bootstrap.servers":      strings.Join(kf.BootstrapServers, ","),
		"security.protocol":      kf.SecurityProtocol.String(),
		"go.logs.channel.enable": true,
		"go.logs.channel":        logChan,
	}

	if kf.SecurityProtocol == Ssl || kf.SecurityProtocol == SaslSsl {
		_ = configMap.SetKey("ssl.ca.location", kf.SSL.CACertificateLocation)
		_ = configMap.SetKey("ssl.certificate.location", kf.SSL.CertificateLocation)
		_ = configMap.SetKey("ssl.key.location", kf.SSL.KeyLocation)
		_ = configMap.SetKey("ssl.key.password", kf.SSL.KeyPassword)
		if kf.SSL.SkipVerification {
			_ = configMap.SetKey("enable.ssl.certificate.verification", false)
		}
	}

	if kf.SecurityProtocol == SaslPlaintext || kf.SecurityProtocol == SaslSsl {
		_ = configMap.SetKey("sasl.mechanism", kf.SASL.Mechanism.String())
		if kf.SASL.Mechanism == Plain || kf.SASL.Mechanism == ScramSha256 ||
			kf.SASL.Mechanism == ScramSha512 {
			_ = configMap.SetKey("sasl.username", kf.SASL.Username)
			_ = configMap.SetKey("sasl.password", kf.SASL.Password)
		}
		if kf.SASL.Mechanism == OAuthBearer {
			_ = configMap.SetKey("sasl.oauthbearer.method", kf.SASL.OAuth.Method)
			_ = configMap.SetKey("sasl.oauthbearer.client.id", kf.SASL.OAuth.ClientID)
			_ = configMap.SetKey("sasl.oauthbearer.client.secret", kf.SASL.OAuth.ClientSecret)
			_ = configMap.SetKey("sasl.oauthbearer.token.endpoint.url", kf.SASL.OAuth.TokenEndpointURL)
			_ = configMap.SetKey("sasl.oauthbearer.extensions", kf.SASL.OAuth.Extensions)
			_ = configMap.SetKey("sasl.oauthbearer.scope", kf.SASL.OAuth.Scope)
		}
	}

	return configMap
}

func (kf *KafkaFactory) CreateConsumer(handler Handler, topics ...string) {

	// Follow the same default as librdkafka
	if kf.AutoOffsetReset == "" {
		kf.AutoOffsetReset = Latest
	}

	commonProperties := kf.configMap()
	consumerProperties := kafka.ConfigMap{
		"group.id":                 kf.GroupID,
		"auto.offset.reset":        kf.AutoOffsetReset.String(),
		"enable.auto.offset.store": false,
	}

	// If some properties are zero value on KafkaFactory we don't want to set them.
	// Instead, we will rely on the defaults from the underlying Confluent Kafka and
	// librdkafka libraries.
	if kf.SessionTimeout > 0 {
		consumerProperties["session.timeout.ms"] = int(kf.SessionTimeout.Milliseconds())
	}
	if kf.HeartbeatInterval > 0 {
		consumerProperties["heartbeat.interval.ms"] = int(kf.HeartbeatInterval.Milliseconds())
	}
	if kf.CommitInterval > 0 {
		consumerProperties["auto.commit.interval.ms"] = int(kf.CommitInterval.Milliseconds())
	}
	if kf.MaxBytes > 0 {
		consumerProperties["message.max.bytes"] = kf.MaxBytes
	}

	for k, v := range *commonProperties {
		consumerProperties[k] = v
	}

	baseConsumer, err := kafka.NewConsumer(&consumerProperties)
	if err != nil {
		// todo: handler error
	}

	if err := baseConsumer.SubscribeTopics(topics, nil); err != nil {
		// todo: handle error
	}

	// TODO implement me
	panic("implement me")
}

func (kf *KafkaFactory) CreateProducer() {
	// TODO implement me
	panic("implement me")
}

func (kf *KafkaFactory) CreateAdmin() *kafka.AdminClient {
	// TODO implement me
	panic("implement me")
}
