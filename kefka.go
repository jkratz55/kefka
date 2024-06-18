package kefka

import (
	"context"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/sethvargo/go-envconfig"
)

const (
	defaultMessageMaxBytes   = 1048576  // 1MiB
	defaultMaxFetchBytes     = 52428800 // 50MiB
	defaultSessionTimeout    = time.Second * 45
	defaultHeartbeatInterval = time.Second * 3
	defaultCommitInterval    = time.Second * 5
	defaultOffsetReset       = Latest
	defaultRequiredAcks      = AckAll
	defaultSecurityProtocol  = Plaintext
	defaultPollTimeout       = time.Millisecond * 100
)

type Config struct {
	// The Kafka brokers addresses used to establish the initial connection to
	// Kafka. This is a required field.
	//
	// Applies To: Consumer, Producer
	BootstrapServers []string `env:"KAFKA_BOOTSTRAP_SERVERS, required" json:"bootstrapServers" yaml:"bootstrapServers"`

	// The ID of the consumer group to join. This is a required field when using
	// Consumer.
	//
	// Applies To: Consumer
	GroupID string `env:"KAFKA_CONSUMER_GROUP_ID" json:"groupId" yaml:"groupId"`

	// Client group session and failure detection timeout. The consumer sends
	// periodic heartbeats to indicate its liveness to the broker. If no heart
	// beats are received by the broker for a group member within the session
	// timeout, the broker will remove the consumer from the group and trigger
	// a rebalance.
	//
	// The unit is milliseconds with a default of 45000 (45 seconds).
	//
	// Applies To: Consumer
	SessionTimeout time.Duration `env:"KAFKA_CONSUMER_SESSION_TIMEOUT, default=45s" json:"sessionTimeout" yaml:"sessionTimeout"`

	// Interval at which the consumer sends heartbeats to the broker. The default
	// is 3000 (3 seconds).
	//
	// Applies To: Consumer
	HeartbeatInterval time.Duration `env:"KAFKA_CONSUMER_HEARTBEAT_INTERVAL, default=3s" json:"heartbeatInterval" yaml:"heartbeatInterval"`

	// The interval between committing offsets back to Kafka brokers. The default
	// is 5000ms (5 seconds). When set to -1 the offsets will be commited after
	// processing each message. This can be useful for certain workloads where
	// you absolutely don't want to reprocess messages. However, this can have
	// significant implications on throughput and performance, and is generally
	// not recommended.
	//
	// Applies To: Consumer
	CommitInterval time.Duration `env:"KAFKA_CONSUMER_COMMIT_INTERVAL, default=5s" json:"commitInterval" yaml:"commitInterval"`

	//
	PollTimeout time.Duration `env:"KAFKA_CONSUMER_POLL_TIMEOUT, default=100ms" json:"pollTimeout" yaml:"pollTimeout"`

	// Configures the behavior when there are no stored offsets found for the
	// Consumer group for topic/partition.
	//
	// The default is latest which means that the consumer will start reading
	// from the latest message in the topic/partition.
	//
	// Applies To: Consumer
	AutoOffsetReset AutoOffsetReset `env:"KAFKA_CONSUMER_AUTO_OFFSET_RESET, default=latest" json:"autoOffsetReset" yaml:"autoOffsetReset"`

	// The maximum size for a message. The default is 1048576 (1MB).
	//
	// Applies To: Consumer, Producer
	MessageMaxBytes int `env:"KAFKA_MAX_BYTES, default=1048576" json:"messageMaxBytes" yaml:"messageMaxBytes"`

	// Maximum amount of data the broker shall return for a Fetch request. Messages
	// are fetched in batches by the consumer. The default is 52428800 (50MB).
	//
	// Applies To: Consumer
	MaxFetchBytes int `env:"KAFKA_MAX_FETCH_BYTES, default=52428800" json:"maxFetchBytes" yaml:"maxFetchBytes"`

	// The security protocol used to communicate with the brokers.
	//
	// Valid values are: plaintext, ssl, sasl_plaintext, sasl_ssl.
	//
	// Applies To: Consumer, Producer
	SecurityProtocol SecurityProtocol `env:"KAFKA_SECURITY_PROTOCOL, default=plaintext" json:"securityProtocol" yaml:"securityProtocol"`

	// The location of the certificate authority file used to verify the brokers.
	//
	// Applies To: Consumer, Producer
	CertificateAuthorityLocation string `env:"KAFKA_CA_LOCATION" json:"certificateAuthorityLocation" yaml:"certificateAuthorityLocation"`

	// The location of the client certificate used to authenticate with the brokers.
	//
	// Applies To: Consumer, Producer
	CertificateLocation string `env:"KAFKA_CERT_LOCATION" json:"certificateLocation" yaml:"certificateLocation"`

	// The location of the key for the client certificate.
	//
	// Applies To: Consumer, Producer
	CertificateKeyLocation string `env:"KAFKA_CERT_KEY_LOCATION" json:"certificateKeyLocation" yaml:"certificateKeyLocation"`

	// The password for the key used for the client certificate.
	//
	// Applies To: Consumer, Producer
	CertificateKeyPassword string `env:"KAFKA_CERT_KEY_PASSWORD" json:"certificateKeyPassword" yaml:"certificateKeyPassword"`

	// Skip TLS verification when using SSL or SASL_SSL.
	//
	// Applies To: Consumer, Producer
	SkipTlsVerification bool `env:"KAFKA_SKIP_TLS_VERIFICATION, default=false" json:"skipTlsVerification" yaml:"skipTlsVerification"`

	// The SASL mechanism to use for SASL authentication.
	//
	// Applies To: Consumer, Producer
	SASLMechanism SaslMechanism `env:"KAFKA_SASL_MECHANISM, default=PLAIN" json:"saslMechanism" yaml:"saslMechanism"`

	// The username for authenticating with SASL.
	//
	// Applies To: Consumer, Producer
	SASLUsername string `env:"KAFKA_SASL_USER" json:"saslUsername" yaml:"saslUsername"`

	// The password for authenticating with SASL.
	//
	// Applies To: Consumer, Producer
	SASLPassword string `env:"KAFKA_SASL_PASSWORD" json:"saslPassword" yaml:"saslPassword"`

	// The number of acknowledgements the producer requires the leader to have
	// received before considering a request complete. The default value is
	// AckAll ("all"), which will wait for all in-sync replicas to acknowledge
	// before proceeding.
	//
	// Applies To: Producer
	RequiredAcks Ack `env:"KAFKA_PRODUCER_REQUIRED_ACKS, default=all" json:"requiredAcks" yaml:"requiredAcks"`

	// When set to `true`, the producer will ensure that messages are successfully
	// produced exactly once and in the original produce order.
	//
	// The following configuration properties are adjusted automatically (if not modified
	// by the user) when idempotence is enabled: `max.in.flight.requests.per.connection=5`
	// (must be less than or equal to 5), `retries=INT32_MAX` (must be greater than 0),
	// `acks=all`, `queuing.strategy=fifo`. Producer instantation will fail if user-supplied
	// configuration is incompatible.
	//
	// Applies To: Producer
	Idempotence bool `env:"KAFKA_PRODUCER_IDEMPOTENCE, default=false" json:"idempotence" yaml:"idempotence"`

	// The transactional ID to use for messages produced by the producer. This
	// is used to ensure that messages are produced atomically and in order. This
	// is required when using transactions.
	//
	// Applies To: Producer
	TransactionID string `env:"KAFKA_PRODUCER_TRANSACTION_ID" json:"transactionId" yaml:"transactionId"`

	// Configures the logger used by the Consumer and Producer types.
	//
	// When nil a default logger will be used that logs to os.Stderr at ERROR
	// level using JSON format.
	//
	// Note: Logs from librdkafka are logged at DEBUG level. If the provided
	// logger is configured at DEBUG level, you will see logs from librdkafka
	// as well as Kefka logs.
	//
	// Applies To: Consumer, Producer
	Logger *slog.Logger

	// A callback that is called when Confluent Kafka Client/librdkafka returns
	// a Kafka error. This can be useful for logging errors or capturing metrics.
	// The default value is nil and won't be called.
	//
	// Applies To: Consumer, Producer
	OnError func(err error)
}

// init initializes the configuration with default values when the zero value
// is present on the type.
func (c *Config) init() {
	if c.SessionTimeout == 0 {
		c.SessionTimeout = defaultSessionTimeout
	}
	if c.HeartbeatInterval == 0 {
		c.HeartbeatInterval = defaultHeartbeatInterval
	}
	if c.AutoOffsetReset == "" {
		c.AutoOffsetReset = defaultOffsetReset
	}
	if c.MessageMaxBytes == 0 {
		c.MessageMaxBytes = defaultMessageMaxBytes
	}
	if c.MaxFetchBytes == 0 {
		c.MaxFetchBytes = defaultMaxFetchBytes
	}
	if c.CommitInterval == 0 {
		c.CommitInterval = defaultCommitInterval
	}
	if c.RequiredAcks == "" {
		c.RequiredAcks = defaultRequiredAcks
	}
	if c.SecurityProtocol == "" {
		c.SecurityProtocol = defaultSecurityProtocol
	}
	if c.PollTimeout == 0 {
		c.PollTimeout = defaultPollTimeout
	}
	if c.Logger == nil {
		c.Logger = DefaultLogger()
	}
}

// LoadConfigFromEnv loads the configuration from the environment and returns
// an instance of Config populated from the environment.
func LoadConfigFromEnv() (Config, error) {
	var c Config
	if err := envconfig.Process(context.Background(), &c); err != nil {
		return Config{}, fmt.Errorf("failed to load config from env: %w", err)
	}
	c.init()

	// Because the way envconfig works, we need to set the logger after calling
	// init() to ensure it has a valid logger. Because the way interfaces work
	// in Go and nil checks the nil check for the logger done in init will always
	// be true but the internal handler for slog.Logger will be nil leading to
	// runtime panics.
	c.Logger = DefaultLogger()
	return c, nil
}

// producerConfigMap prints the Kafka configuration to stdout for debugging
// while obfuscating any password fields.
func printConfigMap(cm *kafka.ConfigMap) {
	fmt.Println("Kafka Config:")
	for key, value := range *cm {
		if strings.Contains(key, "password") {
			fmt.Printf("\t%s: %s\n", key, "********")
		} else {
			fmt.Printf("\t%s: %s\n", key, value)
		}
	}
}

// obfuscateConfig returns a copy of the ConfigMap with any password fields
// or non-serializable fields obfuscated. This is for internal use only to
// log the configuration used to initialize the kafka client.
func obfuscateConfig(c *kafka.ConfigMap) map[string]interface{} {
	obfuscated := make(map[string]interface{})
	for k, v := range *c {
		switch reflect.TypeOf(v).Kind() {
		case reflect.Chan:
			// Channels are not serializable, so we skip them
			continue
		default:
			if strings.Contains(k, "password") {
				obfuscated[k] = "********"
			} else {
				obfuscated[k] = v
			}
		}
	}
	return obfuscated
}
