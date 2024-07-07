package kefka

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/sethvargo/go-envconfig"
	"gopkg.in/yaml.v3"
)

const version = "2.0.0"

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
	BootstrapServers []string `env:"KAFKA_BOOTSTRAP_SERVERS, required"`

	// The ID of the consumer group to join. This is a required field when using
	// Consumer.
	//
	// Applies To: Consumer
	GroupID string `env:"KAFKA_CONSUMER_GROUP_ID"`

	// Client group session and failure detection timeout. The consumer sends
	// periodic heartbeats to indicate its liveness to the broker. If no heart
	// beats are received by the broker for a group member within the session
	// timeout, the broker will remove the consumer from the group and trigger
	// a rebalance.
	//
	// The unit is milliseconds with a default of 45000 (45 seconds).
	//
	// Applies To: Consumer
	SessionTimeout time.Duration `env:"KAFKA_CONSUMER_SESSION_TIMEOUT, default=45s"`

	// Interval at which the consumer sends heartbeats to the broker. The default
	// is 3000 (3 seconds).
	//
	// Applies To: Consumer
	HeartbeatInterval time.Duration `env:"KAFKA_CONSUMER_HEARTBEAT_INTERVAL, default=3s"`

	// The interval between committing offsets back to Kafka brokers. The default
	// is 5000ms (5 seconds). When set to -1 the offsets will be commited after
	// processing each message. This can be useful for certain workloads where
	// you absolutely don't want to reprocess messages. However, this can have
	// significant implications on throughput and performance, and is generally
	// not recommended.
	//
	// Applies To: Consumer
	CommitInterval time.Duration `env:"KAFKA_CONSUMER_COMMIT_INTERVAL, default=5s"`

	//
	PollTimeout time.Duration `env:"KAFKA_CONSUMER_POLL_TIMEOUT, default=100ms"`

	// Configures the behavior when there are no stored offsets found for the
	// Consumer group for topic/partition.
	//
	// The default is latest which means that the consumer will start reading
	// from the latest message in the topic/partition.
	//
	// Applies To: Consumer
	AutoOffsetReset AutoOffsetReset `env:"KAFKA_CONSUMER_AUTO_OFFSET_RESET, default=latest"`

	// The maximum size for a message. The default is 1048576 (1MB).
	//
	// Applies To: Consumer, Producer
	MessageMaxBytes int `env:"KAFKA_MAX_BYTES, default=1048576"`

	// Maximum amount of data the broker shall return for a Fetch request. Messages
	// are fetched in batches by the consumer. The default is 52428800 (50MB).
	//
	// Applies To: Consumer
	MaxFetchBytes int `env:"KAFKA_MAX_FETCH_BYTES, default=52428800"`

	// The security protocol used to communicate with the brokers.
	//
	// Valid values are: plaintext, ssl, sasl_plaintext, sasl_ssl.
	//
	// Applies To: Consumer, Producer
	SecurityProtocol SecurityProtocol `env:"KAFKA_SECURITY_PROTOCOL, default=plaintext"`

	// The location of the certificate authority file used to verify the brokers.
	//
	// Applies To: Consumer, Producer
	CertificateAuthorityLocation string `env:"KAFKA_CA_LOCATION"`

	// The location of the client certificate used to authenticate with the brokers.
	//
	// Applies To: Consumer, Producer
	CertificateLocation string `env:"KAFKA_CERT_LOCATION"`

	// The location of the key for the client certificate.
	//
	// Applies To: Consumer, Producer
	CertificateKeyLocation string `env:"KAFKA_CERT_KEY_LOCATION"`

	// The password for the key used for the client certificate.
	//
	// Applies To: Consumer, Producer
	CertificateKeyPassword string `env:"KAFKA_CERT_KEY_PASSWORD"`

	// Skip TLS verification when using SSL or SASL_SSL.
	//
	// Applies To: Consumer, Producer
	SkipTlsVerification bool `env:"KAFKA_SKIP_TLS_VERIFICATION, default=false"`

	// The SASL mechanism to use for SASL authentication.
	//
	// Applies To: Consumer, Producer
	SASLMechanism SaslMechanism `env:"KAFKA_SASL_MECHANISM, default=PLAIN"`

	// The username for authenticating with SASL.
	//
	// Applies To: Consumer, Producer
	SASLUsername string `env:"KAFKA_SASL_USER"`

	// The password for authenticating with SASL.
	//
	// Applies To: Consumer, Producer
	SASLPassword string `env:"KAFKA_SASL_PASSWORD"`

	// The number of acknowledgements the producer requires the leader to have
	// received before considering a request complete. The default value is
	// AckAll ("all"), which will wait for all in-sync replicas to acknowledge
	// before proceeding.
	//
	// Applies To: Producer
	RequiredAcks Ack `env:"KAFKA_PRODUCER_REQUIRED_ACKS, default=all"`

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
	Idempotence bool `env:"KAFKA_PRODUCER_IDEMPOTENCE, default=false"`

	// The transactional ID to use for messages produced by the producer. This
	// is used to ensure that messages are produced atomically and in order. This
	// is required when using transactions.
	//
	// Applies To: Producer
	TransactionID string `env:"KAFKA_PRODUCER_TRANSACTION_ID"`

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

func LoadConfigFromFile(fp string) (Config, error) {
	type config struct {
		BootstrapServers             []string         `json:"bootstrapServers" yaml:"bootstrapServers"`
		GroupID                      string           `json:"groupId" yaml:"groupId"`
		SessionTimeout               Duration         `json:"sessionTimeout" yaml:"sessionTimeout"`
		HeartbeatInterval            Duration         `json:"heartbeatInterval" yaml:"heartbeatInterval"`
		CommitInterval               Duration         `json:"commitInterval" yaml:"commitInterval"`
		PollTimeout                  Duration         `json:"pollTimeout" yaml:"pollTimeout"`
		AutoOffsetReset              AutoOffsetReset  `json:"autoOffsetReset" yaml:"autoOffsetReset"`
		MessageMaxBytes              int              `json:"messageMaxBytes" yaml:"messageMaxBytes"`
		MaxFetchBytes                int              `json:"maxFetchBytes" yaml:"maxFetchBytes"`
		SecurityProtocol             SecurityProtocol `json:"securityProtocol" yaml:"securityProtocol"`
		CertificateAuthorityLocation string           `json:"certificateAuthorityLocation" yaml:"certificateAuthorityLocation"`
		CertificateLocation          string           `json:"certificateLocation" yaml:"certificateLocation"`
		CertificateKeyLocation       string           `json:"certificateKeyLocation" yaml:"certificateKeyLocation"`
		CertificateKeyPassword       string           `json:"certificateKeyPassword" yaml:"certificateKeyPassword"`
		SkipTlsVerification          bool             `json:"skipTlsVerification" yaml:"skipTlsVerification"`
		SASLMechanism                SaslMechanism    `json:"saslMechanism" yaml:"saslMechanism"`
		SASLUsername                 string           `json:"saslUsername" yaml:"saslUsername"`
		SASLPassword                 string           `json:"saslPassword" yaml:"saslPassword"`
		RequiredAcks                 Ack              `json:"requiredAcks" yaml:"requiredAcks"`
		Idempotence                  bool             `json:"idempotence" yaml:"idempotence"`
		TransactionID                string           `json:"transactionID" yaml:"transactionID"`
	}

	file, err := os.Open(fp)
	if err != nil {
		return Config{}, fmt.Errorf("failed to open file: %w", err)
	}

	contents, err := io.ReadAll(file)
	if err != nil {
		return Config{}, fmt.Errorf("failed to read file: %w", err)
	}

	var conf config
	ext := filepath.Ext(fp)
	switch ext {
	case ".json":
		err := json.Unmarshal(contents, &conf)
		if err != nil {
			return Config{}, fmt.Errorf("failed to unmarshal json: %w", err)
		}
		kafkaConf := Config{
			BootstrapServers:             conf.BootstrapServers,
			GroupID:                      conf.GroupID,
			SessionTimeout:               conf.SessionTimeout.Duration,
			HeartbeatInterval:            conf.HeartbeatInterval.Duration,
			CommitInterval:               conf.CommitInterval.Duration,
			PollTimeout:                  conf.PollTimeout.Duration,
			AutoOffsetReset:              conf.AutoOffsetReset,
			MessageMaxBytes:              conf.MessageMaxBytes,
			MaxFetchBytes:                conf.MaxFetchBytes,
			SecurityProtocol:             conf.SecurityProtocol,
			CertificateAuthorityLocation: conf.CertificateAuthorityLocation,
			CertificateLocation:          conf.CertificateLocation,
			CertificateKeyLocation:       conf.CertificateKeyLocation,
			CertificateKeyPassword:       conf.CertificateKeyPassword,
			SkipTlsVerification:          conf.SkipTlsVerification,
			SASLMechanism:                conf.SASLMechanism,
			SASLUsername:                 conf.SASLUsername,
			SASLPassword:                 conf.SASLPassword,
			RequiredAcks:                 conf.RequiredAcks,
			Idempotence:                  conf.Idempotence,
			TransactionID:                conf.TransactionID,
		}
		kafkaConf.init()
		return kafkaConf, nil
	case ".yaml":
		err := yaml.Unmarshal(contents, &conf)
		if err != nil {
			return Config{}, fmt.Errorf("failed to unmarshal yaml: %w", err)
		}
		kafkaConf := Config{
			BootstrapServers:             conf.BootstrapServers,
			GroupID:                      conf.GroupID,
			SessionTimeout:               conf.SessionTimeout.Duration,
			HeartbeatInterval:            conf.HeartbeatInterval.Duration,
			CommitInterval:               conf.CommitInterval.Duration,
			PollTimeout:                  conf.PollTimeout.Duration,
			AutoOffsetReset:              conf.AutoOffsetReset,
			MessageMaxBytes:              conf.MessageMaxBytes,
			MaxFetchBytes:                conf.MaxFetchBytes,
			SecurityProtocol:             conf.SecurityProtocol,
			CertificateAuthorityLocation: conf.CertificateAuthorityLocation,
			CertificateLocation:          conf.CertificateLocation,
			CertificateKeyLocation:       conf.CertificateKeyLocation,
			CertificateKeyPassword:       conf.CertificateKeyPassword,
			SkipTlsVerification:          conf.SkipTlsVerification,
			SASLMechanism:                conf.SASLMechanism,
			SASLUsername:                 conf.SASLUsername,
			SASLPassword:                 conf.SASLPassword,
			RequiredAcks:                 conf.RequiredAcks,
			Idempotence:                  conf.Idempotence,
			TransactionID:                conf.TransactionID,
		}
		kafkaConf.init()
		return kafkaConf, nil
	default:
		return Config{}, fmt.Errorf("unsupported file type: %s", ext)
	}
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
