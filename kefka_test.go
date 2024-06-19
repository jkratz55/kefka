package kefka

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/multierr"
)

func TestLoadConfigFromEnv(t *testing.T) {
	err := multierr.Combine(
		os.Setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
		os.Setenv("KAFKA_CONSUMER_GROUP_ID", "kefka-test"),
		os.Setenv("KAFKA_CONSUMER_SESSION_TIMEOUT", "45s"),
		os.Setenv("KAFKA_CONSUMER_HEARTBEAT_INTERVAL", "5s"),
		os.Setenv("KAFKA_CONSUMER_COMMIT_INTERVAL", "10s"),
		os.Setenv("KAFKA_CONSUMER_POLL_TIMEOUT", "200ms"),
		os.Setenv("KAFKA_CONSUMER_AUTO_OFFSET_RESET", "earliest"),
		os.Setenv("KAFKA_MAX_BYTES", "2097152"),
		os.Setenv("KAFKA_MAX_FETCH_BYTES", "52428800"),
		os.Setenv("KAFKA_SECURITY_PROTOCOL", "sasl_ssl"),
		os.Setenv("KAFKA_SASL_MECHANISM", "PLAIN"),
		os.Setenv("KAFKA_SASL_USER", "awesomeuser"),
		os.Setenv("KAFKA_SASL_PASSWORD", "awesomepassword"),
		os.Setenv("KAFKA_PRODUCER_REQUIRED_ACKS", "leader"),
		os.Setenv("KAFKA_PRODUCER_IDEMPOTENCE", "false"),
	)
	if err != nil {
		t.Fatalf("failed to initialize environment variables: %v", err)
	}

	conf, err := LoadConfigFromEnv()
	assert.NoError(t, err)

	expected := Config{
		BootstrapServers:             []string{"localhost:9092"},
		GroupID:                      "kefka-test",
		SessionTimeout:               time.Second * 45,
		HeartbeatInterval:            time.Second * 5,
		CommitInterval:               time.Second * 10,
		PollTimeout:                  time.Millisecond * 200,
		AutoOffsetReset:              Earliest,
		MessageMaxBytes:              2097152,
		MaxFetchBytes:                52428800,
		SecurityProtocol:             SaslSsl,
		CertificateAuthorityLocation: "",
		CertificateLocation:          "",
		CertificateKeyLocation:       "",
		CertificateKeyPassword:       "",
		SkipTlsVerification:          false,
		SASLMechanism:                Plain,
		SASLUsername:                 "awesomeuser",
		SASLPassword:                 "awesomepassword",
		RequiredAcks:                 AckLeader,
		Idempotence:                  false,
		TransactionID:                "",
		Logger:                       DefaultLogger(),
	}

	assert.Equal(t, expected, conf)
}
