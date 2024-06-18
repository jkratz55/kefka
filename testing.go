package kefka

import (
	"context"

	"github.com/testcontainers/testcontainers-go"
	testKafka "github.com/testcontainers/testcontainers-go/modules/kafka"

	"github.com/jkratz55/kefka/v2/internal"
)

func initKafkaTestContainer(ctx context.Context) (*internal.KafkaContainer, error) {
	return internal.RunContainer(ctx,
		testKafka.WithClusterID("test-cluster"),
		testcontainers.WithImage("confluentinc/confluent-local:7.5.0"),
	)
}
