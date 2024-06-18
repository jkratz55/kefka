package kefka

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"

	"github.com/jkratz55/kefka/v2/internal"
)

func TestProducerSuite(t *testing.T) {
	if !internal.IsTestContainersEnabled() {
		t.Skip("testcontainers not enabled")
	}
	suite.Run(t, new(producerTestSuite))
}

type producerTestSuite struct {
	suite.Suite

	kafkaContainer *internal.KafkaContainer
	producer       *Producer
}

func (s *producerTestSuite) SetupSuite() {
	ctx := context.Background()
	kafkaContainer, err := initKafkaTestContainer(ctx)
	s.Require().NoError(err)
	s.kafkaContainer = kafkaContainer

	brokers, err := kafkaContainer.Brokers(ctx)
	s.Require().NoError(err)

	producer, err := NewProducer(Config{
		BootstrapServers: brokers,
		RequiredAcks:     AckLeader,
		Logger:           NopLogger(),
	})
	s.Require().NoError(err)
	s.producer = producer
}

func (s *producerTestSuite) TearDownSuite() {
	remaining := s.producer.Flush(time.Second * 5)
	s.Require().Equal(0, remaining)
	s.producer.Close()
	s.Require().Equal(true, s.producer.IsClosed())

	if err := s.kafkaContainer.Terminate(context.Background()); err != nil {
		s.FailNow(err.Error())
	}
}

func (s *producerTestSuite) TestProducer_Produce() {
	topic := "test"
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Value: []byte("world"),
		Key:   []byte("hello"),
	}

	ch := make(chan kafka.Event)
	err := s.producer.Produce(msg, ch)
	s.Require().NoError(err)

	e := <-ch
	switch ev := e.(type) {
	case *kafka.Message:
		s.Require().NoError(ev.TopicPartition.Error)
		s.Require().Equal("test", *ev.TopicPartition.Topic)
		s.Require().Equal([]byte("hello"), ev.Key)
		s.Require().Equal([]byte("world"), ev.Value)
	default:
		s.Error(fmt.Errorf("unexpected kafka event: %T", e))
	}
}

func (s *producerTestSuite) TestProducer_ProduceAndWait() {
	topic := "test"
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Value: []byte("world"),
		Key:   []byte("hello"),
	}

	err := s.producer.ProduceAndWait(msg)
	s.Require().NoError(err)
}

func (s *producerTestSuite) TestProducer_Transactional() {

	// Transactions requires a different Producer configuration
	brokers, err := s.kafkaContainer.Brokers(context.Background())
	s.Require().NoError(err)

	producer, err := NewProducer(Config{
		BootstrapServers: brokers,
		RequiredAcks:     AckAll,
		Logger:           NopLogger(),
		Idempotence:      true,
		TransactionID:    uuid.New().String(),
	})
	s.Require().NoError(err)
	s.producer = producer

	topic := "test"
	messages := []*kafka.Message{
		{
			TopicPartition: kafka.TopicPartition{
				Topic:     &topic,
				Partition: kafka.PartitionAny,
			},
			Value: []byte("world"),
			Key:   []byte("hello"),
		},
		{
			TopicPartition: kafka.TopicPartition{
				Topic:     &topic,
				Partition: kafka.PartitionAny,
			},
			Value: []byte("world"),
			Key:   []byte("hello"),
		},
	}

	err = s.producer.Transactional(context.Background(), messages)
	s.Require().NoError(err)
}
