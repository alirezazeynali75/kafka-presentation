package config

import (
	"time"

	"github.com/Shopify/sarama"
)

type KafkaConfig struct {
	Brokers  []string `env:"KAFKA_BROKERS"   envSeparator:"," envDefault:"localhost:9092"`
	ClientID string   `env:"KAFKA_CLIENT_ID"                  envDefault:"test-kafka"`
	TxID     string   `env:"KAFKA_TX_ID"                      envDefault:"blockchain-knowledge-sharing"`
}


func (conf KafkaConfig) ToSaramaConfig() (*sarama.Config, error) {
	kafkaConfig := sarama.NewConfig()

	kafkaConfig.ClientID = conf.ClientID

	kafkaConfig.Producer.Idempotent = true
	kafkaConfig.Producer.RequiredAcks = sarama.WaitForAll
	kafkaConfig.Producer.Return.Successes = true
	kafkaConfig.Producer.Return.Errors = true
	kafkaConfig.Producer.Retry.Max = 30
	kafkaConfig.Producer.Partitioner = sarama.NewHashPartitioner
	kafkaConfig.Producer.Transaction.Retry.Backoff = 10
	kafkaConfig.Net.MaxOpenRequests = 1
	kafkaConfig.Producer.Transaction.ID = "test-kafka-id"

	kafkaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	kafkaConfig.Consumer.Offsets.Retry.Max = 30
	kafkaConfig.Consumer.MaxProcessingTime = 15 * time.Second
	kafkaConfig.Consumer.Return.Errors = true
	kafkaConfig.Consumer.Group.Rebalance.Timeout = 45 * time.Second

	kafkaConfig.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{
		sarama.BalanceStrategyRoundRobin,
	}

	return kafkaConfig, nil
}