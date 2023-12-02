package main

import (
	"context"
	"kafka-presentation/internal/config"
	"kafka-presentation/internal/eventbus"
	"os"
	"time"

	"github.com/Shopify/sarama"
	"github.com/hashicorp/go-uuid"
	"github.com/sirupsen/logrus"
)

func main () {
	logger := logrus.New()
	cfg, err := config.Configure()
	if err != nil {
		logger.WithError(err).Fatal("can not load envs")
	}
	logger.SetOutput(os.Stdout)
	logger.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
		DisableColors: false,
	})

	kafkaConfig, err := cfg.Kafka.ToSaramaConfig()
	if err != nil {
		logger.WithError(err).Fatal("can not create sarama config")
	}
	kafkaProducer, err := sarama.NewSyncProducer(cfg.Kafka.Brokers, kafkaConfig)
	if err != nil {
		logger.WithError(err).Fatal("can not create producer")
	}
	eventPublisher := eventbus.NewEventPublisher(logger, kafkaProducer)
	uuid, err := uuid.GenerateUUID()
	if err != nil {
		logger.WithError(err).Fatal("can not generate uuid")
	}
	payload := []byte("{}")
	messages := []eventbus.Message{
		{
			Topic: "test-kafka-consumer",
			Key: []byte(uuid),
			Payload: payload,
			Time: time.Now(),
		},
		{
			Topic: "test-kafka-consumer",
			Key: []byte(uuid),
			Payload: payload,
			Time: time.Now(),
		},
		{
			Topic: "test-kafka-consumer",
			Key: []byte(uuid),
			Payload: payload,
			Time: time.Now(),
		},
		{
			Topic: "test-kafka-consumer",
			Key: []byte(uuid),
			Payload: payload,
			Time: time.Now(),
		},
	}
	err = eventPublisher.PublishMessages(context.Background(), messages)
	if err != nil {
		logger.WithError(err).Fatal("can not produce")
	}
	eventPublisher.Shutdown(context.Background())
	logger.Info("App closed successfully")
}