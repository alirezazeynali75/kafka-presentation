package main

import (
	"context"
	"kafka-presentation/internal/config"
	"kafka-presentation/internal/kafkarouter"
	"os"
	"os/signal"

	"github.com/Shopify/sarama"
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

	handler := func(ctx context.Context, msg *kafkarouter.Message) error {
		key := string(msg.Key)
		value := string(msg.Payload)
		time := msg.Timestamp
		topic := msg.Topic
		logger.WithFields(logrus.Fields{
			"key": key,
			"value": value,
			"time": time,
			"topic": topic,
		}).Info("new message came")
		return nil
	}

	topic := "test-kafka-consumer"
	kafkaConfig, err := cfg.Kafka.ToSaramaConfig()
	if err != nil {
		logger.WithError(err).Fatal("failed to load kafka configuration")
	}

	groupID := "test-kafka"
	client, err := sarama.NewConsumerGroup(cfg.Kafka.Brokers, groupID, kafkaConfig)
	if err != nil {
		logger.WithError(err).Fatal("can not connect to kafka")
	}
	router := kafkarouter.New(logger, client)
	err = router.RegisterHandler(topic, handler)
	if err != nil {
		logger.WithError(err).Fatal("can not register withdrawals handler")
		return
	}

	go func() {
		err = router.Start(context.Background())
		if err != nil {
			logger.WithError(err).Fatal("cannot run kafka router")
		}
	}()

	go func() {
		errChan := client.Errors()
		for err := range errChan {
			logger.WithError(err).Error("received kafka client error")
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)
	<-quit
	logger.Info("try to shutdown the app")
	err = router.Shutdown(context.Background())
	if err != nil {
		logger.WithError(err).Warn("can not shutdown router")
	}
	logger.Info("shutdown process was successful")
}


