package eventbus

import (
	"context"
	"fmt"
	"time"

	"github.com/Shopify/sarama"
	"github.com/sirupsen/logrus"
)

func NewEventPublisher(logger *logrus.Logger, producer sarama.SyncProducer) *kafkaEventPublisher {
	return &kafkaEventPublisher{
		logger:   logger,
		producer: producer,
	}
}

type kafkaEventPublisher struct {
	logger   *logrus.Logger
	producer sarama.SyncProducer
}

func (svc *kafkaEventPublisher) PublishMessage(ctx context.Context, message Message) error {
	kafkaMessages := message.toKafkaMessage()
	partition, offset, err := svc.producer.SendMessage(kafkaMessages)
	if err != nil {
		svc.logger.WithError(err).Error("can not produce message")
		return err
	}
	svc.logger.WithFields(logrus.Fields{
		"partition": partition,
		"offset":    offset,
		}).Info("message published")
		return nil
	}


func (svc *kafkaEventPublisher) PublishMessages(ctx context.Context, messages []Message) error {
	if err := svc.producer.BeginTxn(); err != nil {
		return fmt.Errorf("creating kafka transaction error: %w", err)
	}

	kafkaMessages := make([]*sarama.ProducerMessage, len(messages))
	for i, msg := range messages {
		kafkaMessages[i] = msg.toKafkaMessage()
	}

	err := svc.producer.SendMessages(kafkaMessages)
	if err != nil {
		if abortError := svc.producer.AbortTxn(); err != nil {
			return fmt.Errorf("aborting kafka transaction failed: %w: %w", abortError, err)
		}

		return fmt.Errorf("publishing kafka messages error: %w", err)
	}

	if err := svc.producer.CommitTxn(); err != nil {
		svc.logger.WithError(err).Error("committing kafka messages failed")

		// Recommit messages with backoff
		time.Sleep(time.Second * 2)

		if err := svc.producer.CommitTxn(); err != nil {
			if abortError := svc.producer.AbortTxn(); err != nil {
				return fmt.Errorf("aborting kafka transaction failed: %w: %w", abortError, err)
			}
			return nil
		}
		return fmt.Errorf("commit kafka transaction error: %w", err)
	}
	return nil
}

func (svc *kafkaEventPublisher) Shutdown(ctx context.Context) error {
	return svc.producer.Close()
}
