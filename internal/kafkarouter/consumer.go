package kafkarouter

import (
	"context"
	"time"

	"github.com/Shopify/sarama"
	"github.com/sirupsen/logrus"
)

type Consumer struct {
	// ready    chan bool
	logger   *logrus.Logger
	handlers map[string]Handler
}

// TODO: graceful, error handling
func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	// close(consumer.ready)
	consumer.logger.Info("kafka consumer started")
	return nil
}

// TODO: graceful, error handling
func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	consumer.logger.Info("kafka consumer cleaning up rebalance occurred")
	return nil
}

func (consumer *Consumer) ConsumeClaim(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) error {
	for {
		select {
		case message := <-claim.Messages():
			logger := consumer.logger.WithFields(logrus.Fields{
				"topic":     message.Topic,
				"key":       message.Key,
				"partition": message.Partition,
				"offset":    message.Offset,
			})
			logger.Info("new message came")

			handler, ok := consumer.handlers[message.Topic]
			if !ok {
				logger.Warn("handler does not exists for this topic!")
				session.MarkMessage(message, "")
				continue
			}

			ctx, cancel := context.WithTimeout(session.Context(), time.Second*59)
			msg := &Message{
				Topic:     message.Topic,
				Key:       message.Key,
				Payload:   message.Value,
				Timestamp: message.Timestamp,
			}

			err := handler(ctx, msg)
			cancel()

			if err != nil {
				logger.WithError(err).Error("processing message failed")
				return err
			}

			logger.Debug("message handled successfully")
			session.MarkMessage(message, "")
		case <-session.Context().Done():
			return nil
		}
	}
}
