package kafkarouter

import (
	"context"
	"errors"

	"github.com/Shopify/sarama"
	"github.com/sirupsen/logrus"
)

type Handler func(ctx context.Context, msg *Message) error

func New(logger *logrus.Logger, consumerGroup sarama.ConsumerGroup) *KafkaRouter {
	return &KafkaRouter{
		logger:        logger,
		consumerGroup: consumerGroup,
		handlers:      make(map[string]Handler),
	}
}

type KafkaRouter struct {
	logger        *logrus.Logger
	consumerGroup sarama.ConsumerGroup
	handlers      map[string]Handler
}

func (r *KafkaRouter) RegisterHandler(topic string, handler Handler) error {
	r.handlers[topic] = handler
	return nil
}

func (r *KafkaRouter) Start(ctx context.Context) error {
	topics := make([]string, 0, len(r.handlers))
	for topic := range r.handlers {
		topics = append(topics, topic)
	}

	for {
		r.logger.Info("creating new kafka consumer claim")
		consumer := &Consumer{handlers: r.handlers, logger: r.logger}
		err := r.consumerGroup.Consume(ctx, topics, consumer)
		if errors.Is(err, sarama.ErrClosedConsumerGroup) {
			r.logger.Info("consumer group stopped")
			return nil
		}
		if err != nil {
			r.logger.WithError(err).Error("kafka consumer stopped with an error, retrying")
		}
	}
}

func (r *KafkaRouter) Shutdown(ctx context.Context) error {
	return r.consumerGroup.Close()
}
