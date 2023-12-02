package eventbus

import (
	"time"

	"github.com/Shopify/sarama"
)

type Message struct {
	Topic   string
	Key     []byte
	Payload []byte
	Headers map[string]string
	Time    time.Time
}

func (m Message) toKafkaMessage() *sarama.ProducerMessage {
	headers := make([]sarama.RecordHeader, 0, len(m.Headers))
	for k, v := range m.Headers {
		headers = append(headers, sarama.RecordHeader{
			Key:   []byte(k),
			Value: []byte(v),
		})
	}

	return &sarama.ProducerMessage{
		Topic:     m.Topic,
		Key:       sarama.ByteEncoder(m.Key),
		Value:     sarama.ByteEncoder(m.Payload),
		Headers:   headers,
		Timestamp: m.Time,
	}
}
